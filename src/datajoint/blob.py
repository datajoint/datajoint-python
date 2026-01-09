"""
Binary serialization for DataJoint blob storage.

Provides (de)serialization for Python/NumPy objects with backward compatibility
for MATLAB mYm-format blobs. Supports arrays, scalars, structs, cells, and
Python built-in types (dict, list, tuple, set, datetime, UUID, Decimal).
"""

from __future__ import annotations

import collections
import datetime
import uuid
import zlib
from decimal import Decimal
from itertools import repeat

import numpy as np

from .errors import DataJointError

deserialize_lookup = {
    0: {"dtype": None, "scalar_type": "UNKNOWN"},
    1: {"dtype": None, "scalar_type": "CELL"},
    2: {"dtype": None, "scalar_type": "STRUCT"},
    3: {"dtype": np.dtype("bool"), "scalar_type": "LOGICAL"},
    4: {"dtype": np.dtype("c"), "scalar_type": "CHAR"},
    5: {"dtype": np.dtype("O"), "scalar_type": "VOID"},
    6: {"dtype": np.dtype("float64"), "scalar_type": "DOUBLE"},
    7: {"dtype": np.dtype("float32"), "scalar_type": "SINGLE"},
    8: {"dtype": np.dtype("int8"), "scalar_type": "INT8"},
    9: {"dtype": np.dtype("uint8"), "scalar_type": "UINT8"},
    10: {"dtype": np.dtype("int16"), "scalar_type": "INT16"},
    11: {"dtype": np.dtype("uint16"), "scalar_type": "UINT16"},
    12: {"dtype": np.dtype("int32"), "scalar_type": "INT32"},
    13: {"dtype": np.dtype("uint32"), "scalar_type": "UINT32"},
    14: {"dtype": np.dtype("int64"), "scalar_type": "INT64"},
    15: {"dtype": np.dtype("uint64"), "scalar_type": "UINT64"},
    16: {"dtype": None, "scalar_type": "FUNCTION"},
    65_536: {"dtype": np.dtype("datetime64[Y]"), "scalar_type": "DATETIME64[Y]"},
    65_537: {"dtype": np.dtype("datetime64[M]"), "scalar_type": "DATETIME64[M]"},
    65_538: {"dtype": np.dtype("datetime64[W]"), "scalar_type": "DATETIME64[W]"},
    65_539: {"dtype": np.dtype("datetime64[D]"), "scalar_type": "DATETIME64[D]"},
    65_540: {"dtype": np.dtype("datetime64[h]"), "scalar_type": "DATETIME64[h]"},
    65_541: {"dtype": np.dtype("datetime64[m]"), "scalar_type": "DATETIME64[m]"},
    65_542: {"dtype": np.dtype("datetime64[s]"), "scalar_type": "DATETIME64[s]"},
    65_543: {"dtype": np.dtype("datetime64[ms]"), "scalar_type": "DATETIME64[ms]"},
    65_544: {"dtype": np.dtype("datetime64[us]"), "scalar_type": "DATETIME64[us]"},
    65_545: {"dtype": np.dtype("datetime64[ns]"), "scalar_type": "DATETIME64[ns]"},
    65_546: {"dtype": np.dtype("datetime64[ps]"), "scalar_type": "DATETIME64[ps]"},
    65_547: {"dtype": np.dtype("datetime64[fs]"), "scalar_type": "DATETIME64[fs]"},
    65_548: {"dtype": np.dtype("datetime64[as]"), "scalar_type": "DATETIME64[as]"},
}
serialize_lookup = {
    v["dtype"]: {"type_id": k, "scalar_type": v["scalar_type"]}
    for k, v in deserialize_lookup.items()
    if v["dtype"] is not None
}


compression = {b"ZL123\0": zlib.decompress}

# runtime setting to read integers as 32-bit to read blobs created by the 32-bit
# version of the mYm library for MATLAB
use_32bit_dims = False


def len_u64(obj):
    return np.uint64(len(obj)).tobytes()


def len_u32(obj):
    return np.uint32(len(obj)).tobytes()


class MatCell(np.ndarray):
    """
    NumPy ndarray subclass representing a MATLAB cell array.

    Used to distinguish cell arrays from regular arrays during serialization
    for MATLAB compatibility.
    """

    pass


class MatStruct(np.recarray):
    """
    NumPy recarray subclass representing a MATLAB struct array.

    Used to distinguish struct arrays from regular recarrays during
    serialization for MATLAB compatibility.
    """

    pass


class Blob:
    """
    Binary serializer/deserializer for DataJoint blob storage.

    Handles packing Python objects into binary format and unpacking binary
    data back to Python objects. Supports two protocols:

    - ``mYm``: Original MATLAB-compatible format (default)
    - ``dj0``: Extended format for Python-specific types

    Parameters
    ----------
    squeeze : bool, optional
        If True, remove singleton dimensions from arrays and convert
        0-dimensional arrays to scalars. Default False.

    Attributes
    ----------
    protocol : bytes or None
        Current serialization protocol (``b"mYm\\0"`` or ``b"dj0\\0"``).
    """

    def __init__(self, squeeze: bool = False) -> None:
        self._squeeze = squeeze
        self._blob = None
        self._pos = 0
        self.protocol = None

    def set_dj0(self) -> None:
        """Switch to dj0 protocol for extended type support."""
        self.protocol = b"dj0\0"  # when using new blob features

    def squeeze(self, array: np.ndarray, convert_to_scalar: bool = True) -> np.ndarray:
        """
        Remove singleton dimensions from an array.

        Parameters
        ----------
        array : np.ndarray
            Input array.
        convert_to_scalar : bool, optional
            If True, convert 0-dimensional arrays to Python scalars. Default True.

        Returns
        -------
        np.ndarray or scalar
            Squeezed array or scalar value.
        """
        if not self._squeeze:
            return array
        array = array.squeeze()
        return array.item() if array.ndim == 0 and convert_to_scalar else array

    def unpack(self, blob):
        self._blob = blob
        try:
            # decompress
            prefix = next(p for p in compression if self._blob[self._pos :].startswith(p))
        except StopIteration:
            pass  # assume uncompressed but could be unrecognized compression
        else:
            self._pos += len(prefix)
            blob_size = self.read_value()
            blob = compression[prefix](self._blob[self._pos :])
            assert len(blob) == blob_size
            self._blob = blob
            self._pos = 0
        blob_format = self.read_zero_terminated_string()
        if blob_format in ("mYm", "dj0"):
            return self.read_blob(n_bytes=len(self._blob) - self._pos)

    def read_blob(self, n_bytes=None):
        start = self._pos
        data_structure_code = chr(self.read_value("uint8"))
        try:
            call = {
                # MATLAB-compatible, inherited from original mYm
                "A": self.read_array,  # matlab-compatible numeric arrays and scalars with ndim==0
                "P": self.read_sparse_array,  # matlab sparse array -- not supported yet
                "S": self.read_struct,  # matlab struct array
                "C": self.read_cell_array,  # matlab cell array
                # basic data types
                "\xff": self.read_none,  # None
                "\x01": self.read_tuple,  # a Sequence (e.g. tuple)
                "\x02": self.read_list,  # a MutableSequence (e.g. list)
                "\x03": self.read_set,  # a Set
                "\x04": self.read_dict,  # a Mapping (e.g. dict)
                "\x05": self.read_string,  # a UTF8-encoded string
                "\x06": self.read_bytes,  # a ByteString
                "\x0a": self.read_int,  # unbounded scalar int
                "\x0b": self.read_bool,  # scalar boolean
                "\x0c": self.read_complex,  # scalar 128-bit complex number
                "\x0d": self.read_float,  # scalar 64-bit float
                "F": self.read_recarray,  # numpy array with fields, including recarrays
                "d": self.read_decimal,  # a decimal
                "t": self.read_datetime,  # date, time, or datetime
                "u": self.read_uuid,  # UUID
            }[data_structure_code]
        except KeyError:
            raise DataJointError('Unknown data structure code "%s". Upgrade datajoint.' % data_structure_code)
        v = call()
        if n_bytes is not None and self._pos - start != n_bytes:
            raise DataJointError("Blob length check failed! Invalid blob")
        return v

    def pack_blob(self, obj):
        # original mYm-based serialization from datajoint-matlab
        if isinstance(obj, MatCell):
            return self.pack_cell_array(obj)
        if isinstance(obj, MatStruct):
            return self.pack_struct(obj)
        if isinstance(obj, np.ndarray) and obj.dtype.fields is None:
            return self.pack_array(obj)

        # blob types in the expanded dj0 blob format
        self.set_dj0()
        if not isinstance(obj, (np.ndarray, np.number)):
            # python built-in data types
            if isinstance(obj, bool):
                return self.pack_bool(obj)
            if isinstance(obj, int):
                return self.pack_int(obj)
            if isinstance(obj, complex):
                return self.pack_complex(obj)
            if isinstance(obj, float):
                return self.pack_float(obj)
        if isinstance(obj, np.ndarray) and obj.dtype.fields:
            return self.pack_recarray(np.array(obj))
        if isinstance(obj, (np.number, np.datetime64)):
            return self.pack_array(np.array(obj))
        if isinstance(obj, (bool, np.bool_)):
            return self.pack_array(np.array(obj))
        if isinstance(obj, (float, int, complex)):
            return self.pack_array(np.array(obj))
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return self.pack_datetime(obj)
        if isinstance(obj, Decimal):
            return self.pack_decimal(obj)
        if isinstance(obj, uuid.UUID):
            return self.pack_uuid(obj)
        if isinstance(obj, collections.abc.Mapping):
            return self.pack_dict(obj)
        if isinstance(obj, str):
            return self.pack_string(obj)
        if isinstance(obj, (bytes, bytearray)):
            return self.pack_bytes(obj)
        if isinstance(obj, collections.abc.MutableSequence):
            return self.pack_list(obj)
        if isinstance(obj, collections.abc.Sequence):
            return self.pack_tuple(obj)
        if isinstance(obj, collections.abc.Set):
            return self.pack_set(obj)
        if obj is None:
            return self.pack_none()
        raise DataJointError("Packing object of type %s currently not supported!" % type(obj))

    def read_array(self):
        n_dims = int(self.read_value())
        shape = self.read_value(count=n_dims)
        n_elem = np.prod(shape, dtype=int)
        dtype_id, is_complex = self.read_value("uint32", 2)

        # Get dtype from type id
        dtype = deserialize_lookup[dtype_id]["dtype"]

        # Check if name is void
        if deserialize_lookup[dtype_id]["scalar_type"] == "VOID":
            data = np.array(
                list(self.read_blob(self.read_value()) for _ in range(n_elem)),
                dtype=np.dtype("O"),
            )
        # Check if name is char
        elif deserialize_lookup[dtype_id]["scalar_type"] == "CHAR":
            # compensate for MATLAB packing of char arrays
            data = self.read_value(dtype, count=2 * n_elem)
            data = data[::2].astype("U1")
            if n_dims == 2 and shape[0] == 1 or n_dims == 1:
                compact = data.squeeze()
                data = compact if compact.shape == () else np.array("".join(data.squeeze()))
                shape = (1,)
        else:
            data = self.read_value(dtype, count=n_elem)
            if is_complex:
                data = data + 1j * self.read_value(dtype, count=n_elem)
        return self.squeeze(data.reshape(shape, order="F"))

    def pack_array(self, array: np.ndarray) -> bytes:
        """
        Serialize a NumPy array into bytes.

        Parameters
        ----------
        array : np.ndarray
            Array to serialize. Scalars are encoded with ndim=0.

        Returns
        -------
        bytes
            Serialized array data.
        """
        if "datetime64" in array.dtype.name:
            self.set_dj0()
        blob = b"A" + np.uint64(array.ndim).tobytes() + np.array(array.shape, dtype=np.uint64).tobytes()
        is_complex = np.iscomplexobj(array)
        if is_complex:
            array, imaginary = np.real(array), np.imag(array)
        try:
            type_id = serialize_lookup[array.dtype]["type_id"]
        except KeyError:
            # U is for unicode string
            if array.dtype.char == "U":
                type_id = serialize_lookup[np.dtype("O")]["type_id"]
            else:
                raise DataJointError(f"Type {array.dtype} is ambiguous or unknown")

        blob += np.array([type_id, is_complex], dtype=np.uint32).tobytes()
        if array.dtype.char == "U" or serialize_lookup[array.dtype]["scalar_type"] == "VOID":
            blob += b"".join(len_u64(it) + it for it in (self.pack_blob(e) for e in array.flatten(order="F")))
            self.set_dj0()  # not supported by original mym
        elif serialize_lookup[array.dtype]["scalar_type"] == "CHAR":
            blob += array.view(np.uint8).astype(np.uint16).tobytes()  # convert to 16-bit chars for MATLAB
        else:  # numeric arrays
            if array.ndim == 0:  # not supported by original mym
                self.set_dj0()
            blob += array.tobytes(order="F")
            if is_complex:
                blob += imaginary.tobytes(order="F")
        return blob

    def read_recarray(self):
        """
        Serialize an np.ndarray with fields, including recarrays
        """
        n_fields = self.read_value("uint32")
        if not n_fields:
            return np.array(None)  # empty array
        field_names = [self.read_zero_terminated_string() for _ in range(n_fields)]
        arrays = [self.read_blob() for _ in range(n_fields)]
        rec = np.empty(
            arrays[0].shape,
            np.dtype([(f, t.dtype) for f, t in zip(field_names, arrays)]),
        )
        for f, t in zip(field_names, arrays):
            rec[f] = t
        return rec.view(np.recarray)

    def pack_recarray(self, array):
        """Serialize a Matlab struct array"""
        return (
            b"F"
            + len_u32(array.dtype)
            + "\0".join(array.dtype.names).encode()  # number of fields
            + b"\0"
            + b"".join(  # field names
                (self.pack_recarray(array[f]) if array[f].dtype.fields else self.pack_array(array[f]))
                for f in array.dtype.names
            )
        )

    def read_sparse_array(self):
        raise DataJointError("datajoint-python does not yet support sparse arrays. Issue (#590)")

    def read_int(self):
        return int.from_bytes(self.read_binary(self.read_value("uint16")), byteorder="little", signed=True)

    @staticmethod
    def pack_int(v):
        n_bytes = v.bit_length() // 8 + 1
        assert 0 < n_bytes <= 0xFFFF, "Integers are limited to 65535 bytes"
        return b"\x0a" + np.uint16(n_bytes).tobytes() + v.to_bytes(n_bytes, byteorder="little", signed=True)

    def read_bool(self):
        return bool(self.read_value("bool"))

    @staticmethod
    def pack_bool(v):
        return b"\x0b" + np.array(v, dtype="bool").tobytes()

    def read_complex(self):
        return complex(self.read_value("complex128"))

    @staticmethod
    def pack_complex(v):
        return b"\x0c" + np.array(v, dtype="complex128").tobytes()

    def read_float(self):
        return float(self.read_value("float64"))

    @staticmethod
    def pack_float(v):
        return b"\x0d" + np.array(v, dtype="float64").tobytes()

    def read_decimal(self):
        return Decimal(self.read_string())

    @staticmethod
    def pack_decimal(d):
        s = str(d)
        return b"d" + len_u64(s) + s.encode()

    def read_string(self):
        return self.read_binary(self.read_value()).decode()

    @staticmethod
    def pack_string(s):
        blob = s.encode()
        return b"\5" + len_u64(blob) + blob

    def read_bytes(self):
        return self.read_binary(self.read_value())

    @staticmethod
    def pack_bytes(s):
        return b"\6" + len_u64(s) + s

    def read_none(self):
        pass

    @staticmethod
    def pack_none():
        return b"\xff"

    def read_tuple(self):
        return tuple(self.read_blob(self.read_value()) for _ in range(self.read_value()))

    def pack_tuple(self, t):
        return b"\1" + len_u64(t) + b"".join(len_u64(it) + it for it in (self.pack_blob(i) for i in t))

    def read_list(self):
        return list(self.read_blob(self.read_value()) for _ in range(self.read_value()))

    def pack_list(self, t):
        return b"\2" + len_u64(t) + b"".join(len_u64(it) + it for it in (self.pack_blob(i) for i in t))

    def read_set(self):
        return set(self.read_blob(self.read_value()) for _ in range(self.read_value()))

    def pack_set(self, t):
        return b"\3" + len_u64(t) + b"".join(len_u64(it) + it for it in (self.pack_blob(i) for i in t))

    def read_dict(self):
        return dict((self.read_blob(self.read_value()), self.read_blob(self.read_value())) for _ in range(self.read_value()))

    def pack_dict(self, d):
        return (
            b"\4"
            + len_u64(d)
            + b"".join(
                b"".join((len_u64(it) + it) for it in packed) for packed in (map(self.pack_blob, pair) for pair in d.items())
            )
        )

    def read_struct(self):
        """deserialize matlab struct"""
        n_dims = self.read_value()
        shape = self.read_value(count=n_dims)
        n_elem = np.prod(shape, dtype=int)
        n_fields = self.read_value("uint32")
        if not n_fields:
            return np.array(None)  # empty array
        field_names = [self.read_zero_terminated_string() for _ in range(n_fields)]
        raw_data = [tuple(self.read_blob(n_bytes=int(self.read_value())) for _ in range(n_fields)) for __ in range(n_elem)]
        data = np.array(raw_data, dtype=list(zip(field_names, repeat(object))))
        return self.squeeze(data.reshape(shape, order="F"), convert_to_scalar=False).view(MatStruct)

    def pack_struct(self, array):
        """Serialize a Matlab struct array"""
        return (
            b"S"
            + np.array((array.ndim,) + array.shape, dtype=np.uint64).tobytes()
            + len_u32(array.dtype.names)  # dimensionality
            + "\0".join(array.dtype.names).encode()  # number of fields
            + b"\0"
            + b"".join(  # field names
                len_u64(it) + it for it in (self.pack_blob(e) for rec in array.flatten(order="F") for e in rec)
            )
        )  # values

    def read_cell_array(self):
        """
        Deserialize MATLAB cell array.

        Handles edge cases from MATLAB:
        - Empty cell arrays ({})
        - Cell arrays with empty elements ({[], [], []})
        - Nested arrays ({[1,2], [3,4,5]}) - ragged arrays
        - Cell matrices with mixed content
        """
        n_dims = self.read_value()
        shape = self.read_value(count=n_dims)
        n_elem = int(np.prod(shape))
        result = [self.read_blob(n_bytes=self.read_value()) for _ in range(n_elem)]

        # Handle empty cell array
        if n_elem == 0:
            return np.empty(0, dtype=object).view(MatCell)

        # Use object dtype to handle ragged/nested arrays without reshape errors.
        # This avoids NumPy's array homogeneity requirements that cause failures
        # with MATLAB cell arrays containing arrays of different sizes.
        arr = np.empty(n_elem, dtype=object)
        arr[:] = result
        return self.squeeze(arr.reshape(shape, order="F"), convert_to_scalar=False).view(MatCell)

    def pack_cell_array(self, array):
        return (
            b"C"
            + np.array((array.ndim,) + array.shape, dtype=np.uint64).tobytes()
            + b"".join(len_u64(it) + it for it in (self.pack_blob(e) for e in array.flatten(order="F")))
        )

    def read_datetime(self):
        """deserialize datetime.date, .time, or .datetime"""
        date, time = self.read_value("int32"), self.read_value("int64")
        date = datetime.date(year=date // 10000, month=(date // 100) % 100, day=date % 100) if date >= 0 else None
        time = (
            datetime.time(
                hour=(time // 10000000000) % 100,
                minute=(time // 100000000) % 100,
                second=(time // 1000000) % 100,
                microsecond=time % 1000000,
            )
            if time >= 0
            else None
        )
        return time and date and datetime.datetime.combine(date, time) or time or date

    @staticmethod
    def pack_datetime(d):
        if isinstance(d, datetime.datetime):
            date, time = d.date(), d.time()
        elif isinstance(d, datetime.date):
            date, time = d, None
        else:
            date, time = None, d
        return b"t" + (
            np.int32(-1 if date is None else (date.year * 100 + date.month) * 100 + date.day).tobytes()
            + np.int64(
                -1 if time is None else ((time.hour * 100 + time.minute) * 100 + time.second) * 1000000 + time.microsecond
            ).tobytes()
        )

    def read_uuid(self):
        q = self.read_binary(16)
        return uuid.UUID(bytes=q)

    @staticmethod
    def pack_uuid(obj):
        return b"u" + obj.bytes

    def read_zero_terminated_string(self):
        target = self._blob.find(b"\0", self._pos)
        data = self._blob[self._pos : target].decode()
        self._pos = target + 1
        return data

    def read_value(self, dtype=None, count=1):
        if dtype is None:
            dtype = "uint32" if use_32bit_dims else "uint64"
        data = np.frombuffer(self._blob, dtype=dtype, count=count, offset=self._pos)
        self._pos += data.dtype.itemsize * data.size
        return data[0] if count == 1 else data

    def read_binary(self, size):
        self._pos += int(size)
        return self._blob[self._pos - int(size) : self._pos]

    def pack(self, obj, compress):
        self.protocol = b"mYm\0"  # will be replaced with dj0 if new features are used
        blob = self.pack_blob(obj)  # this may reset the protocol and must precede protocol evaluation
        blob = self.protocol + blob
        if compress and len(blob) > 1000:
            compressed = b"ZL123\0" + len_u64(blob) + zlib.compress(blob)
            if len(compressed) < len(blob):
                blob = compressed
        return blob


def pack(obj, compress: bool = True) -> bytes:
    """
    Serialize a Python object to binary blob format.

    Parameters
    ----------
    obj : any
        Object to serialize. Supports NumPy arrays, Python scalars,
        collections (dict, list, tuple, set), datetime objects, UUID,
        Decimal, and MATLAB-compatible MatCell/MatStruct.
    compress : bool, optional
        If True (default), compress blobs larger than 1000 bytes using zlib.

    Returns
    -------
    bytes
        Serialized binary data.

    Raises
    ------
    DataJointError
        If the object type is not supported.

    Examples
    --------
    >>> data = np.array([1, 2, 3])
    >>> blob = pack(data)
    >>> unpacked = unpack(blob)
    """
    return Blob().pack(obj, compress=compress)


def unpack(blob: bytes, squeeze: bool = False):
    """
    Deserialize a binary blob to a Python object.

    Parameters
    ----------
    blob : bytes
        Binary data from ``pack()`` or MATLAB mYm serialization.
    squeeze : bool, optional
        If True, remove singleton dimensions from arrays. Default False.

    Returns
    -------
    any
        Deserialized Python object.

    Examples
    --------
    >>> blob = pack({'a': 1, 'b': [1, 2, 3]})
    >>> data = unpack(blob)
    >>> data['b']
    [1, 2, 3]
    """
    if blob is not None:
        return Blob(squeeze=squeeze).unpack(blob)
