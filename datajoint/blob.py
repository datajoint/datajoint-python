"""
Provides serialization methods for numpy.ndarrays that ensure compatibility with Matlab.
"""

import zlib
from itertools import repeat
from collections import OrderedDict, Mapping, Iterable
from decimal import Decimal
from datetime import datetime
import numpy as np
from .errors import DataJointError

mxClassID = OrderedDict((
    # see http://www.mathworks.com/help/techdoc/apiref/mxclassid.html
    ('mxUNKNOWN_CLASS', None),
    ('mxCELL_CLASS', None),
    ('mxSTRUCT_CLASS', None),
    ('mxLOGICAL_CLASS', np.dtype('bool')),
    ('mxCHAR_CLASS', np.dtype('c')),
    ('mxVOID_CLASS', None),
    ('mxDOUBLE_CLASS', np.dtype('float64')),
    ('mxSINGLE_CLASS', np.dtype('float32')),
    ('mxINT8_CLASS', np.dtype('int8')),
    ('mxUINT8_CLASS', np.dtype('uint8')),
    ('mxINT16_CLASS', np.dtype('int16')),
    ('mxUINT16_CLASS', np.dtype('uint16')),
    ('mxINT32_CLASS', np.dtype('int32')),
    ('mxUINT32_CLASS', np.dtype('uint32')),
    ('mxINT64_CLASS', np.dtype('int64')),
    ('mxUINT64_CLASS', np.dtype('uint64')),
    ('mxFUNCTION_CLASS', None)))

rev_class_id = {dtype: i for i, dtype in enumerate(mxClassID.values())}
dtype_list = list(mxClassID.values())
type_names = list(mxClassID)

decode_lookup = {
    b'ZL123\0': zlib.decompress
}


class BlobReader:
    def __init__(self, blob, squeeze=False, as_dict=False):
        self._squeeze = squeeze
        self._blob = blob
        self._pos = 0
        self._as_dict = as_dict

    def decompress(self):
        for pattern, decoder in decode_lookup.items():
            if self._blob[self._pos:].startswith(pattern):
                self._pos += len(pattern)
                blob_size = self.read_value('uint64')
                blob = decoder(self._blob[self._pos:])
                assert len(blob) == blob_size
                self._blob = blob
                self._pos = 0
                break

    def unpack(self):
        self.decompress()
        blob_format = self.read_zero_terminated_string()
        if blob_format == 'mYm':
            return self.read_blob(n_bytes=len(self._blob) - self._pos)

    def read_blob(self, n_bytes):
        start = self._pos
        call = {
            'A': self.read_array,
            'S': self.read_structure,
            'C': self.read_cell_array}[
            self.read_value('c').decode()]
        v = call()
        if self._pos - start != n_bytes:
            raise DataJointError('Blob was incorrectly structured')
        return v

    def read_array(self):
        n_dims = int(self.read_value('uint64'))
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        dtype_id = self.read_value('uint32')
        dtype = dtype_list[dtype_id]
        is_complex = self.read_value('uint32')

        if type_names[dtype_id] == 'mxCHAR_CLASS':
            # compensate for MATLAB packing of char arrays
            data = self.read_value(dtype, count=2 * n_elem)
            data = data[::2].astype('U1')
            if n_dims == 2 and shape[0] == 1 or n_dims == 1:
                compact = data.squeeze()
                data = compact if compact.shape == () else np.array(''.join(data.squeeze()))
                shape = (1,)
        else:
            data = self.read_value(dtype, count=n_elem)
            if is_complex:
                data = data + 1j * self.read_value(dtype, count=n_elem)

        return self.squeeze(data.reshape(shape, order='F'))

    def read_structure(self):
        n_dims = self.read_value('uint64').item()
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        n_field = int(self.read_value('uint32'))
        if not n_field:
            return np.array(None)  # empty array
        field_names = [self.read_zero_terminated_string() for _ in range(n_field)]
        raw_data = [
            tuple(self.read_blob(n_bytes=int(self.read_value('uint64'))) for _ in range(n_field))
            for __ in range(n_elem)]

        if self._as_dict and n_elem == 1:
            return dict(zip(field_names, raw_data[0]))
        data = np.rec.array(raw_data, dtype=list(zip(field_names, repeat(np.object))))
        return self.squeeze(data.reshape(shape, order='F'))

    def squeeze(self, array):
        """
        Simplify the input array - squeeze out all singleton
        dimensions and also convert a zero dimensional array into array scalar
        """
        if not self._squeeze:
            return array
        array = array.copy().squeeze()
        if array.ndim == 0:
            array = array[()]
        return array

    def read_cell_array(self):
        n_dims = self.read_value('uint64').item()
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        return self.squeeze(np.array(
            [self.read_blob(n_bytes=self.read_value('uint64').item()) for _ in range(n_elem)], dtype=np.object))

    def read_zero_terminated_string(self):
        target = self._blob.find(b'\0', self._pos)
        data = self._blob[self._pos:target].decode()
        self._pos = target + 1
        return data
        
    def read_value(self, dtype='uint64', count=1):
        data = np.frombuffer(self._blob, dtype=dtype, count=count, offset=self._pos)
        self._pos += data.dtype.itemsize * data.size
        return data[0] if count == 1 else data

    def __repr__(self):
        return repr(self._blob[self._pos:])

    def __str__(self):
        return str(self._blob[self._pos:])


def pack(obj, compress=True):
    blob = b"mYm\0" + pack_obj(obj)
    if compress:
        compressed = b'ZL123\0' + np.uint64(len(blob)).tobytes() + zlib.compress(blob)
        if len(compressed) < len(blob):
            blob = compressed
    return blob


def pack_obj(obj):
    """serialize object"""
    if isinstance(obj, np.ndarray):
        return pack_array(obj)
    if isinstance(obj, Mapping):
        return pack_dict(obj)
    if isinstance(obj, str):
        return pack_array(np.array(obj, dtype=np.dtype('c')))
    if isinstance(obj, Iterable):
        return pack_array(np.array(list(obj)))
    if isinstance(obj, (int, float, np.float32, np.float64, np.int64, np.uint64,
                        np.int32, np.uint32, np.int16, np.uint16, np.int8, np.uint8)):
        return pack_array(np.array(obj))
    if isinstance(obj, Decimal):
        return pack_array(np.array(np.float64(obj)))
    if isinstance(obj, datetime):
        return pack_obj(str(obj))

    raise DataJointError("Packing object of type %s currently not supported!" % type(obj))


def pack_array(array):
    """ Serialize a np.ndarray object into bytes """
    blob = b"A" + np.uint64(array.ndim).tobytes() + np.array(array.shape, dtype=np.uint64).tobytes()
    is_complex = np.iscomplexobj(array)
    if is_complex:
        array, imaginary = np.real(array), np.imag(array)

    type_id = rev_class_id[array.dtype]
    if dtype_list[type_id] is None:
        raise DataJointError("Type %s is ambiguous or unknown" % array.dtype)

    blob += np.array([type_id, is_complex], dtype=np.uint32).tobytes()

    if type_names[type_id] == 'mxCHAR_CLASS':
        blob += array.view(np.uint8).astype(np.uint16).tobytes()  # convert to 16-bit chars for MATLAB
    else:
        blob += array.tobytes(order='F')
        if is_complex:
            blob += imaginary.tobytes(order='F')
    return blob


def pack_dict(obj):
    """ Serialize a dict-like object into a singular structure array """
    return (b'S' + np.uint64(1).tobytes() + np.uint64(1).tobytes() +  # dimensionality and dimensions
            np.uint32(len(obj)).tobytes() +  # number of fields
            b''.join(map(lambda x: x.encode() + b'\0', obj)) +  # field names
            b''.join(np.uint64(len(p)).tobytes() + p for p in (pack_obj(v) for v in obj.values())))  # values


def unpack(blob, **kwargs):
    if blob is not None:
        return BlobReader(blob, **kwargs).unpack()
