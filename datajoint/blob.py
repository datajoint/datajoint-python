"""
Provides serialization methods for numpy.ndarrays that ensure compatibility with Matlab.
"""

import zlib
from collections import OrderedDict, Mapping, Iterable
import numpy as np
from . import DataJointError

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

decode_lookup = {
    b'ZL123\0': zlib.decompress
}


class BlobReader:
    def __init__(self, blob, squeeze=False, as_dict=False):
        self._squeeze = squeeze
        self._blob = blob
        self._pos = 0
        self._as_dict = as_dict

    @property
    def pos(self):
        return self._pos

    @pos.setter
    def pos(self, val):
        self._pos = val

    def reset(self):
        self.pos = 0

    def decompress(self):
        for pattern, decoder in decode_lookup.items():
            if self._blob[self.pos:].startswith(pattern):
                self.pos += len(pattern)
                blob_size = self.read_value('uint64')
                blob = decoder(self._blob[self.pos:])
                assert len(blob) == blob_size
                self._blob = blob
                self._pos = 0
                break

    def unpack(self):
        self.decompress()
        blob_format = self.read_string()
        if blob_format == 'mYm':
            return self.read_mym_data(n_bytes=-1)

    def read_mym_data(self, n_bytes=None):
        if n_bytes is not None:
            if n_bytes == -1:
                n_bytes = len(self._blob) - self.pos
            n_bytes -= 1

        type_id = self.read_value('c')
        if type_id == b'A':
            return self.read_array(n_bytes=n_bytes)
        elif type_id == b'S':
            return self.read_structure(n_bytes=n_bytes)
        elif type_id == b'C':
            return self.read_cell_array(n_bytes=n_bytes)

    def read_array(self, advance=True, n_bytes=None):
        start = self.pos
        n_dims = int(self.read_value('uint64'))
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        dtype_id = self.read_value('uint32')
        dtype = dtype_list[dtype_id]
        is_complex = self.read_value('uint32')

        if dtype_id == 4:  # if dealing with character array
            data = self.read_value(dtype, count=2 * n_elem)
            data = data[::2].astype('U1')
            if n_dims == 2 and shape[0] == 1 or n_dims == 1:
                compact = data.squeeze()
                data = compact if compact.shape == () else np.array(''.join(data.squeeze()))
                shape = (1,)
        else:
            if is_complex:
                n_elem *= 2 # read real and imaginary parts
            data = self.read_value(dtype, count=n_elem)
            if is_complex:
                data = data[:n_elem//2] + 1j * data[n_elem//2:]

        if n_bytes is not None:
            assert self.pos - start == n_bytes

        if not advance:
            self.pos = start

        return self.squeeze(data.reshape(shape, order='F'))

    def read_structure(self, advance=True, n_bytes=None):
        start = self.pos
        n_dims = self.read_value('uint64').item()
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        n_field = int(self.read_value('uint32'))
        field_names = []
        for i in range(n_field):
            field_names.append(self.read_string())
        if not field_names:
            # return an empty array
            return np.array(None)
        dt = [(f, np.object) for f in field_names]
        raw_data = []
        for k in range(n_elem):
            values = []
            for i in range(n_field):
                nb = int(self.read_value('uint64')) # dealing with a weird bug of numpy
                values.append(self.read_mym_data(n_bytes=nb))
            raw_data.append(tuple(values))
        if n_bytes is not None:
            assert self.pos - start == n_bytes
        if not advance:
            self.pos = start

        if self._as_dict and n_elem == 1:
            data = dict(zip(field_names, values))
            return data
        else:
            data = np.rec.array(raw_data, dtype=dt)
            return self.squeeze(data.reshape(shape, order='F'))

    def squeeze(self, array):
        """
        Simplify the given array as much as possible - squeeze out all singleton
        dimensions and also convert a zero dimensional array into array scalar
        """
        if not self._squeeze:
            return array
        array = array.copy()
        array = array.squeeze()
        if array.ndim == 0:
            array = array[()]
        return array

    def read_cell_array(self, advance=True, n_bytes=None):
        start = self.pos
        n_dims = self.read_value('uint64').item()
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        data = np.empty(n_elem, dtype=np.object)
        for i in range(n_elem):
            nb = self.read_value('uint64').item()
            data[i] = self.read_mym_data(n_bytes=nb)
        if n_bytes is not None:
            assert self.pos - start == n_bytes
        if not advance:
            self.pos = start
        return data

    def read_string(self, advance=True):
        """
        Read a string terminated by null byte '\0'. The returned string
        object is ASCII decoded, and will not include the terminating null byte.
        """
        target = self._blob.find(b'\0', self.pos)
        assert target >= self._pos
        data = self._blob[self._pos:target]
        if advance:
            self._pos = target + 1
        return data.decode('ascii')

    def read_value(self, dtype='uint64', count=1, advance=True):
        """
        Read one or more scalars of the indicated dtype. Count specifies the number of
        scalars to be read in.
        """
        data = np.frombuffer(self._blob, dtype=dtype, count=count, offset=self.pos)
        if advance:
            # probably the same thing as data.nbytes * 8
            self._pos += data.dtype.itemsize * data.size
        if count == 1:
            data = data[0]
        return data

    def __repr__(self):
        return repr(self._blob[self.pos:])

    def __str__(self):
        return str(self._blob[self.pos:])


def pack(obj, compress=True):
    blob = b"mYm\0"
    blob += pack_obj(obj)

    if compress:
        compressed = b'ZL123\0' + np.uint64(len(blob)).tostring() + zlib.compress(blob)
        if len(compressed) < len(blob):
            blob = compressed

    return blob


def pack_obj(obj):
    blob = b''
    if isinstance(obj, np.ndarray):
        blob += pack_array(obj)
    elif isinstance(obj, Mapping):  # TODO: check if this is a good inheritance check for dict etc.
        blob += pack_dict(obj)
    elif isinstance(obj, str):
        blob += pack_array(np.array(obj, dtype=np.dtype('c')))
    elif isinstance(obj, Iterable):
        blob += pack_array(np.array(list(obj)))
    elif isinstance(obj, int) or isinstance(obj, float):
        blob += pack_array(np.array(obj))
    else:
        raise DataJointError("Packing object of type %s currently not supported!" % type(obj))

    return blob


def pack_array(array):
    if not isinstance(array, np.ndarray):
        raise ValueError("argument must be a numpy array!")

    blob = b"A"
    blob += np.array((len(array.shape), ) + array.shape, dtype=np.uint64).tostring()

    is_complex = np.iscomplexobj(array)
    if is_complex:
        array, imaginary = np.real(array), np.imag(array)

    type_number = rev_class_id[array.dtype]

    if dtype_list[type_number] is None:
        raise DataJointError("Type %s is ambiguous or unknown" % array.dtype)

    blob += np.array(type_number, dtype=np.uint32).tostring()

    blob += np.int32(is_complex).tostring()
    if type_number == 4:  # if dealing with character array
        blob += ('\x00'.join(array.tostring(order='F').decode()) + '\x00').encode()
    else:
        blob += array.tostring(order='F')

    if is_complex:
        blob += imaginary.tostring(order='F')

    return blob


def pack_string(value):
    return value.encode('ascii') + b'\0'


def pack_dict(obj):
    """
    Write dictionary object as a singular structure array
    :param obj: dictionary object to serialize. The fields must be simple scalar or an array.
    """
    obj = OrderedDict(obj)
    blob = b'S'
    blob += np.array((1, 1), dtype=np.uint64).tostring()
    blob += np.array(len(obj), dtype=np.uint32).tostring()

    # write out field names
    for k in obj:
        blob += pack_string(k)

    for k, v in obj.items():
        blob_part = pack_obj(v)
        blob += np.array(len(blob_part), dtype=np.uint64).tostring()
        blob += blob_part

    return blob


def unpack(blob, **kwargs):
    if blob is None:
        return None

    return BlobReader(blob, **kwargs).unpack()

