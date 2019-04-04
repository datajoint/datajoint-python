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
from datetime import datetime, date, time

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

def len_u64(obj):
    return np.uint64(len(obj)).tobytes()

class MatCell(np.ndarray):
    """ a numpy ndarray representing a Matlab cell array """
    pass


class MatStruct(np.recarray):
    """ numpy.recarray representing a Matlab struct array """
    pass


class Blob:
    def __init__(self, blob, squeeze=False, as_dict=False):
        self._squeeze = squeeze
        self._blob = blob
        self._pos = 0

    def decompress(self):
        try:
            pattern = next(p for p in decode_lookup if self._blob[self._pos:].startswith(p))
        except:
            pass  # assume uncompressed
        else:
            self._pos += len(pattern)
            blob_size = self.read_value('uint64')
            blob = decode_lookup[pattern](self._blob[self._pos:])
            assert len(blob) == blob_size
            self._blob = blob
            self._pos = 0
            
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

    def unpack(self):
        self.decompress()
        blob_format = self.read_zero_terminated_string()
        if blob_format in ('mYm', 'dj0'):
            return self.read_blob(n_bytes=len(self._blob) - self._pos)

    def read_blob(self, n_bytes):
        start = self._pos
        data_structure_code = self.read_value('c').decode()
        try:
            call = {
                # MATLAB-compatible, inherited from original mYm
                'A': self.read_array,    # matlab numeric arrays
                'P': self.read_sparse_array,  # matlab sparse array
                'S': self.read_struct,   # matlab struct array
                'C': self.read_cell_array,  # matlab cell array
                # Python-native
                's': self.read_string,   # UTF8 encoded string
                'l': self.read_tuple,    # an iterable (tuple, list, set), decoded as a tuple
                'd': self.read_dict,     # a python dict
                'b': self.read_bytes,    # a raw bytes string
                't': self.read_datetime  # date, time, or datetime
            }[data_structure_code]
        except KeyError:
            raise DataJointError('Unknown data structure code "%s"' % data_structure_code)
        v = call()
        if self._pos - start != n_bytes:
            raise DataJointError('Blob length did not match')
        return v

    @staticmethod
    def pack_blob(obj):
        """serialize object"""
        if isinstance(obj, bytes):
            return Blob.pack_bytes(obj)
        if isinstance(obj, MatCell):
            return Blob.pack_cell(obj)
        if isinstance(obj, MatStruct):
            return Blob.pack_struct(obj)
        if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
            return Blob.pack_datetime(obj)
        if isinstance(obj, (np.ndarray, np.number)):  # np.number provides np.ndarray interface
            return Blob.pack_array(obj)
        if isinstance(obj, (bool, np.bool)):
            return Blob.pack_array(np.array(obj))
        if isinstance(obj, (float, Decimal)):
            return Blob.pack_array(np.float64(obj))
        if isinstance(obj, int):
            return Blob.pack_array(np.int64(obj))
        if isinstance(obj, Mapping):
            return Blob.pack_dict(obj)
        if isinstance(obj, str):
            return Blob.pack_string(obj)
        if isinstance(obj, Iterable):
            return Blob.pack_tuple(obj)
        raise DataJointError("Packing object of type %s currently not supported!" % type(obj))


    def read_array(self):
        n_dims = int(self.read_value('uint64'))
        shape = self.read_value('uint64', count=n_dims)
        n_elem = np.prod(shape, dtype=int)
        dtype_id, is_complex = self.read_value('uint32', 2)
        dtype = dtype_list[dtype_id]

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

    @staticmethod
    def pack_array(array):
        """
        Serialize a np.ndarray or np.number object into bytes.
        Scalars are encoded with ndim=0.
        """
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

    def read_sparse_array(self):
        raise DataJointError('datajoint-python does not yet support sparse arrays. Issue (#590)')

    def read_string(self):
        return self.read_binary(self.read_value()).decode()

    @staticmethod
    def pack_string(s):
        blob = s.encode()
        return len_u64(blob) + blob
    
    def read_bytes(self):
        return self.read_binary(self.read_value())
    
    @staticmethod
    def pack_bytes(s):
        return len_u64(s) + s

    def read_dict(self):
        return {self.read_blob(self.read_value()): self.read_blob(self.read_value()) for _ in range(self.read_value())}

    @staticmethod
    def pack_dict(d):
        return b"d" + len_u64(d) + b"".join(
            b"".join((len_u64(it) + it) for it in packed)
            for packed in (map(Blob.pack_blob, pair) for pair in d.items()))

    def read_tuple(self):
        return tuple(self.read_blob(self.read_value()) for _ in range(self.read_value()))

    @staticmethod
    def pack_tuple(t):
        return b"l" + len_u64(t) + b"".join(
            len_u64(it) + it for it in (Blob.pack_blob(i) for i in t))

    def read_struct(self):
        """deserialize matlab stuct"""
        n_dims = self.read_value()
        shape = self.read_value(count=n_dims)
        n_elem = np.prod(shape, dtype=int)
        n_field = self.read_value('uint32')
        if not n_field:
            return np.array(None)  # empty array
        field_names = [self.read_zero_terminated_string() for _ in range(n_field)]
        raw_data = [
            tuple(self.read_blob(n_bytes=int(self.read_value('uint64'))) for _ in range(n_field))
            for __ in range(n_elem)]
        data = MatStruct(raw_data, dtype=list(zip(field_names, repeat(np.object))))
        return self.squeeze(data.reshape(shape, order='F'))

    @staticmethod
    def pack_struct(array):
        """ Serialize a Matlab struct array """
        return (b"S" + np.array((array.ndim,) + array.shape, dtype=np.uint64).tobytes() +  # dimensionality
                len_u64(array) +  # number of fields
                b"".join(map(lambda x: x.encode() + b'\0', array)) +  # field names
                b"".join(len_u64(it) + it for it in (
                    Blob.pack_blob(e) for rec in array.flatten() for e in rec)))  # values

    def read_cell_array(self):
        """ deserialize MATLAB cell array """
        n_dims = self.read_value()
        shape = self.read_value('uint64', count=n_dims)
        n_elem = int(np.prod(shape))
        return self.squeeze(MatCell(
            [self.read_blob(n_bytes=self.read_value()) for _ in range(n_elem)], dtype=np.object))

    @staticmethod
    def pack_cell_array(array):
        return (b"C" + np.array((array.ndim,) + array.shape, dtype=np.uint64).tobytes() +
                
                )

    def read_zero_terminated_string(self):
        target = self._blob.find(b'\0', self._pos)
        data = self._blob[self._pos:target].decode()
        self._pos = target + 1
        return data
        
    def read_value(self, dtype='uint64', count=1):
        data = np.frombuffer(self._blob, dtype=dtype, count=count, offset=self._pos)
        self._pos += data.dtype.itemsize * data.size
        return data[0] if count == 1 else data

    def read_binary(self, size):
        self._pos += size
        return self._blob[self._pos-size:self._pos]

    def __repr__(self):
        return repr(self._blob[self._pos:])

    def __str__(self):
        return str(self._blob[self._pos:])


    def set_dj0(self):
        self.protocol = b"dj0\0"   # if using new blob features

    def pack(self, obj):
        self.protocol = b"mYm\0"  # may be replaced with dj0 if new features are used
        blob = self.pack_blob(obj)  # this may reset the protocol and must precede protocol evaluation
        blob = self.protocol + blob
        if self.compress and len(blob) > 1000:
            compressed = b'ZL123\0' + len_u64(blob) + zlib.compress(blob)
            if len(compressed) < len(blob):
                blob = compressed
        return blob






def pack(obj, compress=True):
    return Blob().pack(obj, compress=compress)

def unpack(blob, **kwargs):
    if blob is not None:
        return Blob(blob, **kwargs).unpack()



