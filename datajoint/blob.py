import zlib
from collections import OrderedDict
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

reverseClassID = {dtype: i for i, dtype in enumerate(mxClassID.values())}
dtypeList = list(mxClassID.values())


def pack(obj):
    """
    packs an object into a blob to be compatible with mym.mex
    """
    if not isinstance(obj, np.ndarray):
        raise DataJointError("Only numpy arrays can be saved in blobs")

    blob = b"mYm\0A"  # TODO: extend to process other data types besides arrays
    blob += np.asarray((len(obj.shape),) + obj.shape, dtype=np.uint64).tostring()

    is_complex = np.iscomplexobj(obj)
    if is_complex:
        obj, imaginary = np.real(obj), np.imag(obj)

    type_number = reverseClassID[obj.dtype]
    assert dtypeList[type_number] is obj.dtype, 'ambigous or unknown array type'
    blob += np.asarray(type_number, dtype=np.uint32).tostring()
    blob += np.int8(is_complex).tostring() + b'\0\0\0'
    blob += obj.tostring()

    if is_complex:
        blob += imaginary.tostring()

    compressed = b'ZL123\0' + np.uint64(len(blob)).tostring() + zlib.compress(blob)
    if len(compressed) < len(blob):
        blob = compressed
    return blob


def unpack(blob):
    """
    unpack blob into a numpy array
    """
    # decompress if necessary
    if blob[0:5] == b'ZL123':
        blob_length = np.fromstring(blob[6:14], dtype=np.uint64)[0]
        blob = zlib.decompress(blob[14:])
        assert len(blob) == blob_length

    blob_type = blob[4]
    if blob_type != 65:  # TODO: also process structure arrays, cell arrays, etc.
        raise DataJointError('only arrays are currently allowed in blobs')
    p = 5
    dimensions = np.fromstring(blob[p:p+8], dtype=np.uint64)
    p += 8
    array_shape = np.fromstring(blob[p:p+8*dimensions], dtype=np.uint64)
    p += 8 * dimensions
    type_number = np.fromstring(blob[p:p+4], dtype=np.uint32)[0]
    dtype = dtypeList[type_number]
    if dtype is None:
        raise DataJointError('Unsupported MATLAB data type '+type_number+' in blob')
    p += 4
    is_complex = np.fromstring(blob[p:p+4], dtype=np.uint32)[0]
    p += 4
    obj = np.fromstring(blob[p:], dtype=dtype)
    if is_complex:
        obj = obj[:len(obj)/2] + 1j*obj[len(obj)/2:]
    return obj.reshape(array_shape)