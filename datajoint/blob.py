import zlib
import collections
import numpy as np
from .core import DataJointError


mxClassID = collections.OrderedDict(
    # see http://www.mathworks.com/help/techdoc/apiref/mxclassid.html
    mxUNKNOWN_CLASS = None,
    mxCELL_CLASS    = None,   # not yet implemented
    mxSTRUCT_CLASS  = None,   # not yet implemented
    mxLOGICAL_CLASS = np.dtype('bool'),
    mxCHAR_CLASS    = np.dtype('c'),
    mxVOID_CLASS    = None,
    mxDOUBLE_CLASS  = np.dtype('float64'),
    mxSINGLE_CLASS  = np.dtype('float32'),
    mxINT8_CLASS    = np.dtype('int8'),
    mxUINT8_CLASS   = np.dtype('uint8'),
    mxINT16_CLASS   = np.dtype('int16'),
    mxUINT16_CLASS  = np.dtype('uint16'),
    mxINT32_CLASS   = np.dtype('int32'),
    mxUINT32_CLASS  = np.dtype('uint32'),
    mxINT64_CLASS   = np.dtype('int64'),
    mxUINT64_CLASS  = np.dtype('uint64'),
    mxFUNCTION_CLASS= None
    )

reverseClassID = {v:i for i,v in enumerate(mxClassID.values())}


def pack(obj):
    """
    packs an object into a blob to be compatible with mym.mex
    """
    if not isinstance(obj, np.ndarray):
        raise DataJointError("Only numpy arrays can be saved in blobs")

    blob = b"mYm\0A"  # TODO: extend to process other datatypes besides arrays
    blob += np.asarray((len(obj.shape),)+obj.shape,dtype=np.uint64).tostring()

    isComplex = np.iscomplexobj(obj)
    if isComplex:
        obj, objImag = np.real(obj), np.imag(obj)

    typeNum = reverseClassID[obj.dtype]
    blob+= np.asarray(typeNum, dtype=np.uint32).tostring()
    blob+= np.int8(isComplex).tostring() + b'\0\0\0'
    blob+= obj.tostring()

    if isComplex:
        blob+= objImag.tostring()

    if len(blob)>1000:
        compressed = b'ZL123\0'+np.asarray(len(blob),dtype=np.uint64).tostring() + zlib.compress(blob)
        if len(compressed) < len(blob):
            blob = compressed
    return blob



def unpack(blob):
    """
    unpack blob into a numpy array
    """
    # decompress if necessary
    if blob[0:5]==b'ZL123':
        blobLen = np.fromstring(blob[6:14],dtype=np.uint64)[0]
        blob = zlib.decompress(blob[14:])
        assert(len(blob)==blobLen)

    blobType = blob[4]
    if blobType!=65:  # TODO: also process structure arrays, cell arrays, etc.
        raise DataJointError('only arrays are currently allowed in blobs')
    p = 5
    ndims = np.fromstring(blob[p:p+8], dtype=np.uint64)
    p += 8
    arrDims = np.fromstring(blob[p:p+8*ndims], dtype=np.uint64)
    p += 8 * ndims
    mxType, dtype = [q for q in mxClassID.items()][np.fromstring(blob[p:p+4],dtype=np.uint32)[0]]
    if dtype is None:
        raise DataJointError('Unsupported matlab datatype '+mxType+' in blob')
    p += 4
    complexity = np.fromstring(blob[p:p+4],dtype=np.uint32)[0]
    p += 4
    obj = np.fromstring(blob[p:], dtype=dtype)
    if complexity:
        obj = obj[:len(obj)/2] + 1j*obj[len(obj)/2:]
        
    return obj.reshape(arrDims)