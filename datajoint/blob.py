import zlib
import collections
import numpy as np
from core import *


def pack(obj):
    """
    packs an object into a blob similar for compatibility with mym.mex
    """
    if not isinstance(obj, np.ndarray):
        raise DataJointError("Only numpy arrays can be saved in blobs")

    blob = "mYm\0A"
    blob+= np.asarray((len(obj.shape),)+obj.shape,dtype=np.uint64).tostring()

    isComplex = np.iscomplexobj(obj)
    if isComplex:
        objImag = np.imag(obj)
        obj = np.real(obj)

    typeNum = reverseClassID[obj.dtype]
    blob+= np.asarray(typeNum, dtype = np.uint32).tostring()

    if isComplex:
        blob+= '\1\0\0\0'
    else:
        blob+= '\0\0\0\0'

    blob += obj.tostring()

    if isComplex:
        blob+= objImag.tostring()
    
    if len(blob)>1000:
        compressed = 'ZL123\0'+np.asarray(len(blob),dtype=np.uint64).tostring() + zlib.compress(blob) 
        if len(compressed) < len(blob):
            blob = compressed
    return blob



def unpack(blob):
    # decompess if necessary
    if blob[0:5]=='ZL123':
        blobLen = np.fromstring(blob[6:14],dtype=np.uint64)[0]
        blob = zlib.decompress(blob[14:])
        assert(len(blob)==blobLen)
    
    structure = blob[4]
    if structure<>'A': 
        raise DataJointError('only arrays are currently allowed in blobs')

    p = 5
    ndims = np.fromstring(blob[p:p+8], dtype=np.uint64)
    p+=8
    arrDims = np.fromstring(blob[p:p+8*ndims], dtype=np.uint64)
    p+=8*ndims
    mxType, dtype = mxClassID.items()[np.fromstring(blob[p:p+4],dtype=np.uint32)[0]]
    p+=4
    complexity = np.fromstring(blob[p:p+4],dtype=np.uint32)[0]
    p+=4
    obj = np.fromstring(blob[p:], dtype=dtype)
    if complexity:
        obj = obj[:len(obj)/2] + 1j*obj[len(obj)/2:]
    obj = obj.reshape(arrDims)
    
    return obj



mxClassID = collections.OrderedDict(
    # from http://www.mathworks.com/help/techdoc/apiref/mxclassid.html
    mxUNKNOWN_CLASS = None,
    mxCELL_CLASS = None,
    mxSTRUCT_CLASS = None,
    mxLOGICAL_CLASS = np.dtype('bool'),
    mxCHAR_CLASS = np.dtype('c'),
    mxVOID_CLASS = None,
    mxDOUBLE_CLASS = np.dtype('float64'),
    mxSINGLE_CLASS = np.dtype('float32'),
    mxINT8_CLASS = np.dtype('int8'),
    mxUINT8_CLASS = np.dtype('uint8'),
    mxINT16_CLASS = np.dtype('int16'),
    mxUINT16_CLASS = np.dtype('uint16'),
    mxINT32_CLASS = np.dtype('int32'),
    mxUINT32_CLASS = np.dtype('uint32'),
    mxINT64_CLASS = np.dtype('int64'),
    mxUINT64_CLASS = np.dtype('uint64'),
    mxFUNCTION_CLASS = None
)

reverseClassID = {v:i for i,v in enumerate(mxClassID.values())}
