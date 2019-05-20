import copy
import numpy as np
from datajoint.blob import MatStruct
from collections.abc import Iterable
import importlib
try:
    dj = importlib.import_module('datajoint-python.datajoint', None)
except:
    import datajoint as dj
import datetime as dt


def convert_blob(*blob_fields, mode='forward'):

    ret = [None]*len(blob_fields)
    ret = np.array(ret)

    for p, blob_values in enumerate(blob_fields):

        res = [None]*len(blob_values)
        res = np.array(res)

        for i, value in enumerate(blob_values):
            res[i] = _convert_blob_single(value, mode)

        ret[p] = res

    if len(blob_fields) == 1:
        ret = ret[0]

    return ret


def _convert_blob_single(blob_value, mode='forward'):
    if type(blob_value) is dict:
        ret = copy.deepcopy(blob_value)
    elif type(blob_value) is list:
        ret = blob_value[:]
    elif type(blob_value) is np.ndarray:
        ret = np.copy(blob_value)
    elif type(blob_value) is tuple or type(blob_value) is set:
        ret = list(blob_value)
    else:
        ret = blob_value

    if type(ret) is dict and mode == 'backward':
        ret = np.array([tuple(ret.values())], dtype=[(
                x, 'O') for x in ret.keys()]).view(np.recarray)
    elif type(ret) is dj.MatStruct and mode == 'forward':
        ret = [dict(zip(ret.dtype.names, x)) for x in ret][0]
    elif type(ret) is dj.MatStruct and mode == 'backward':
        ret = ret.view(np.recarray)

    if type(ret) is dj.blob.MatStruct or type(ret) is np.recarray:
        ret_iter = ret.dtype.names
    else:
        ret_iter = ret

    try:
        for i, n in enumerate(ret_iter):

            if type(ret) is dict or type(ret) is dj.blob.MatStruct or type(
                    ret) is np.recarray:
                val = ret[n]
                z = n
            else:
                val = n
                z = i

            if not isinstance(val, str) and isinstance(val, Iterable):
                iter_val = iter(val)
                ret[z] = _convert_blob_single(val, mode)
            else:
                if isinstance(val, str) and mode == 'backward':
                    ret[z] = np.array([val])
                elif isinstance(val, dt.date) and mode == 'backward':
                    ret[z] = np.array([str(val)])
                else:
                    ret[z] = val

    except TypeError:
        if isinstance(ret, str) and mode == 'backward':
            ret = np.array([ret])
        elif isinstance(ret, dt.date) and mode == 'backward':
            ret = np.array([str(ret)])

    if mode == 'forward':
        if type(blob_value) is tuple:
            return tuple(ret)
        elif type(blob_value) is set:
            return set(ret)
    else:
        if type(blob_value) is list:
            return np.array(ret)
        elif type(blob_value) is tuple or type(blob_value) is set:
            return np.array(list(ret))

    return ret
