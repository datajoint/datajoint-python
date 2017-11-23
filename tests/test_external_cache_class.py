
from nose.tools import raises
import numpy as np

import datajoint as dj

from datajoint.external import CacheFileHandler

from . import schema_cache as modu

[modu]  # flakes.


class MemHandler(dj.external.ExternalFileHandler):
    '''
    dummy in-memory handler for objects
    XXX: useful?
    '''

    def __init__(self, store, database):
        super().__init__(store, database)
        self._backend = {}

    def put(self, obj):
        (blob, hash) = self.hash_obj(obj)
        self._backend[hash] = blob
        return (blob, hash)

    def get(self, hash):
        return self._backend[hash]


class CacheMemHandler(CacheFileHandler, MemHandler):
    pass


@raises(NotImplementedError)
def test_cache_put_needs_impl():
    cache = CacheFileHandler('external-cache', 'bogus_test_database')
    dat = np.random.randn(3, 7, 8)
    cache.put(dat)


@raises(NotImplementedError)
def test_cache_get_needs_impl():
    cache = CacheFileHandler('external-cache', 'bogus_test_database')
    dat = np.random.randn(3, 7, 8)
    (blob, hash) = cache.hash_obj(dat)
    cache.get(hash)


def test_cache_put():
    cache = CacheMemHandler('external-cache', 'bogus_test_database')
    dat = np.random.randn(3, 7, 8)
    cache.put(dat)


def test_cache_get():
    cache = CacheMemHandler('external-cache', 'bogus_test_database')
    dat = np.random.randn(3, 7, 8)
    (blob, hash) = cache.put(dat)
    ndat = cache.get(hash)
    assert(np.array_equal(dat, ndat))
