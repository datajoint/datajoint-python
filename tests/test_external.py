import numpy as np
from numpy.testing import assert_array_equal
from nose.tools import assert_true, assert_equal
from datajoint.external import ExternalTable
from datajoint.blob import pack, unpack

from . schema_external import schema


def test_external_put():
    """
    external storage put and get and remove
    """
    ext = ExternalTable(schema.connection, schema.database)
    input_ = np.random.randn(3, 7, 8)
    count = 7
    extra = 3
    for i in range(count):
        hash1 = ext.put('external-raw', pack(input_))
    for i in range(extra):
        hash2 = ext.put('external-raw', pack(np.random.randn(4, 3, 2)))

    fetched_hashes = ext.fetch('hash')
    assert_true(all(hash in fetched_hashes for hash in (hash1, hash2)))
    assert_equal(len(ext), 1 + extra)

    output_ = unpack(ext.get(hash1))
    assert_array_equal(input_, output_)
