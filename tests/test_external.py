import numpy as np
from numpy.testing import assert_array_equal
from nose.tools import assert_true, assert_equal
import datajoint as dj
from datajoint.external import ExternalTable

from . import PREFIX, CONN_INFO


dj.config['external'] = {
    'protocol': 'file',
    'location': 'dj-store/external'}

dj.config['external-raw'] = {
    'protocol': 'file',
    'location': 'dj-store/raw'}

dj.config['external-compute'] = {
    'protocol': 's3',
    'location': '/datajoint-projects/test',
    'user': 'dimitri',
    'token': '2e05709792545ce'
}

dj.config['cache'] = {
    'protocol': 'file',
    'location': '/media/dimitri/ExtraDrive1/dj-store/cache'}


schema = dj.schema(PREFIX + '_external_test1', locals(), connection=dj.conn(**CONN_INFO))


def test_external_put_get_remove():
    """
    external storage put and get and remove
    """
    ext = ExternalTable(schema.connection, schema.database)
    input_ = np.random.randn(3, 7, 8)
    count = 7
    extra = 3
    for i in range(count):
        hash1 = ext.put('external-raw', input_)
    for i in range(extra):
        hash2 = ext.put('external-raw', np.random.randn(4, 3, 2))

    assert_true(hash1 in ext.fetch('hash'))
    assert_equal(count, (ext & {'hash': hash1}).fetch1('count'))
    assert_equal(len(ext), 1 + extra)

    output_ = ext.get(hash1)
    assert_array_equal(input_, output_)

    ext.remove(hash1)
    assert_equal(count-1, (ext & {'hash': hash1}).fetch1('count'))
