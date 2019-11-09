import numpy as np
from numpy.testing import assert_array_equal
from nose.tools import assert_true, assert_equal
from datajoint.external import ExternalTable
from datajoint.blob import pack, unpack

from .schema_external import schema
import datajoint as dj
from .schema_external import stores_config
from .schema_external import SimpleRemote
current_location_s3 = dj.config['stores']['share']['location']
current_location_local = dj.config['stores']['local']['location']


def setUp(self):
    dj.config['stores'] = stores_config


def tearDown(self):
    dj.config['stores']['share']['location'] = current_location_s3
    dj.config['stores']['local']['location'] = current_location_local


def test_external_put():
    """
    external storage put and get and remove
    """
    ext = ExternalTable(schema.connection, store='raw', database=schema.database)
    initial_length = len(ext)
    input_ = np.random.randn(3, 7, 8)
    count = 7
    extra = 3
    for i in range(count):
        hash1 = ext.put(pack(input_))
    for i in range(extra):
        hash2 = ext.put(pack(np.random.randn(4, 3, 2)))

    fetched_hashes = ext.fetch('hash')
    assert_true(all(hash in fetched_hashes for hash in (hash1, hash2)))
    assert_equal(len(ext), initial_length + 1 + extra)

    output_ = unpack(ext.get(hash1))
    assert_array_equal(input_, output_)


def test_s3_leading_slash(index=100, store='share'):
    """
    s3 external storage configured with leading slash
    """
    value = np.array([1, 2, 3])

    id = index
    dj.config['stores'][store]['location'] = 'leading/slash/test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 1
    dj.config['stores'][store]['location'] = '/leading/slash/test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 2
    dj.config['stores'][store]['location'] = 'leading\\slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 3
    dj.config['stores'][store]['location'] = 'f:\\leading\\slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 4
    dj.config['stores'][store]['location'] = 'f:\\leading/slash\\test'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 5
    dj.config['stores'][store]['location'] = '/'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 6
    dj.config['stores'][store]['location'] = 'C:\\'
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))

    id = index + 7
    dj.config['stores'][store]['location'] = ''
    SimpleRemote.insert([{'simple': id, 'item': value}])
    assert_true(np.array_equal(
        value, (SimpleRemote & 'simple={}'.format(id)).fetch1('item')))


def test_file_leading_slash():
    """
    file external storage configured with leading slash
    """
    test_s3_leading_slash(index=200, store='local')
