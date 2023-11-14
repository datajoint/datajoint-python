# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_external.py
# Compiled at: 2023-02-19 06:56:42
# Size of source mod 2**32: 3913 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint.external import ExternalTable
from datajoint.blob import pack, unpack
import os, numpy as np
from numpy.testing import assert_array_equal
from . import connection_root, connection_test, bucket
from schemas.external import schema, stores, store_raw, store_local, store_share, Simple, SimpleRemote

def test_external_put(schema, store_raw):
    """
    external storage put and get and remove
    """
    np.random.seed(600)
    ext = ExternalTable((schema.connection), store='raw', database=(schema.database))
    initial_length = len(ext)
    input_ = np.random.randn(3, 7, 8)
    count = 7
    extra = 3
    for i in range(count):
        hash1 = ext.put(pack(input_))

    for i in range(extra):
        hash2 = ext.put(pack(np.random.randn(4, 3, 2)))

    fetched_hashes = ext.fetch('hash')
    @py_assert1 = (hash in fetched_hashes for hash in (hash1, hash2))
    @py_assert3 = all(@py_assert1)
    if not @py_assert3:
        @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n}' % {'py0':@pytest_ar._saferepr(all) if 'all' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(all) else 'all',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = @py_assert3 = None
    @py_assert2 = len(ext)
    @py_assert6 = 1
    @py_assert8 = initial_length + @py_assert6
    @py_assert10 = @py_assert8 + extra
    @py_assert4 = @py_assert2 == @py_assert10
    if not @py_assert4:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == ((%(py5)s + %(py7)s) + %(py9)s)', ), (@py_assert2, @py_assert10)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(initial_length) if 'initial_length' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(initial_length) else 'initial_length',  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(extra) if 'extra' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(extra) else 'extra'}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = None
    output_ = unpack(ext.get(hash1))
    assert_array_equal(input_, output_)


def test_s3_leading_slash(store_share, SimpleRemote):
    """
    s3 external storage configured with leading slash
    """
    store_name = store_share
    table = SimpleRemote
    value = np.array([1, 2, 3])
    id = 100
    dj.config['stores'][store_name]['location'] = 'leading/slash/test'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = '/leading/slash/test'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = 'leading\\slash\\test'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = 'f:\\leading\\slash\\test'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = 'f:\\leading/slash\\test'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))


def test_s3_leading_slash_root(store_share, SimpleRemote):
    """
    s3 external storage configured with leading slash at root
    """
    store_name = store_share
    table = SimpleRemote
    value = np.array([1, 2, 3])
    id = 100
    dj.config['stores'][store_name]['location'] = '/'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = 'C:\\'
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))
    id += 1
    dj.config['stores'][store_name]['location'] = ''
    table.insert([{'simple':id,  'item':value}])
    assert_array_equal(value, (table & 'simple={}'.format(id)).fetch1('item'))


def test_remove_fail(schema, Simple):
    data = dict(simple=2, item=[1, 2, 3])
    Simple.insert1(data)
    path1 = dj.config['stores']['local']['location'] + '/djtest_extern/4/c/'
    currentMode = int(oct(os.stat(path1).st_mode), 8)
    os.chmod(path1, 16749)
    (Simple & 'simple=2').delete()
    listOfErrors = schema.external['local'].delete(delete_external_files=True)
    @py_assert2 = len(listOfErrors)
    @py_assert5 = 1
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(listOfErrors) if 'listOfErrors' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(listOfErrors) else 'listOfErrors',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = (@pytest_ar._format_assertmsg('unexpected number of errors') + '\n>assert %(py8)s') % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert4 = @py_assert5 = None
    @py_assert1 = schema.external['local']
    @py_assert4 = listOfErrors[0][0]
    @py_assert6 = dict(hash=@py_assert4)
    @py_assert8 = @py_assert1 & @py_assert6
    @py_assert9 = len(@py_assert8)
    @py_assert12 = 1
    @py_assert11 = @py_assert9 == @py_assert12
    if not @py_assert11:
        @py_format14 = @pytest_ar._call_reprcompare(('==', ), (@py_assert11,), ('%(py10)s\n{%(py10)s = %(py0)s((%(py2)s & %(py7)s\n{%(py7)s = %(py3)s(hash=%(py5)s)\n}))\n} == %(py13)s', ), (@py_assert9, @py_assert12)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(dict) if 'dict' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dict) else 'dict',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py10':@pytest_ar._saferepr(@py_assert9),  'py13':@pytest_ar._saferepr(@py_assert12)}
        @py_format16 = (@pytest_ar._format_assertmsg('unexpected number of rows in external table') + '\n>assert %(py15)s') % {'py15': @py_format14}
        raise AssertionError(@pytest_ar._format_explanation(@py_format16))
    @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert9 = @py_assert11 = @py_assert12 = None
    os.chmod(path1, currentMode)