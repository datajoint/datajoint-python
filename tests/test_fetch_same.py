# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_fetch_same.py
# Compiled at: 2023-02-19 07:13:21
# Size of source mod 2**32: 1723 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, numpy as np, pytest
from . import PREFIX, connection_root, connection_test

@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + '_fetch_same'), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def ProjData(schema):
    np.random.seed(700)

    @schema
    class ProjData(dj.Lookup):
        definition = '\n        id : int\n        ---\n        resp : float\n        sim  : float\n        big : longblob\n        blah : varchar(10)\n        '
        contents = [
         {
           'id': 0, 'resp': 20.33, 'sim': 45.324, 'big': 3, 'blah': 'yes'},
         {'id':1, 
          'resp':94.3, 
          'sim':34.23, 
          'big':{'key1': np.random.randn(20, 10)}, 
          'blah':'si'},
         {'id':2, 
          'resp':1.9, 
          'sim':10.23, 
          'big':np.random.randn(4, 2), 
          'blah':'sim'}]

    yield ProjData
    ProjData.drop()


def test_object_conversion_one(ProjData):
    new = ProjData.proj(sub='resp').fetch('sub')
    @py_assert1 = new.dtype
    @py_assert5 = np.float64
    @py_assert3 = @py_assert1 == @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py6)s\n{%(py6)s = %(py4)s.float64\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(new) if 'new' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(new) else 'new',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None


def test_object_conversion_two(ProjData):
    sub, add = ProjData.proj(sub='resp', add='sim').fetch('sub', 'add')
    @py_assert1 = sub.dtype
    @py_assert5 = np.float64
    @py_assert3 = @py_assert1 == @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py6)s\n{%(py6)s = %(py4)s.float64\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(sub) if 'sub' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(sub) else 'sub',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    @py_assert1 = add.dtype
    @py_assert5 = np.float64
    @py_assert3 = @py_assert1 == @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py6)s\n{%(py6)s = %(py4)s.float64\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(add) if 'add' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(add) else 'add',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None


def test_object_conversion_all(ProjData):
    new = ProjData.proj(sub='resp', add='sim').fetch()
    @py_assert0 = new['sub']
    @py_assert2 = @py_assert0.dtype
    @py_assert6 = np.float64
    @py_assert4 = @py_assert2 == @py_assert6
    if not @py_assert4:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py1)s.dtype\n} == %(py7)s\n{%(py7)s = %(py5)s.float64\n}', ), (@py_assert2, @py_assert6)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert6 = None
    @py_assert0 = new['add']
    @py_assert2 = @py_assert0.dtype
    @py_assert6 = np.float64
    @py_assert4 = @py_assert2 == @py_assert6
    if not @py_assert4:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py1)s.dtype\n} == %(py7)s\n{%(py7)s = %(py5)s.float64\n}', ), (@py_assert2, @py_assert6)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert6 = None


def test_object_no_convert(ProjData):
    new = ProjData.fetch()
    @py_assert0 = new['big']
    @py_assert2 = @py_assert0.dtype
    @py_assert5 = 'object'
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py1)s.dtype\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert5 = None
    @py_assert0 = new['blah']
    @py_assert2 = @py_assert0.dtype
    @py_assert5 = 'object'
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py1)s.dtype\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert5 = None