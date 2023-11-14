# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_blob.py
# Compiled at: 2023-02-17 22:18:42
# Size of source mod 2**32: 8131 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint.blob import pack, unpack
import numpy as np, uuid, timeit
from datetime import datetime
from decimal import Decimal
from numpy.testing import assert_array_equal
import pytest
from . import connection_root, connection_test
from schemas.default import schema, Longblob

def test_pack():
    np.random.seed(100)
    for x in (
     32,
     -0.037,
     np.float64(3e+31),
     -np.inf,
     np.int8(-3),
     np.uint8(-1),
     np.int16(-33),
     np.uint16(-33),
     np.int32(-3),
     np.uint32(-1),
     np.int64(373),
     np.uint64(-3)):
        @py_assert5 = pack(x)
        @py_assert7 = unpack(@py_assert5)
        @py_assert1 = x == @py_assert7
        if not @py_assert1:
            @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_format11 = (@pytest_ar._format_assertmsg("Scalars don't match!") + '\n>assert %(py10)s') % {'py10': @py_format9}
            raise AssertionError(@pytest_ar._format_explanation(@py_format11))
        else:
            @py_assert1 = @py_assert5 = @py_assert7 = None

    x = np.nan
    @py_assert1 = np.isnan
    @py_assert6 = pack(x)
    @py_assert8 = unpack(@py_assert6)
    @py_assert10 = @py_assert1(@py_assert8)
    if not @py_assert10:
        @py_format12 = (@pytest_ar._format_assertmsg('nan scalar did not match!') + '\n>assert %(py11)s\n{%(py11)s = %(py2)s\n{%(py2)s = %(py0)s.isnan\n}(%(py9)s\n{%(py9)s = %(py3)s(%(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n})\n})\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py4':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py5':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert1 = @py_assert6 = @py_assert8 = @py_assert10 = None
    x = np.random.randn(8, 10)
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')
    x = np.random.randn(10)
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')
    x = complex(0.0, 7.0)
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Complex scalar does not match') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')
    x = np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')
    x = None
    @py_assert3 = pack(x)
    @py_assert5 = unpack(@py_assert3)
    @py_assert8 = None
    @py_assert7 = @py_assert5 is @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('is', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s(%(py4)s\n{%(py4)s = %(py1)s(%(py2)s)\n})\n} is %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py1':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = (@pytest_ar._format_assertmsg('None did not match') + '\n>assert %(py11)s') % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert3 = @py_assert5 = @py_assert7 = @py_assert8 = None
    x = -255
    y = unpack(pack(x))
    @py_assert1 = []
    @py_assert3 = x == y
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert11 = isinstance(y, int)
        @py_assert0 = @py_assert11
        if @py_assert11:
            @py_assert17 = np.ndarray
            @py_assert19 = isinstance(y, @py_assert17)
            @py_assert21 = not @py_assert19
            @py_assert0 = @py_assert21
    if not @py_assert0:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py4)s', ), (x, y)) % {'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y'}
        @py_format7 = '%(py6)s' % {'py6': @py_format5}
        @py_assert1.append(@py_format7)
        if @py_assert3:
            @py_format13 = '%(py12)s\n{%(py12)s = %(py8)s(%(py9)s, %(py10)s)\n}' % {'py8':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py9':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py10':@pytest_ar._saferepr(int) if 'int' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(int) else 'int',  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_assert1.append(@py_format13)
            if @py_assert11:
                @py_format22 = 'not %(py20)s\n{%(py20)s = %(py14)s(%(py15)s, %(py18)s\n{%(py18)s = %(py16)s.ndarray\n})\n}' % {'py14':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py15':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py16':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py18':@pytest_ar._saferepr(@py_assert17),  'py20':@pytest_ar._saferepr(@py_assert19)}
                @py_assert1.append(@py_format22)
        @py_format23 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format25 = (@pytest_ar._format_assertmsg('Scalar int did not match') + '\n>assert %(py24)s') % {'py24': @py_format23}
        raise AssertionError(@pytest_ar._format_explanation(@py_format25))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert11 = @py_assert17 = @py_assert19 = @py_assert21 = None
    x = -25523987234234287910987234987098245697129798713407812347
    y = unpack(pack(x))
    @py_assert1 = []
    @py_assert3 = x == y
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert11 = isinstance(y, int)
        @py_assert0 = @py_assert11
        if @py_assert11:
            @py_assert17 = np.ndarray
            @py_assert19 = isinstance(y, @py_assert17)
            @py_assert21 = not @py_assert19
            @py_assert0 = @py_assert21
    if not @py_assert0:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py4)s', ), (x, y)) % {'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y'}
        @py_format7 = '%(py6)s' % {'py6': @py_format5}
        @py_assert1.append(@py_format7)
        if @py_assert3:
            @py_format13 = '%(py12)s\n{%(py12)s = %(py8)s(%(py9)s, %(py10)s)\n}' % {'py8':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py9':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py10':@pytest_ar._saferepr(int) if 'int' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(int) else 'int',  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_assert1.append(@py_format13)
            if @py_assert11:
                @py_format22 = 'not %(py20)s\n{%(py20)s = %(py14)s(%(py15)s, %(py18)s\n{%(py18)s = %(py16)s.ndarray\n})\n}' % {'py14':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py15':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py16':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py18':@pytest_ar._saferepr(@py_assert17),  'py20':@pytest_ar._saferepr(@py_assert19)}
                @py_assert1.append(@py_format22)
        @py_format23 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format25 = (@pytest_ar._format_assertmsg('Unbounded int did not match') + '\n>assert %(py24)s') % {'py24': @py_format23}
        raise AssertionError(@pytest_ar._format_explanation(@py_format25))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert11 = @py_assert17 = @py_assert19 = @py_assert21 = None
    x = 7.0
    y = unpack(pack(x))
    @py_assert1 = []
    @py_assert3 = x == y
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert11 = isinstance(y, float)
        @py_assert0 = @py_assert11
        if @py_assert11:
            @py_assert17 = np.ndarray
            @py_assert19 = isinstance(y, @py_assert17)
            @py_assert21 = not @py_assert19
            @py_assert0 = @py_assert21
    if not @py_assert0:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py4)s', ), (x, y)) % {'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y'}
        @py_format7 = '%(py6)s' % {'py6': @py_format5}
        @py_assert1.append(@py_format7)
        if @py_assert3:
            @py_format13 = '%(py12)s\n{%(py12)s = %(py8)s(%(py9)s, %(py10)s)\n}' % {'py8':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py9':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py10':@pytest_ar._saferepr(float) if 'float' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(float) else 'float',  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_assert1.append(@py_format13)
            if @py_assert11:
                @py_format22 = 'not %(py20)s\n{%(py20)s = %(py14)s(%(py15)s, %(py18)s\n{%(py18)s = %(py16)s.ndarray\n})\n}' % {'py14':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py15':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py16':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py18':@pytest_ar._saferepr(@py_assert17),  'py20':@pytest_ar._saferepr(@py_assert19)}
                @py_assert1.append(@py_format22)
        @py_format23 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format25 = (@pytest_ar._format_assertmsg('Scalar float did not match') + '\n>assert %(py24)s') % {'py24': @py_format23}
        raise AssertionError(@pytest_ar._format_explanation(@py_format25))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert11 = @py_assert17 = @py_assert19 = @py_assert21 = None
    x = complex(0.0, 7.0)
    y = unpack(pack(x))
    @py_assert1 = []
    @py_assert3 = x == y
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert11 = isinstance(y, complex)
        @py_assert0 = @py_assert11
        if @py_assert11:
            @py_assert17 = np.ndarray
            @py_assert19 = isinstance(y, @py_assert17)
            @py_assert21 = not @py_assert19
            @py_assert0 = @py_assert21
    if not @py_assert0:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py4)s', ), (x, y)) % {'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y'}
        @py_format7 = '%(py6)s' % {'py6': @py_format5}
        @py_assert1.append(@py_format7)
        if @py_assert3:
            @py_format13 = '%(py12)s\n{%(py12)s = %(py8)s(%(py9)s, %(py10)s)\n}' % {'py8':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py9':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py10':@pytest_ar._saferepr(complex) if 'complex' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(complex) else 'complex',  'py12':@pytest_ar._saferepr(@py_assert11)}
            @py_assert1.append(@py_format13)
            if @py_assert11:
                @py_format22 = 'not %(py20)s\n{%(py20)s = %(py14)s(%(py15)s, %(py18)s\n{%(py18)s = %(py16)s.ndarray\n})\n}' % {'py14':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py15':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y',  'py16':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py18':@pytest_ar._saferepr(@py_assert17),  'py20':@pytest_ar._saferepr(@py_assert19)}
                @py_assert1.append(@py_format22)
        @py_format23 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format25 = (@pytest_ar._format_assertmsg('Complex scalar did not match') + '\n>assert %(py24)s') % {'py24': @py_format23}
        raise AssertionError(@pytest_ar._format_explanation(@py_format25))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert11 = @py_assert17 = @py_assert19 = @py_assert21 = None
    x = True
    @py_assert3 = pack(x)
    @py_assert5 = unpack(@py_assert3)
    @py_assert8 = True
    @py_assert7 = @py_assert5 is @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('is', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s(%(py4)s\n{%(py4)s = %(py1)s(%(py2)s)\n})\n} is %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py1':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py2':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = (@pytest_ar._format_assertmsg('Scalar bool did not match') + '\n>assert %(py11)s') % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert3 = @py_assert5 = @py_assert7 = @py_assert8 = None
    x = [
     None]
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = {'name':'Anonymous', 
     'age':15, 
     99:datetime.now(), 
     'range':[
      110, 190], 
     (11, 12):None}
    y = unpack(pack(x))
    @py_assert1 = x == y
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (x, y)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(y) if 'y' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(y) else 'y'}
        @py_format5 = (@pytest_ar._format_assertmsg('Dict do not match!') + '\n>assert %(py4)s') % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    @py_assert1 = ['range'][0]
    @py_assert4 = np.ndarray
    @py_assert6 = isinstance(@py_assert1, @py_assert4)
    @py_assert8 = not @py_assert6
    if not @py_assert8:
        @py_format9 = (@pytest_ar._format_assertmsg('Scalar int was coerced into arrray.') + '\n>assert not %(py7)s\n{%(py7)s = %(py0)s(%(py2)s, %(py5)s\n{%(py5)s = %(py3)s.ndarray\n})\n}') % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = None
    x = uuid.uuid4()
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('UUID did not match') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = Decimal('-112122121.000003000')
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Decimal did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = [
     1, datetime.now(), {1:'one',  'two':2}, (1, 2)]
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('List did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = (
     1, datetime.now(), {1:'one',  'two':2}, (uuid.uuid4(), 2))
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Tuple did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = (
     1,
     {datetime.now().date(): 'today', 'now': datetime.now().date()},
     {'yes!': [1, 2, np.array((3, 4))]})
    y = unpack(pack(x))
    @py_assert0 = x[1]
    @py_assert3 = y[1]
    @py_assert2 = @py_assert0 == @py_assert3
    if not @py_assert2:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert2,), ('%(py1)s == %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert0 = @py_assert2 = @py_assert3 = None
    assert_array_equal(x[2]['yes!'][2], y[2]['yes!'][2])
    x = {
     'elephant'}
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Set did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = tuple(range(10))
    @py_assert5 = 10
    @py_assert7 = range(@py_assert5)
    @py_assert9 = pack(@py_assert7)
    @py_assert11 = unpack(@py_assert9)
    @py_assert1 = x == @py_assert11
    if not @py_assert1:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py12)s\n{%(py12)s = %(py2)s(%(py10)s\n{%(py10)s = %(py3)s(%(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n})\n})\n}', ), (x, @py_assert11)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(range) if 'range' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(range) else 'range',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = (@pytest_ar._format_assertmsg('Iterator did not pack/unpack correctly') + '\n>assert %(py14)s') % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert1 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = None
    x = Decimal('1.24')
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Decimal object did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = datetime.now()
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Datetime object did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = np.bool_(True)
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Numpy bool object did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = 'test'
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('String object did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = np.array(['yes'])
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Numpy string array object did not pack/unpack correctly') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None
    x = np.datetime64('1998').astype('datetime64[us]')
    @py_assert5 = pack(x)
    @py_assert7 = unpack(@py_assert5)
    @py_assert1 = x == @py_assert7
    if not @py_assert1:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py8)s\n{%(py8)s = %(py2)s(%(py6)s\n{%(py6)s = %(py3)s(%(py4)s)\n})\n}', ), (x, @py_assert7)) % {'py0':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py2':@pytest_ar._saferepr(unpack) if 'unpack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unpack) else 'unpack',  'py3':@pytest_ar._saferepr(pack) if 'pack' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pack) else 'pack',  'py4':@pytest_ar._saferepr(x) if 'x' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(x) else 'x',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert5 = @py_assert7 = None


def test_recarrays():
    x = np.array([(1.0, 2), (3.0, 4)], dtype=[('x', float), ('y', int)])
    assert_array_equal(x, unpack(pack(x)))
    x = x.view(np.recarray)
    assert_array_equal(x, unpack(pack(x)))
    x = np.array([(3, 4)], dtype=[('tmp0', float), ('tmp1', 'O')]).view(np.recarray)
    assert_array_equal(x, unpack(pack(x)))


def test_object_arrays():
    x = np.array(((1, 2, 3), True), dtype='object')
    assert_array_equal(x, unpack(pack(x)), 'Object array did not serialize correctly')


def test_complex():
    np.random.seed(100)
    z = np.random.randn(8, 10) + complex(0.0, 1.0) * np.random.randn(8, 10)
    assert_array_equal(z, unpack(pack(z)), 'Arrays do not match!')
    z = np.random.randn(10) + complex(0.0, 1.0) * np.random.randn(10)
    assert_array_equal(z, unpack(pack(z)), 'Arrays do not match!')
    x = np.float32(np.random.randn(3, 4, 5)) + complex(0.0, 1.0) * np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')
    x = np.int16(np.random.randn(1, 2, 3)) + complex(0.0, 1.0) * np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), 'Arrays do not match!')


insert_dj_blob = {'id':3, 
 'data':[1, 2, 3]}
query_mym_blob = {'id':2,  'data':np.array([1, 2, 3])}

@pytest.fixture
def LongblobWithData(Longblob):
    Longblob.insert1(insert_dj_blob)
    Longblob.insert1(query_mym_blob)
    yield Longblob
    Longblob.delete()


def test_insert_longblob(LongblobWithData):
    @py_assert1 = 'id=3'
    @py_assert3 = LongblobWithData & @py_assert1
    @py_assert4 = @py_assert3.fetch1
    @py_assert6 = @py_assert4()
    @py_assert8 = @py_assert6 == insert_dj_blob
    if not @py_assert8:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}()\n} == %(py9)s', ), (@py_assert6, insert_dj_blob)) % {'py0':@pytest_ar._saferepr(LongblobWithData) if 'LongblobWithData' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(LongblobWithData) else 'LongblobWithData',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(insert_dj_blob) if 'insert_dj_blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(insert_dj_blob) else 'insert_dj_blob'}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = None
    assert_array_equal((LongblobWithData & 'id=2').fetch1()['data'], query_mym_blob['data'])


@pytest.fixture
def LongblobWith32BitData(schema, Longblob):
    query_32_blob = "INSERT INTO djtest_test1.longblob (id, data) VALUES (1, X'6D596D00530200000001000000010000000400000068697473007369646573007461736B73007374616765004D000000410200000001000000070000000600000000000000000000000000F8FF000000000000F03F000000000000F03F0000000000000000000000000000F03F0000000000000000000000000000F8FF230000004102000000010000000700000004000000000000006C006C006C006C00720072006C002300000041020000000100000007000000040000000000000064006400640064006400640064002500000041020000000100000008000000040000000000000053007400610067006500200031003000')"
    schema.connection.query(query_32_blob)
    dj.blob.use_32bit_dims = True
    yield Longblob
    dj.blob.use_32bit_dims = False
    Longblob.delete()


def test_insert_longblob_32bit(LongblobWith32BitData):
    expected = np.rec.array([
     [
      (
       np.array([[np.nan, 1.0, 1.0, 0.0, 1.0, 0.0, np.nan]]),
       np.array(['llllrrl'], dtype='<U7'),
       np.array(['ddddddd'], dtype='<U7'),
       np.array(['Stage 10'], dtype='<U8'))]],
      dtype=[
     ('hits', 'O'), ('sides', 'O'), ('tasks', 'O'), ('stage', 'O')])
    fetched = (LongblobWith32BitData & 'id=1').fetch1()['data']
    assert_array_equal(fetched['hits'][0][0][0], expected['hits'][0][0][0])
    assert_array_equal(fetched['sides'], expected['sides'])
    assert_array_equal(fetched['tasks'], expected['tasks'])
    assert_array_equal(fetched['stage'], expected['stage'])


def test_datetime_serialization_speed():
    optimized_exe_time = timeit.timeit(setup="myarr=pack(np.array([np.datetime64('2022-10-13 03:03:13') for _ in range(0, 10000)]))",
      stmt='unpack(myarr)',
      number=10,
      globals=(globals()))
    print(f"optimized time {optimized_exe_time}")
    baseline_exe_time = timeit.timeit(setup='myarr2=pack(np.array([datetime(2022,10,13,3,3,13) for _ in range (0, 10000)]))',
      stmt='unpack(myarr2)',
      number=10,
      globals=(globals()))
    print(f"baseline time {baseline_exe_time}")
    @py_assert1 = 900
    @py_assert3 = optimized_exe_time * @py_assert1
    @py_assert4 = @py_assert3 < baseline_exe_time
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('<', ), (@py_assert4,), ('(%(py0)s * %(py2)s) < %(py5)s', ), (@py_assert3, baseline_exe_time)) % {'py0':@pytest_ar._saferepr(optimized_exe_time) if 'optimized_exe_time' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(optimized_exe_time) else 'optimized_exe_time',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(baseline_exe_time) if 'baseline_exe_time' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(baseline_exe_time) else 'baseline_exe_time'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None