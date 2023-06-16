# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_hash.py
# Compiled at: 2023-02-20 15:57:14
# Size of source mod 2**32: 209 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
from datajoint import hash

def test_hash():
    @py_assert1 = hash.uuid_from_buffer
    @py_assert3 = b'abc'
    @py_assert5 = @py_assert1(@py_assert3)
    @py_assert7 = @py_assert5.hex
    @py_assert10 = '900150983cd24fb0d6963f7d28e17f72'
    @py_assert9 = @py_assert7 == @py_assert10
    if not @py_assert9:
        @py_format12 = @pytest_ar._call_reprcompare(('==', ), (@py_assert9,), ('%(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.uuid_from_buffer\n}(%(py4)s)\n}.hex\n} == %(py11)s', ), (@py_assert7, @py_assert10)) % {'py0':@pytest_ar._saferepr(hash) if 'hash' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(hash) else 'hash',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py11':@pytest_ar._saferepr(@py_assert10)}
        @py_format14 = 'assert %(py13)s' % {'py13': @py_format12}
        raise AssertionError(@pytest_ar._format_explanation(@py_format14))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert10 = None
    @py_assert1 = hash.uuid_from_buffer
    @py_assert3 = b''
    @py_assert5 = @py_assert1(@py_assert3)
    @py_assert7 = @py_assert5.hex
    @py_assert10 = 'd41d8cd98f00b204e9800998ecf8427e'
    @py_assert9 = @py_assert7 == @py_assert10
    if not @py_assert9:
        @py_format12 = @pytest_ar._call_reprcompare(('==', ), (@py_assert9,), ('%(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.uuid_from_buffer\n}(%(py4)s)\n}.hex\n} == %(py11)s', ), (@py_assert7, @py_assert10)) % {'py0':@pytest_ar._saferepr(hash) if 'hash' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(hash) else 'hash',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py11':@pytest_ar._saferepr(@py_assert10)}
        @py_format14 = 'assert %(py13)s' % {'py13': @py_format12}
        raise AssertionError(@pytest_ar._format_explanation(@py_format14))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert10 = None