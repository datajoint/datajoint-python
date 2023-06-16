# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_blob_matlab.py
# Compiled at: 2023-02-17 20:46:06
# Size of source mod 2**32: 8591 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint.blob import pack, unpack
import numpy as np
from numpy.testing import assert_array_equal
import pytest
from . import PREFIX, connection_root, connection_test

@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + '_test1'), (locals()), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def Blob(schema):

    @schema
    class Blob(dj.Manual):
        definition = '  # diverse types of blobs\n        id : int\n        -----\n        comment  :  varchar(255)\n        blob  : longblob\n        '

    schema.connection.query(f"\n        INSERT INTO {Blob.full_table_name} VALUES\n        (1,'simple string',0x6D596D00410200000000000000010000000000000010000000000000000400000000000000630068006100720061006300740065007200200073007400720069006E006700),\n        (2,'1D vector',0x6D596D0041020000000000000001000000000000000C000000000000000600000000000000000000000000F03F00000000000030400000000000003F4000000000000047400000000000804E4000000000000053400000000000C056400000000000805A400000000000405E4000000000000061400000000000E062400000000000C06440),\n        (3,'string array',0x6D596D00430200000000000000010000000000000002000000000000002F0000000000000041020000000000000001000000000000000700000000000000040000000000000073007400720069006E00670031002F0000000000000041020000000000000001000000000000000700000000000000040000000000000073007400720069006E0067003200),\n        (4,'struct array',0x6D596D005302000000000000000100000000000000020000000000000002000000610062002900000000000000410200000000000000010000000000000001000000000000000600000000000000000000000000F03F9000000000000000530200000000000000010000000000000001000000000000000100000063006900000000000000410200000000000000030000000000000003000000000000000600000000000000000000000000204000000000000008400000000000001040000000000000F03F0000000000001440000000000000224000000000000018400000000000001C40000000000000004029000000000000004102000000000000000100000000000000010000000000000006000000000000000000000000000040100100000000000053020000000000000001000000000000000100000000000000010000004300E9000000000000004102000000000000000500000000000000050000000000000006000000000000000000000000003140000000000000374000000000000010400000000000002440000000000000264000000000000038400000000000001440000000000000184000000000000028400000000000003240000000000000F03F0000000000001C400000000000002A400000000000003340000000000000394000000000000020400000000000002C400000000000003440000000000000354000000000000000400000000000002E400000000000003040000000000000364000000000000008400000000000002240),\n        (5,'3D double array',0x6D596D004103000000000000000200000000000000030000000000000004000000000000000600000000000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C40000000000000204000000000000022400000000000002440000000000000264000000000000028400000000000002A400000000000002C400000000000002E40000000000000304000000000000031400000000000003240000000000000334000000000000034400000000000003540000000000000364000000000000037400000000000003840),\n        (6,'3D uint8 array',0x6D596D0041030000000000000002000000000000000300000000000000040000000000000009000000000000000102030405060708090A0B0C0D0E0F101112131415161718),\n        (7,'3D complex array',0x6D596D0041030000000000000002000000000000000300000000000000040000000000000006000000010000000000000000C0724000000000000028C000000000000038C0000000000000000000000000000038C0000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000AA4C58E87AB62B400000000000000000AA4C58E87AB62BC0000000000000008000000000000052400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080000000000000008000000000000052C000000000000000800000000000000080000000000000008000000000000000800000000000000080\n        );\n        ")
    yield Blob
    Blob.drop()


def test_complex_matlab_blobs(Blob):
    """
    test correct de-serialization of various blob types
    """
    blobs = Blob().fetch('blob', order_by='KEY')
    blob = blobs[0]
    @py_assert0 = blob[0]
    @py_assert3 = 'character string'
    @py_assert2 = @py_assert0 == @py_assert3
    if not @py_assert2:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert2,), ('%(py1)s == %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert0 = @py_assert2 = @py_assert3 = None
    blob = blobs[1]
    assert_array_equal(blob, np.r_[1:180:15][None, :])
    assert_array_equal(blob, unpack(pack(blob)))
    blob = blobs[2]
    @py_assert3 = dj.MatCell
    @py_assert5 = isinstance(blob, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.MatCell\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    assert_array_equal(blob, np.array([['string1', 'string2']]))
    assert_array_equal(blob, unpack(pack(blob)))
    blob = blobs[3]
    @py_assert3 = dj.MatStruct
    @py_assert5 = isinstance(blob, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.MatStruct\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    @py_assert1 = blob.dtype
    @py_assert3 = @py_assert1.names
    @py_assert6 = ('a', 'b')
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.dtype\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    assert_array_equal(blob.a[(0, 0)], np.array([[1.0]]))
    assert_array_equal(blob.a[(0, 1)], np.array([[2.0]]))
    @py_assert1 = blob.b[(0, 1)]
    @py_assert4 = dj.MatStruct
    @py_assert6 = isinstance(@py_assert1, @py_assert4)
    if not @py_assert6:
        @py_format8 = 'assert %(py7)s\n{%(py7)s = %(py0)s(%(py2)s, %(py5)s\n{%(py5)s = %(py3)s.MatStruct\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert4 = @py_assert6 = None
    @py_assert0 = blob.b[(0, 1)].C[(0, 0)]
    @py_assert2 = @py_assert0.shape
    @py_assert5 = (5, 5)
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py1)s.shape\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert5 = None
    b = unpack(pack(blob))
    assert_array_equal(b[(0, 0)].b[(0, 0)].c, blob[(0, 0)].b[(0, 0)].c)
    assert_array_equal(b[(0, 1)].b[(0, 0)].C, blob[(0, 1)].b[(0, 0)].C)
    blob = blobs[4]
    assert_array_equal(blob, np.r_[1:25].reshape((2, 3, 4), order='F'))
    @py_assert1 = blob.dtype
    @py_assert4 = 'float64'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    assert_array_equal(blob, unpack(pack(blob)))
    blob = blobs[5]
    @py_assert1 = np.array_equal
    @py_assert4 = np.r_[1:25]
    @py_assert6 = @py_assert4.reshape
    @py_assert8 = (2, 3, 4)
    @py_assert10 = 'F'
    @py_assert12 = @py_assert6(@py_assert8, order=@py_assert10)
    @py_assert14 = @py_assert1(blob, @py_assert12)
    if not @py_assert14:
        @py_format16 = 'assert %(py15)s\n{%(py15)s = %(py2)s\n{%(py2)s = %(py0)s.array_equal\n}(%(py3)s, %(py13)s\n{%(py13)s = %(py7)s\n{%(py7)s = %(py5)s.reshape\n}(%(py9)s, order=%(py11)s)\n})\n}' % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format16))
    @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = None
    @py_assert1 = blob.dtype
    @py_assert4 = 'uint8'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    assert_array_equal(blob, unpack(pack(blob)))
    blob = blobs[6]
    @py_assert1 = blob.shape
    @py_assert4 = (2, 3, 4)
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.shape\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = blob.dtype
    @py_assert4 = 'complex128'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    assert_array_equal(blob, unpack(pack(blob)))


def test_complex_matlab_squeeze(Blob):
    """
    test correct de-serialization of various blob types
    """
    blob = (Blob & 'id=1').fetch1('blob',
      squeeze=True)
    @py_assert2 = 'character string'
    @py_assert1 = blob == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (blob, @py_assert2)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    blob = (Blob & 'id=2').fetch1('blob', squeeze=True)
    assert_array_equal(blob, np.r_[1:180:15])
    blob = (Blob & 'id=3').fetch1('blob',
      squeeze=True)
    @py_assert3 = dj.MatCell
    @py_assert5 = isinstance(blob, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.MatCell\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    assert_array_equal(blob, np.array(['string1', 'string2']))
    blob = (Blob & 'id=4').fetch1('blob',
      squeeze=True)
    @py_assert3 = dj.MatStruct
    @py_assert5 = isinstance(blob, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.MatStruct\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    @py_assert1 = blob.dtype
    @py_assert3 = @py_assert1.names
    @py_assert6 = ('a', 'b')
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.dtype\n}.names\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    assert_array_equal(blob.a, np.array([1.0, 2]))
    @py_assert1 = blob[1]
    @py_assert3 = @py_assert1.b
    @py_assert6 = dj.MatStruct
    @py_assert8 = isinstance(@py_assert3, @py_assert6)
    if not @py_assert8:
        @py_format10 = 'assert %(py9)s\n{%(py9)s = %(py0)s(%(py4)s\n{%(py4)s = %(py2)s.b\n}, %(py7)s\n{%(py7)s = %(py5)s.MatStruct\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py5':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert6 = @py_assert8 = None
    @py_assert0 = blob[1]
    @py_assert2 = @py_assert0.b
    @py_assert4 = @py_assert2.C
    @py_assert6 = @py_assert4.item
    @py_assert8 = @py_assert6()
    @py_assert10 = @py_assert8.shape
    @py_assert13 = (5, 5)
    @py_assert12 = @py_assert10 == @py_assert13
    if not @py_assert12:
        @py_format15 = @pytest_ar._call_reprcompare(('==', ), (@py_assert12,), ('%(py11)s\n{%(py11)s = %(py9)s\n{%(py9)s = %(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.b\n}.C\n}.item\n}()\n}.shape\n} == %(py14)s', ), (@py_assert10, @py_assert13)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py14':@pytest_ar._saferepr(@py_assert13)}
        @py_format17 = 'assert %(py16)s' % {'py16': @py_format15}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert13 = None
    blob = (Blob & 'id=5').fetch1('blob',
      squeeze=True)
    @py_assert1 = np.array_equal
    @py_assert4 = np.r_[1:25]
    @py_assert6 = @py_assert4.reshape
    @py_assert8 = (2, 3, 4)
    @py_assert10 = 'F'
    @py_assert12 = @py_assert6(@py_assert8, order=@py_assert10)
    @py_assert14 = @py_assert1(blob, @py_assert12)
    if not @py_assert14:
        @py_format16 = 'assert %(py15)s\n{%(py15)s = %(py2)s\n{%(py2)s = %(py0)s.array_equal\n}(%(py3)s, %(py13)s\n{%(py13)s = %(py7)s\n{%(py7)s = %(py5)s.reshape\n}(%(py9)s, order=%(py11)s)\n})\n}' % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format16))
    @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = None
    @py_assert1 = blob.dtype
    @py_assert4 = 'float64'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    blob = (Blob & 'id=6').fetch1('blob', squeeze=True)
    @py_assert1 = np.array_equal
    @py_assert4 = np.r_[1:25]
    @py_assert6 = @py_assert4.reshape
    @py_assert8 = (2, 3, 4)
    @py_assert10 = 'F'
    @py_assert12 = @py_assert6(@py_assert8, order=@py_assert10)
    @py_assert14 = @py_assert1(blob, @py_assert12)
    if not @py_assert14:
        @py_format16 = 'assert %(py15)s\n{%(py15)s = %(py2)s\n{%(py2)s = %(py0)s.array_equal\n}(%(py3)s, %(py13)s\n{%(py13)s = %(py7)s\n{%(py7)s = %(py5)s.reshape\n}(%(py9)s, order=%(py11)s)\n})\n}' % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format16))
    @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = None
    @py_assert1 = blob.dtype
    @py_assert4 = 'uint8'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    blob = (Blob & 'id=7').fetch1('blob', squeeze=True)
    @py_assert1 = blob.shape
    @py_assert4 = (2, 3, 4)
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.shape\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None
    @py_assert1 = blob.dtype
    @py_assert4 = 'complex128'
    @py_assert3 = @py_assert1 == @py_assert4
    if not @py_assert3:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.dtype\n} == %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(blob) if 'blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(blob) else 'blob',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None


def test_iter(Blob):
    """
    test iterator over the entity set
    """
    from_iter = {d['id']: d for d in Blob()}
    @py_assert2 = len(from_iter)
    @py_assert7 = Blob()
    @py_assert9 = len(@py_assert7)
    @py_assert4 = @py_assert2 == @py_assert9
    if not @py_assert4:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py10)s\n{%(py10)s = %(py5)s(%(py8)s\n{%(py8)s = %(py6)s()\n})\n}', ), (@py_assert2, @py_assert9)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(from_iter) if 'from_iter' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(from_iter) else 'from_iter',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py6':@pytest_ar._saferepr(Blob) if 'Blob' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Blob) else 'Blob',  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert2 = @py_assert4 = @py_assert7 = @py_assert9 = None
    @py_assert0 = from_iter[1]['blob']
    @py_assert3 = 'character string'
    @py_assert2 = @py_assert0 == @py_assert3
    if not @py_assert2:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert2,), ('%(py1)s == %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert0 = @py_assert2 = @py_assert3 = None