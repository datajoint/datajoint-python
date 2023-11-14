# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_attach.py
# Compiled at: 2023-02-17 16:53:19
# Size of source mod 2**32: 2660 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import tempfile, random, sys
from pathlib import Path
from . import connection_root, connection_test, bucket
from schemas.external import schema, stores, store_share, Attach

def test_attach_attributes(Attach):
    """test saving files in attachments"""
    with tempfile.TemporaryDirectory() as source_folder:
        with tempfile.TemporaryDirectory() as download_folder:
            random.seed('attach')
            table = Attach()
            for i in range(2):
                attach1 = Path(source_folder, 'attach1.img')
                data1 = random.getrandbits(800).to_bytes(100, sys.byteorder)
                with attach1.open('wb') as f:
                    f.write(data1)
                attach2 = Path(source_folder, 'attach2.txt')
                data2 = random.getrandbits(1600).to_bytes(200, sys.byteorder)
                with attach2.open('wb') as f:
                    f.write(data2)
                table.insert1(dict(attach=i, img=attach1, txt=attach2))

            keys, path1, path2 = table.fetch('KEY',
              'img', 'txt', download_path=download_folder, order_by='KEY')
            @py_assert0 = path1[0]
            @py_assert3 = path2[0]
            @py_assert2 = @py_assert0 != @py_assert3
            if not @py_assert2:
                @py_format5 = @pytest_ar._call_reprcompare(('!=', ), (@py_assert2,), ('%(py1)s != %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
                @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
                raise AssertionError(@pytest_ar._format_explanation(@py_format7))
            @py_assert0 = @py_assert2 = @py_assert3 = None
            @py_assert0 = path1[0]
            @py_assert3 = path1[1]
            @py_assert2 = @py_assert0 != @py_assert3
            if not @py_assert2:
                @py_format5 = @pytest_ar._call_reprcompare(('!=', ), (@py_assert2,), ('%(py1)s != %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
                @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
                raise AssertionError(@pytest_ar._format_explanation(@py_format7))
            @py_assert0 = @py_assert2 = @py_assert3 = None
            @py_assert1 = path1[0]
            @py_assert3 = Path(@py_assert1)
            @py_assert5 = @py_assert3.parent
            @py_assert10 = Path(download_folder)
            @py_assert7 = @py_assert5 == @py_assert10
            if not @py_assert7:
                @py_format12 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n}.parent\n} == %(py11)s\n{%(py11)s = %(py8)s(%(py9)s)\n}', ), (@py_assert5, @py_assert10)) % {'py0':@pytest_ar._saferepr(Path) if 'Path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Path) else 'Path',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(Path) if 'Path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Path) else 'Path',  'py9':@pytest_ar._saferepr(download_folder) if 'download_folder' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(download_folder) else 'download_folder',  'py11':@pytest_ar._saferepr(@py_assert10)}
                @py_format14 = 'assert %(py13)s' % {'py13': @py_format12}
                raise AssertionError(@pytest_ar._format_explanation(@py_format14))
            @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert10 = None
            with Path(path1[-1]).open('rb') as f:
                check1 = f.read()
            with Path(path2[-1]).open('rb') as f:
                check2 = f.read()
            @py_assert1 = data1 == check1
            if not @py_assert1:
                @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data1, check1)) % {'py0':@pytest_ar._saferepr(data1) if 'data1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data1) else 'data1',  'py2':@pytest_ar._saferepr(check1) if 'check1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(check1) else 'check1'}
                @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
                raise AssertionError(@pytest_ar._format_explanation(@py_format5))
            @py_assert1 = None
            @py_assert1 = data2 == check2
            if not @py_assert1:
                @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data2, check2)) % {'py0':@pytest_ar._saferepr(data2) if 'data2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data2) else 'data2',  'py2':@pytest_ar._saferepr(check2) if 'check2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(check2) else 'check2'}
                @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
                raise AssertionError(@pytest_ar._format_explanation(@py_format5))
            @py_assert1 = None
            p1, p2 = (Attach & keys[0]).fetch1('img', 'txt', download_path=download_folder)
            @py_assert2 = path1[0]
            @py_assert1 = p1 == @py_assert2
            if not @py_assert1:
                @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (p1, @py_assert2)) % {'py0':@pytest_ar._saferepr(p1) if 'p1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(p1) else 'p1',  'py3':@pytest_ar._saferepr(@py_assert2)}
                @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
                raise AssertionError(@pytest_ar._format_explanation(@py_format6))
            @py_assert1 = @py_assert2 = None
            @py_assert2 = path2[0]
            @py_assert1 = p2 == @py_assert2
            if not @py_assert1:
                @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (p2, @py_assert2)) % {'py0':@pytest_ar._saferepr(p2) if 'p2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(p2) else 'p2',  'py3':@pytest_ar._saferepr(@py_assert2)}
                @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
                raise AssertionError(@pytest_ar._format_explanation(@py_format6))
            @py_assert1 = @py_assert2 = None


def test_return_string(Attach):
    """test returning string on fetch"""
    with tempfile.TemporaryDirectory() as source_folder:
        with tempfile.TemporaryDirectory() as download_folder:
            random.seed('attach')
            table = Attach()
            attach1 = Path(source_folder, 'attach1.img')
            data1 = random.getrandbits(800).to_bytes(100, sys.byteorder)
            with attach1.open('wb') as f:
                f.write(data1)
            attach2 = Path(source_folder, 'attach2.txt')
            data2 = random.getrandbits(1600).to_bytes(200, sys.byteorder)
            with attach2.open('wb') as f:
                f.write(data2)
            table.insert1(dict(attach=2, img=attach1, txt=attach2))
            keys, path1, path2 = table.fetch('KEY',
              'img', 'txt', download_path=download_folder, order_by='KEY')
            @py_assert1 = path1[0]
            @py_assert4 = isinstance(@py_assert1, str)
            if not @py_assert4:
                @py_format6 = 'assert %(py5)s\n{%(py5)s = %(py0)s(%(py2)s, %(py3)s)\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py5':@pytest_ar._saferepr(@py_assert4)}
                raise AssertionError(@pytest_ar._format_explanation(@py_format6))
            @py_assert1 = @py_assert4 = None