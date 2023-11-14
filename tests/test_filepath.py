# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_filepath.py
# Compiled at: 2023-02-19 16:10:10
# Size of source mod 2**32: 9101 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from pathlib import Path
import random, sys, io, logging, pytest
from . import connection_root, connection_test, bucket
from schemas.external import schema, stores, store_repo, store_repo_s3, Filepath, FilepathS3
logger = logging.getLogger('datajoint')

def test_path_match(schema, store_repo):
    """test file path matches and empty file"""
    store = store_repo
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'path/to/films'
    managed_file = Path(stage_path, relpath, 'vid.mov')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    open(str(managed_file), 'a').close()
    uuid = ext.upload_filepath(str(managed_file))
    managed_file.unlink()
    @py_assert1 = managed_file.exists
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = 'assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.exists\n}()\n}' % {'py0':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    @py_assert1 = {'hash': uuid}
    @py_assert3 = ext & @py_assert1
    @py_assert4 = @py_assert3.fetch1
    @py_assert6 = 'filepath'
    @py_assert8 = @py_assert4(@py_assert6)
    @py_assert13 = managed_file.relative_to
    @py_assert16 = @py_assert13(stage_path)
    @py_assert18 = @py_assert16.as_posix
    @py_assert20 = @py_assert18()
    @py_assert22 = str(@py_assert20)
    @py_assert10 = @py_assert8 == @py_assert22
    if not @py_assert10:
        @py_format24 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py23)s\n{%(py23)s = %(py11)s(%(py21)s\n{%(py21)s = %(py19)s\n{%(py19)s = %(py17)s\n{%(py17)s = %(py14)s\n{%(py14)s = %(py12)s.relative_to\n}(%(py15)s)\n}.as_posix\n}()\n})\n}', ), (@py_assert8, @py_assert22)) % {'py0':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py12':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py14':@pytest_ar._saferepr(@py_assert13),  'py15':@pytest_ar._saferepr(stage_path) if 'stage_path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(stage_path) else 'stage_path',  'py17':@pytest_ar._saferepr(@py_assert16),  'py19':@pytest_ar._saferepr(@py_assert18),  'py21':@pytest_ar._saferepr(@py_assert20),  'py23':@pytest_ar._saferepr(@py_assert22)}
        @py_format26 = 'assert %(py25)s' % {'py25': @py_format24}
        raise AssertionError(@pytest_ar._format_explanation(@py_format26))
    @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert13 = @py_assert16 = @py_assert18 = @py_assert20 = @py_assert22 = None
    restored_path, checksum = ext.download_filepath(uuid)
    @py_assert4 = str(managed_file)
    @py_assert1 = restored_path == @py_assert4
    if not @py_assert1:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py5)s\n{%(py5)s = %(py2)s(%(py3)s)\n}', ), (restored_path, @py_assert4)) % {'py0':@pytest_ar._saferepr(restored_path) if 'restored_path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restored_path) else 'restored_path',  'py2':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py3':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert4 = None
    @py_assert3 = dj.hash
    @py_assert5 = @py_assert3.uuid_from_file
    @py_assert9 = str(managed_file)
    @py_assert11 = @py_assert5(@py_assert9)
    @py_assert1 = checksum == @py_assert11
    if not @py_assert1:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py12)s\n{%(py12)s = %(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py2)s.hash\n}.uuid_from_file\n}(%(py10)s\n{%(py10)s = %(py7)s(%(py8)s)\n})\n}', ), (checksum, @py_assert11)) % {'py0':@pytest_ar._saferepr(checksum) if 'checksum' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(checksum) else 'checksum',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py7':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py8':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert9 = @py_assert11 = None
    ext.delete(delete_external_files=True)


def test_filepath(schema, store_repo):
    """test file management"""
    random.seed('filepath')
    store = store_repo
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    filename = 'picture.dat'
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = random.getrandbits(24000).to_bytes(3000, sys.byteorder)
    with managed_file.open('wb') as f:
        f.write(data)
    uuid1 = ext.upload_filepath(str(managed_file))
    uuid2 = ext.upload_filepath(str(managed_file))
    @py_assert1 = uuid1 == uuid2
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (uuid1, uuid2)) % {'py0':@pytest_ar._saferepr(uuid1) if 'uuid1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(uuid1) else 'uuid1',  'py2':@pytest_ar._saferepr(uuid2) if 'uuid2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(uuid2) else 'uuid2'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    managed_file.unlink()
    @py_assert1 = managed_file.exists
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = 'assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.exists\n}()\n}' % {'py0':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    for _ in (1, 2):
        restored_path, checksum = ext.download_filepath(uuid1)
        @py_assert4 = str(managed_file)
        @py_assert1 = restored_path == @py_assert4
        if not @py_assert1:
            @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py5)s\n{%(py5)s = %(py2)s(%(py3)s)\n}', ), (restored_path, @py_assert4)) % {'py0':@pytest_ar._saferepr(restored_path) if 'restored_path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restored_path) else 'restored_path',  'py2':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py3':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py5':@pytest_ar._saferepr(@py_assert4)}
            @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
            raise AssertionError(@pytest_ar._format_explanation(@py_format8))
        else:
            @py_assert1 = @py_assert4 = None
            @py_assert3 = dj.hash
            @py_assert5 = @py_assert3.uuid_from_file
            @py_assert9 = str(managed_file)
            @py_assert11 = @py_assert5(@py_assert9)
            @py_assert1 = checksum == @py_assert11
            if not @py_assert1:
                @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py12)s\n{%(py12)s = %(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py2)s.hash\n}.uuid_from_file\n}(%(py10)s\n{%(py10)s = %(py7)s(%(py8)s)\n})\n}', ), (checksum, @py_assert11)) % {'py0':@pytest_ar._saferepr(checksum) if 'checksum' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(checksum) else 'checksum',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py7':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py8':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11)}
                @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
                raise AssertionError(@pytest_ar._format_explanation(@py_format15))
            @py_assert1 = @py_assert3 = @py_assert5 = @py_assert9 = @py_assert11 = None

    with managed_file.open('rb') as f:
        synced_data = f.read()
    @py_assert1 = data == synced_data
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data, synced_data)) % {'py0':@pytest_ar._saferepr(data) if 'data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data) else 'data',  'py2':@pytest_ar._saferepr(synced_data) if 'synced_data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(synced_data) else 'synced_data'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    ext.delete(delete_external_files=True)
    @py_assert1 = ext.exists
    @py_assert4 = ext._make_external_filepath
    @py_assert10 = Path(relpath, filename)
    @py_assert12 = str(@py_assert10)
    @py_assert14 = @py_assert4(@py_assert12)
    @py_assert16 = @py_assert1(@py_assert14)
    @py_assert18 = not @py_assert16
    if not @py_assert18:
        @py_format19 = 'assert not %(py17)s\n{%(py17)s = %(py2)s\n{%(py2)s = %(py0)s.exists\n}(%(py15)s\n{%(py15)s = %(py5)s\n{%(py5)s = %(py3)s._make_external_filepath\n}(%(py13)s\n{%(py13)s = %(py6)s(%(py11)s\n{%(py11)s = %(py7)s(%(py8)s, %(py9)s)\n})\n})\n})\n}' % {'py0':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py5':@pytest_ar._saferepr(@py_assert4),  'py6':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py7':@pytest_ar._saferepr(Path) if 'Path' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Path) else 'Path',  'py8':@pytest_ar._saferepr(relpath) if 'relpath' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(relpath) else 'relpath',  'py9':@pytest_ar._saferepr(filename) if 'filename' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(filename) else 'filename',  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14),  'py17':@pytest_ar._saferepr(@py_assert16)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format19))
    @py_assert1 = @py_assert4 = @py_assert10 = @py_assert12 = @py_assert14 = @py_assert16 = @py_assert18 = None


def test_filepath_s3(schema, store_repo_s3):
    """test file management with s3"""
    test_filepath(schema, store_repo_s3)


def test_duplicate_upload(schema, store_repo):
    random.seed('filepath')
    store = store_repo
    data = random.getrandbits(2400).to_bytes(300, sys.byteorder)
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, 'plot.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open('wb') as f:
        f.write(data)
    ext.upload_filepath(str(managed_file))
    ext.upload_filepath(str(managed_file))


def test_duplicate_upload_s3(schema, store_repo_s3):
    test_duplicate_upload(schema, store_repo_s3)


def test_duplicate_error(schema, store_repo):
    """syncing duplicate non-matching file should fail"""
    random.seed('filepath')
    store = store_repo
    data1 = random.getrandbits(2400).to_bytes(300, sys.byteorder)
    data2 = random.getrandbits(2400).to_bytes(300, sys.byteorder)
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, 'thesis.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open('wb') as f:
        f.write(data1)
    ext.upload_filepath(str(managed_file))
    with managed_file.open('wb') as f:
        f.write(data2)
    with pytest.raises(dj.DataJointError):
        ext.upload_filepath(str(managed_file))


def test_duplicate_error_s3(schema, store_repo_s3):
    test_duplicate_error(schema, store_repo_s3)


def test_filepath_class(Filepath, store_repo, verify_checksum=True):
    random.seed('filepath')
    store = store_repo
    table = Filepath()
    if not verify_checksum:
        dj.config['filepath_checksum_size_limit'] = 0
    stage_path = dj.config['stores'][store]['stage']
    relative_path = 'one/two/three'
    managed_file = Path(stage_path, relative_path, 'attachment.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = random.getrandbits(24000).to_bytes(3000, sys.byteorder)
    with managed_file.open('wb') as f:
        f.write(data)
    with managed_file.open('rb') as f:
        contents = f.read()
    @py_assert1 = data == contents
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data, contents)) % {'py0':@pytest_ar._saferepr(data) if 'data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data) else 'data',  'py2':@pytest_ar._saferepr(contents) if 'contents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(contents) else 'contents'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    table.insert1((1, str(managed_file)))
    managed_file.unlink()
    @py_assert1 = managed_file.is_file
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = 'assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.is_file\n}()\n}' % {'py0':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    filepath = (table & {'fnum': 1}).fetch1('img')
    @py_assert4 = str(managed_file)
    @py_assert1 = filepath == @py_assert4
    if not @py_assert1:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py5)s\n{%(py5)s = %(py2)s(%(py3)s)\n}', ), (filepath, @py_assert4)) % {'py0':@pytest_ar._saferepr(filepath) if 'filepath' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(filepath) else 'filepath',  'py2':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py3':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert4 = None
    with managed_file.open('rb') as f:
        contents = f.read()
    @py_assert1 = data == contents
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data, contents)) % {'py0':@pytest_ar._saferepr(data) if 'data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data) else 'data',  'py2':@pytest_ar._saferepr(contents) if 'contents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(contents) else 'contents'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    table.delete()
    @py_assert0 = table.external[store]
    if not @py_assert0:
        @py_format2 = 'assert %(py1)s' % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format2))
    @py_assert0 = None
    table.external[store].delete(delete_external_files=True)
    dj.config['filepath_checksum_size_limit'] = None


def test_filepath_class_s3(FilepathS3, store_repo_s3):
    test_filepath_class(FilepathS3, store_repo_s3)


def test_filepath_class_no_checksum(Filepath, store_repo):
    log_capture = io.StringIO()
    stream_handler = logging.StreamHandler(log_capture)
    log_format = logging.Formatter('[%(asctime)s][%(funcName)s][%(levelname)s]: %(message)s')
    stream_handler.setFormatter(log_format)
    stream_handler.set_name('test_limit_warning')
    logger.addHandler(stream_handler)
    test_filepath_class(Filepath, store_repo, verify_checksum=False)
    log_contents = log_capture.getvalue()
    log_capture.close()
    for handler in logger.handlers:
        if handler.name == 'test_limit_warning':
            logger.removeHandler(handler)

    @py_assert0 = 'Skipped checksum for file with hash:'
    @py_assert2 = @py_assert0 in log_contents
    if not @py_assert2:
        @py_format4 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py3)s', ), (@py_assert0, log_contents)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(log_contents) if 'log_contents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(log_contents) else 'log_contents'}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert0 = @py_assert2 = None


def test_filepath_cleanup(schema, Filepath, store_repo):
    """test deletion of filepath entries from external table"""
    random.seed('filepath')
    table = Filepath()
    store = store_repo
    stage_path = dj.config['stores'][store]['stage']
    n = 20
    contents = random.getrandbits(2760).to_bytes(345, sys.byteorder)
    for i in range(n):
        relative_path = Path(*random.sample(('one', 'two', 'three', 'four'), k=3))
        managed_file = Path(stage_path, relative_path, 'file.dat')
        managed_file.parent.mkdir(parents=True, exist_ok=True)
        with managed_file.open('wb') as f:
            f.write(contents)
        table.insert1((i, str(managed_file)))

    @py_assert2 = len(table)
    @py_assert4 = @py_assert2 == n
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, n)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(table) if 'table' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table) else 'table',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    ext = schema.external[store]
    @py_assert2 = len(table)
    @py_assert4 = @py_assert2 == n
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, n)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(table) if 'table' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(table) else 'table',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    @py_assert0 = 0
    @py_assert6 = len(ext)
    @py_assert2 = @py_assert0 < @py_assert6
    @py_assert3 = @py_assert6 < n
    if not (@py_assert2 and @py_assert3):
        @py_format9 = @pytest_ar._call_reprcompare(('<', '<'), (@py_assert2, @py_assert3), ('%(py1)s < %(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n}',
                                                                                            '%(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n} < %(py8)s'), (@py_assert0, @py_assert6, n)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py5':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py7':@pytest_ar._saferepr(@py_assert6),  'py8':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert0 = @py_assert2 = @py_assert3 = @py_assert6 = None
    (table & 'fnum in (1, 2, 3, 4, 5, 6)').delete()
    m = n - len(table)
    @py_assert2 = 6
    @py_assert1 = m == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (m, @py_assert2)) % {'py0':@pytest_ar._saferepr(m) if 'm' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(m) else 'm',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None
    ext.delete(delete_external_files=True)
    @py_assert0 = 0
    @py_assert6 = len(ext)
    @py_assert2 = @py_assert0 < @py_assert6
    @py_assert10 = n - m
    @py_assert3 = @py_assert6 <= @py_assert10
    if not (@py_assert2 and @py_assert3):
        @py_format11 = @pytest_ar._call_reprcompare(('<', '<='), (@py_assert2, @py_assert3), ('%(py1)s < %(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n}',
                                                                                              '%(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n} <= (%(py8)s - %(py9)s)'), (@py_assert0, @py_assert6, @py_assert10)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py5':@pytest_ar._saferepr(ext) if 'ext' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ext) else 'ext',  'py7':@pytest_ar._saferepr(@py_assert6),  'py8':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n',  'py9':@pytest_ar._saferepr(m) if 'm' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(m) else 'm'}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert0 = @py_assert2 = @py_assert3 = @py_assert6 = @py_assert10 = None


def test_filepath_cleanup_s3(schema, FilepathS3, store_repo_s3):
    """test deletion of filepath entries from external table"""
    test_filepath_cleanup(schema, FilepathS3, store_repo_s3)


def test_delete_without_files(schema, store_repo):
    """test deletion of filepath entries from external table without removing files"""
    schema.external[store_repo].delete(delete_external_files=False)


def test_return_string(Filepath, store_repo):
    """test returning string on fetch"""
    random.seed('filepath')
    table = Filepath()
    store = store_repo
    stage_path = dj.config['stores'][store]['stage']
    relative_path = 'this/is/a/test'
    managed_file = Path(stage_path, relative_path, 'string.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = random.getrandbits(24000).to_bytes(3000, sys.byteorder)
    with managed_file.open('wb') as f:
        f.write(data)
    with managed_file.open('rb') as f:
        contents = f.read()
    @py_assert1 = data == contents
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (data, contents)) % {'py0':@pytest_ar._saferepr(data) if 'data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(data) else 'data',  'py2':@pytest_ar._saferepr(contents) if 'contents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(contents) else 'contents'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None
    table.insert1((138, str(managed_file)))
    managed_file.unlink()
    @py_assert1 = managed_file.is_file
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = 'assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.is_file\n}()\n}' % {'py0':@pytest_ar._saferepr(managed_file) if 'managed_file' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(managed_file) else 'managed_file',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    filepath = (table & {'fnum': 138}).fetch1('img')
    @py_assert3 = isinstance(filepath, str)
    if not @py_assert3:
        @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py0)s(%(py1)s, %(py2)s)\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(filepath) if 'filepath' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(filepath) else 'filepath',  'py2':@pytest_ar._saferepr(str) if 'str' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(str) else 'str',  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert3 = None