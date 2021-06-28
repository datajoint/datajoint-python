from nose.tools import assert_true, assert_false, assert_equal, raises
import datajoint as dj
import os
from pathlib import Path
import random
import time
import sys
import shutil
import cProfile
from .schema_external import schema, Filepath, FilepathS3, stores_config


def setUp(self):
    dj.config['stores'] = stores_config


def test_path_match(store="repo"):
    """ test file path matches and empty file"""
    dj.errors._switch_filepath_types(True)
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']

    # create a mock file
    relpath = 'path/to/films'
    managed_file = Path(stage_path, relpath, 'vid.mov')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    open(str(managed_file), 'a').close()

    # put the file
    uuid = ext.upload_filepath(str(managed_file))

    # remove
    managed_file.unlink()
    assert_false(managed_file.exists())

    # check filepath
    assert_equal(
        (ext & {'hash': uuid}).fetch1('filepath'),
        str(managed_file.relative_to(stage_path).as_posix()))

    # # Download the file and check its contents.
    restored_path, checksum = ext.download_filepath(uuid)
    assert_equal(restored_path, str(managed_file))
    assert_equal(checksum, dj.hash.uuid_from_file(str(managed_file)))

    # cleanup
    ext.delete(delete_external_files=True)
    dj.errors._switch_filepath_types(False)


def test_filepath(store="repo"):
    """ test file management """
    dj.errors._switch_filepath_types(True)

    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    filename = 'picture.dat'

    # create a mock file
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    with managed_file.open('wb') as f:
        f.write(data)

    # put the same file twice to ensure storing once
    uuid1 = ext.upload_filepath(str(managed_file))
    # no duplication should arise if file is the same
    uuid2 = ext.upload_filepath(str(managed_file))
    assert_equal(uuid1, uuid2)

    # remove to ensure downloading
    managed_file.unlink()
    assert_false(managed_file.exists())

    # Download the file and check its contents. Repeat causes no download from remote
    for _ in 1, 2:
        restored_path, checksum = ext.download_filepath(uuid1)
        assert_equal(restored_path, str(managed_file))
        assert_equal(checksum, dj.hash.uuid_from_file(str(managed_file)))

    # verify same data
    with managed_file.open('rb') as f:
        synced_data = f.read()
    assert_equal(data, synced_data)

    # cleanup
    ext.delete(delete_external_files=True)
    assert_false(ext.exists(ext._make_external_filepath(str(Path(relpath, filename)))))

    dj.errors._switch_filepath_types(False)


def test_filepath_s3():
    """ test file management with s3 """
    test_filepath(store="repo_s3")


def test_duplicate_upload(store="repo"):
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, 'plot.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open('wb') as f:
        f.write(os.urandom(300))
    ext.upload_filepath(str(managed_file))
    ext.upload_filepath(str(managed_file))  # this is fine because the file is the same


def test_duplicate_upload_s3():
    test_duplicate_upload(store="repo_s3")


@raises(dj.DataJointError)
def test_duplicate_error(store="repo"):
    """ syncing duplicate non-matching file should fail """
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, 'thesis.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open('wb') as f:
        f.write(os.urandom(300))
    ext.upload_filepath(str(managed_file))
    with managed_file.open('wb') as f:
        f.write(os.urandom(300))
    # this should raise exception because the file has changed
    ext.upload_filepath(str(managed_file))


def test_duplicate_error_s3():
    test_duplicate_error(store="repo_s3")


def test_filepath_class(table=Filepath(), store="repo"):
    dj.errors._switch_filepath_types(True)
    stage_path = dj.config['stores'][store]['stage']
    # create a mock file
    relative_path = 'one/two/three'
    managed_file = Path(stage_path, relative_path, 'attachment.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    with managed_file.open('wb') as f:
        f.write(data)
    with managed_file.open('rb') as f:
        contents = f.read()
    assert_equal(data, contents)

    # upload file into shared repo
    table.insert1((1, str(managed_file)))

    # remove file locally
    managed_file.unlink()
    assert_false(managed_file.is_file())

    # fetch file from remote
    filepath = (table & {'fnum': 1}).fetch1('img')
    assert_equal(filepath, str(managed_file))

    # verify original contents
    with managed_file.open('rb') as f:
        contents = f.read()
    assert_equal(data, contents)

    # delete from table
    table.delete()
    assert_true(table.external[store])

    # delete from external table
    table.external[store].delete(delete_external_files=True)
    dj.errors._switch_filepath_types(False)


def test_filepath_class_again():
    """test_filepath_class again to deal with existing remote files"""
    test_filepath_class()


def test_filepath_class_s3():
    test_filepath_class(FilepathS3(), "repo_s3")


def test_filepath_class_s3_again():
    """test_filepath_class_s3 again to deal with existing remote files"""
    test_filepath_class(FilepathS3(), "repo_s3")


def test_filepath_cleanup(table=Filepath(), store="repo"):
    """test deletion of filepath entries from external table """

    dj.errors._switch_filepath_types(True)

    stage_path = dj.config['stores'][store]['stage']
    n = 20
    contents = os.urandom(345)
    for i in range(n):
        relative_path = Path(*random.sample(('one', 'two', 'three', 'four'), k=3))
        managed_file = Path(stage_path, relative_path, 'file.dat')
        managed_file.parent.mkdir(parents=True, exist_ok=True)
        with managed_file.open('wb') as f:
            f.write(contents)  # same in all files
        table.insert1((i, str(managed_file)))
    assert_equal(len(table), n)

    ext = schema.external[store]

    assert_equal(len(table), n)
    assert_true(0 < len(ext) < n)

    (table & 'fnum in (1, 2, 3, 4, 5, 6)').delete()
    m = n - len(table)  # number deleted
    assert_true(m == 6)

    ext.delete(delete_external_files=True)  # delete unused entries
    assert_true(0 < len(ext) <= n - m)

    dj.errors._switch_filepath_types(False)


def test_filepath_cleanup_s3():
    """test deletion of filepath entries from external table """
    store = "repo_s3"
    test_filepath_cleanup(FilepathS3(), store)


def test_delete_without_files(store="repo"):
    """test deletion of filepath entries from external table without removing files"""
    dj.errors._switch_filepath_types(True)
    # do not delete unused entries
    schema.external[store].delete(delete_external_files=False)
    dj.errors._switch_filepath_types(False)


def test_return_string(table=Filepath(), store="repo"):
    """ test returning string on fetch """
    dj.errors._switch_filepath_types(True)
    stage_path = dj.config['stores'][store]['stage']
    # create a mock file
    relative_path = 'this/is/a/test'
    managed_file = Path(stage_path, relative_path, 'string.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    with managed_file.open('wb') as f:
        f.write(data)
    with managed_file.open('rb') as f:
        contents = f.read()
    assert_equal(data, contents)

    # upload file into shared repo
    table.insert1((138, str(managed_file)))

    # remove file locally
    managed_file.unlink()
    assert_false(managed_file.is_file())

    # fetch file from remote
    filepath = (table & {'fnum': 138}).fetch1('img')
    assert_true(isinstance(filepath, str))
    dj.errors._switch_filepath_types(False)


class TestFilepathPerformance:
    """ test file path upload/download performance"""

    def setup(self):
        store = 'repo'
        self.table = Filepath()
        self.id_no = 200
        self.ext = schema.external[store]
        self.stage_path = dj.config['stores'][store]['stage']
        dj.errors._switch_filepath_types(True)

        # create a mock file
        self.relative_path = 'path/to/performance/files'
        self.managed_file = Path(self.stage_path, self.relative_path, 'test.dat')
        self.managed_file.parent.mkdir(parents=True, exist_ok=True)

    def teardown(self):
        (self.table & {'fnum': self.id_no}).delete()
        self.ext.delete(delete_external_files=True)
        shutil.rmtree(Path(self.stage_path, self.relative_path.split('/')[0]))
        dj.errors._switch_filepath_types(False)

    def test_performance(self):
        size = 250 * 1024**2  # ~250[MB]
        n = 2 * 4  # ~2[GB]
        # n = 5 * 4  # ~5[GB]
        # n = 10 * 4  # ~10[GB]
        with open(self.managed_file, 'wb') as f:
            [f.write(random.getrandbits(size * 8).to_bytes(length=size,
                                                           byteorder=sys.byteorder))
             for _ in range(n)]

        # upload file into shared repo
        t_insert_start = time.time()
        self.table.insert1((self.id_no, str(self.managed_file)))
        # cProfile.runctx('table.insert1((id_no, str(managed_file)))', globals(), locals())
        insert_delta = time.time() - t_insert_start
        print(f'insert time: {insert_delta}', flush=True)
        assert insert_delta < 12

        # remove file locally
        self.managed_file.unlink()

        # fetch file from remote
        t_fetch_start = time.time()
        filepath = (self.table & {'fnum': self.id_no}).fetch1('img')
        fetch_delta = time.time() - t_fetch_start
        print(f'fetch time: {fetch_delta}', flush=True)
        assert fetch_delta < 9
