from nose.tools import assert_true, assert_false, assert_equal, raises
import datajoint as dj
import os
from pathlib import Path
import random

from .schema_external import schema, Filepath, FilepathS3, stores_config


def setUp(self):
    dj.config['stores'] = stores_config


def test_filepath(store="repo"):
    """ test file management """
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    filename = 'picture.dat'

    # create a mock file
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    managed_file.write_bytes(data)

    # put the same file twice to ensure storing once
    uuid1 = ext.upload_filepath(managed_file)
    uuid2 = ext.upload_filepath(managed_file)   # no duplication should arise if file is the same
    assert_equal(uuid1, uuid2)

    # remove to ensure downloading
    managed_file.unlink()
    assert_false(managed_file.exists())

    # Download the file and check its contents. Repeat causes no download from remote
    for _ in 1, 2:
        restored_path, checksum = ext.download_filepath(uuid1)
        assert_equal(restored_path, managed_file)
        assert_equal(checksum, dj.hash.uuid_from_file(managed_file))

    # verify same data
    synced_data = managed_file.read_bytes()
    assert_equal(data, synced_data)

    # cleanup
    ext.delete(delete_external_files=True)


def test_filepath_s3():
    """ test file management with s3 """
    test_filepath(store="repo_s3")


def test_duplicate_upload(store="repo"):
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    relpath = 'one/two/three'
    managed_file = Path(stage_path, relpath, 'plot.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    managed_file.write_bytes(os.urandom(300))
    ext.upload_filepath(managed_file)
    ext.upload_filepath(managed_file)  # this is fine because the file is the same


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
    managed_file.write_bytes(os.urandom(300))
    ext.upload_filepath(managed_file)
    managed_file.write_bytes(os.urandom(300))
    ext.upload_filepath(managed_file)  # this should raise exception because the file has changed


def test_duplicate_error_s3():
    test_duplicate_error(store="repo_s3")


def test_filepath_class(table=Filepath(), store="repo"):
    stage_path = dj.config['stores'][store]['stage']
    # create a mock file
    relative_path = 'one/two/three'
    managed_file = Path(stage_path, relative_path, 'attachment.dat')
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    managed_file.write_bytes(data)
    contents = managed_file.read_bytes()
    assert_equal(data, contents)

    # upload file into shared repo
    table.insert1((1, managed_file))

    # remove file locally
    managed_file.unlink()
    assert_false(managed_file.is_file())

    # fetch file from remote
    filepath = (table & {'fnum': 1}).fetch1('img')
    assert_equal(filepath, managed_file)

    # verify original contents
    contents = managed_file.read_bytes()
    assert_equal(data, contents)

    # delete from table
    table.delete()
    assert_true(table.external[store])

    # delete from external table
    table.external[store].delete(delete_external_files=True)


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
    stage_path = dj.config['stores'][store]['stage']
    n = 20
    contents = os.urandom(345)
    for i in range(n):
        relative_path = Path(*random.sample(('one', 'two', 'three', 'four'), k=3))
        managed_file = Path(stage_path, relative_path, 'file.dat')
        managed_file.parent.mkdir(parents=True, exist_ok=True)
        managed_file.write_bytes(contents)  # same in all files
        table.insert1((i, managed_file))
    assert_equal(len(table), n)

    ext = schema.external[store]

    assert_equal(len(table), n)
    assert_true(0 < len(ext) < n)

    (table & 'fnum in (1, 2, 3, 4, 5, 6)').delete()
    m = n - len(table)  # number deleted
    assert_true(m == 6)

    ext.delete(delete_external_files=True)  # delete unused entries
    assert_true(0 < len(ext) <= n - m)


def test_filepath_cleanup_s3():
    """test deletion of filepath entries from external table """
    store = "repo_s3"
    test_filepath_cleanup(FilepathS3(), store)
