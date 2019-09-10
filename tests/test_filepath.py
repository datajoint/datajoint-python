from nose.tools import assert_true, assert_false, assert_equal, raises
import datajoint as dj
import os
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
    os.makedirs(os.path.join(stage_path, relpath), exist_ok=True)
    managed_file = os.path.join(stage_path, relpath, filename)
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)

    # put the same file twice to ensure storing once
    uuid1 = ext.fput(managed_file)
    uuid2 = ext.fput(managed_file)   # no duplication should arise if file is the same
    assert_equal(uuid1, uuid2)

    # remove to ensure downloading
    os.remove(managed_file)
    assert_false(os.path.isfile(managed_file))

    # Download the file and check its contents. Repeat causes no download from remote
    for _ in 1, 2:
        restored_path, checksum = ext.fget(uuid1)
        assert_equal(restored_path, managed_file)
        assert_equal(checksum, dj.hash.uuid_from_file(managed_file))

    # verify same data
    with open(managed_file, 'rb') as f:
        synced_data = f.read()
    assert_equal(data, synced_data)

    # cleanup
    ext.delete()


def test_filepath_s3():
    """ test file management with s3 """
    test_filepath(store="repo_s3")


def test_duplicate_upload(store="repo"):
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    filename = 'plot.dat'
    relpath = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relpath), exist_ok=True)
    managed_file = os.path.join(stage_path, relpath, filename)
    with open(managed_file, 'wb') as f:
        f.write(os.urandom(300))
    ext.fput(managed_file)
    ext.fput(managed_file)   # this is fine because the file is the same


def test_duplicate_upload_s3():
    test_duplicate_upload(store="repo_s3")


@raises(dj.DataJointError)
def test_duplicate_error(store="repo"):
    """ syncing duplicate non-matching file should fail """
    ext = schema.external[store]
    stage_path = dj.config['stores'][store]['stage']
    filename = 'thesis.dat'
    relpath = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relpath), exist_ok=True)
    managed_file = os.path.join(stage_path, relpath, filename)
    with open(managed_file, 'wb') as f:
        f.write(os.urandom(300))
    ext.fput(managed_file)
    with open(managed_file, 'wb') as f:
        f.write(os.urandom(300))
    ext.fput(managed_file)  # this should raise exception because the file has changed


def test_duplicate_error_s3():
    test_duplicate_error(store="repo_s3")


def test_filepath_class(table=Filepath(), store="repo"):
    stage_path = dj.config['stores'][store]['stage']
    filename = 'attachment.dat'

    # create a mock file
    relative_path = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relative_path), exist_ok=True)
    managed_file = os.path.join(stage_path, relative_path, filename)
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)
    with open(managed_file, 'rb') as f:
        contents = f.read()
    assert_equal(data, contents)

    # upload file into shared repo
    table.insert1((1, managed_file))

    # remove file locally
    os.remove(managed_file)
    assert_false(os.path.isfile(managed_file))

    # fetch file from remote
    filepath = (table & {'fnum': 1}).fetch1('img')
    assert_equal(filepath, managed_file)

    # verify original contents
    with open(managed_file, 'rb') as f:
        contents = f.read()
    assert_equal(data, contents)

    # delete from table
    table.delete()
    assert_true(table.external[store])

    # delete from external table
    table.external[store].delete()


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
    filename = 'file.dat'
    n = 20
    contents = os.urandom(345)
    for i in range(n):
        relative_path = os.path.join(*random.sample(('one', 'two', 'three', 'four'), k=3))
        os.makedirs(os.path.join(stage_path, relative_path), exist_ok=True)
        managed_file = os.path.join(stage_path, relative_path, filename)
        with open(managed_file, 'wb') as f:
            f.write(contents)   # same contents in all the files
        table.insert1((i, managed_file))
    assert_equal(len(table), n)

    ext = schema.external[store]
    ext.clean_filepaths()

    assert_equal(len(table), n)
    assert_true(0 < len(ext) < n)

    (table & 'fnum in (1, 2, 3, 4, 5, 6)').delete()
    m = n - len(table)  # number deleted
    assert_true(m == 6)

    ext.delete()  # delete unused entries
    assert_true(0 < len(ext) <= n - m)

    unused_files = list(ext.get_untracked_filepaths())
    assert_true(0 < len(unused_files) <= m)

    # check no more untracked files
    ext.clean_filepaths()
    assert_false(bool(list(ext.get_untracked_filepaths())))


def test_filepath_cleanup_s3():
    """test deletion of filepath entries from external table """
    store = "repo_s3"
    test_filepath_cleanup(FilepathS3(), store)
