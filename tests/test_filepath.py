from nose.tools import assert_true, assert_false, assert_equal
import datajoint as dj
import os

from .schema_external import Filepath


def test_filepath(store="repo"):
    """ test file management """
    stage_path = dj.config['stores'][store]['stage']
    filename = 'attachment.dat'

    # create a mock file
    relpath = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relpath), exist_ok=True)
    managed_file = os.path.join(stage_path, relpath, filename)
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)

    # put the same file twice to ensure storing once
    ext = Filepath().external[store]
    uuid1 = ext.fput(managed_file)
    uuid2 = ext.fput(managed_file)
    os.remove(managed_file)   # remove to ensure downloading
    assert_false(os.path.isfile(managed_file))
    relative_filepath, contents_hash = (ext & {'hash': uuid1}).fetch1('filepath', 'contents_hash')

    assert_equal(uuid1, uuid2)
    assert_equal(os.path.dirname(relative_filepath), relpath)

    # download the file and check its contents
    restored_path, uuid_received = ext.fget(relative_filepath)
    assert_equal(restored_path, managed_file)
    assert_equal(uuid_received, contents_hash)
    assert_equal(uuid_received, dj.hash.uuid_from_file(managed_file))

    # repeated sync does trigger download
    restored_path, uuid_received = ext.fget(relative_filepath)
    assert_equal(restored_path, managed_file)
    assert_true(uuid_received is None)

    with open(managed_file, 'rb') as f:
        synced_data = f.read()

    assert_equal(data, synced_data)


def test_filepath_s3():
    """ test file management with s3 """
    test_filepath(store="repo_s3")


def test_filepath_class():

    stage_path = dj.config['stores']["repo"]['stage']
    filename = 'attachment.dat'

    # create a mock file
    relative_path = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relative_path), exist_ok=True)
    managed_file = os.path.join(stage_path, relative_path, filename)
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)

    # upload file into shared repo
    Filepath().insert1((1, managed_file))

    # remove file locally
    os.remove(managed_file)
    assert_false(os.path.isfile(managed_file))

    # fetch file from remote
    filepath = (Filepath & {'fnum': 1}).fetch1('img')
    assert_equal(filepath, managed_file)

    # verify original contents
    with open(managed_file, 'rb') as f:
        contents = f.read()
    assert_equal(data, contents)
