from nose.tools import assert_true, assert_false, assert_equal, assert_not_equal, raises
import datajoint as dj
import os

from .schema_external import schema, Filepath


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
    with open(managed_file, 'rb') as f:
        contents = f.read()
    assert_equal(data, contents)

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
