from nose.tools import assert_true, assert_equal
import tempfile
import datajoint as dj
import os

from .schema_external import Filepath


def test_filepath(store="repo"):
    """ test file management """
    stage_path = dj.config['stores'][store]['stage']
    filename = 'attachment.dat'

    # create a mock file
    relpath = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relpath))
    managed_file = os.path.join(stage_path, relpath, filename)
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)

    # put the same file twice to ensure storing once
    ext = Filepath().external[store]
    uuid1 = ext.fput(managed_file)
    uuid2 = ext.fput(managed_file)
    os.remove(managed_file)
    relative_filepath = (ext & {'hash': uuid1}).fetch1('filepath')

    assert_equal(uuid1, uuid2)
    assert_equal(os.path.dirname(relative_filepath), relpath)

    # download the file and check
    uuid_received = ext.fget(relative_filepath)
    with open(managed_file, 'rb') as f:
        synced_data = f.read()

    assert_equal(uuid1, uuid_received)
    assert_equal(data, synced_data)


def test_filepath_s3():
    """ test file management with s3 """
    test_filepath(store="repo_s3")