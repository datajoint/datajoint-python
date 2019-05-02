from nose.tools import assert_true, assert_equal
import tempfile
import datajoint as dj
import os

from .schema_external import Filepath


def test_filepath():
    """ test file management """
    store = 'repo'
    stage_path = dj.config['stores'][store]['stage']

    # create a mock file
    relpath = 'one/two/three'
    os.makedirs(os.path.join(stage_path, relpath))
    managed_file = os.path.join(stage_path, relpath, 'attachment.dat')
    data = os.urandom(3000)
    with open(managed_file, 'wb') as f:
        f.write(data)

    # put the same file twice
    ext = Filepath().external[store]
    uuid1 = ext.fput(managed_file)
    uuid2 = ext.fput(managed_file)
    os.remove(managed_file)
    relative_path = (ext & {'hash': uuid1}).fetch1('filepath')

    assert_equal(uuid1, uuid2)
    assert_equal(os.path.dirname(relative_path), relpath)
