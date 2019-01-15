from nose.tools import assert_true, assert_not_equal
import tempfile
import filecmp
from datajoint import attach
import os


def test_attach():
    """
    test attaching files and writing attached files
    """
    folder = tempfile.mkdtemp()
    attach_file = os.path.join(folder, 'attachment.dat')
    with open(attach_file, 'wb') as f:
        f.write(os.urandom(3000))
    buffer = attach.load(attach_file)
    download_file = attach.save(buffer, folder)
    assert_true(filecmp.cmp(download_file, attach_file))
    assert_not_equal(os.path.basename(attach_file), os.path.basename(download_file))