from nose.tools import assert_true, assert_not_equal
import tempfile
import filecmp
import shutil
from datajoint import attach
import os

def test_attach():
    """
    test
    """
    attach_file = 'schema.py'
    folder = tempfile.mkdtemp()
    source_file = os.path.join(folder, attach_file)
    shutil.copy(attach_file, source_file)
    buffer = attach.load(source_file)
    download_file = attach.save(buffer, folder)
    assert_true(filecmp.cmp(download_file, source_file))
    assert_not_equal(os.path.basename(attach_file), os.path.basename(download_file))