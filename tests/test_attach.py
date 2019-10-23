from nose.tools import assert_true, assert_equal, assert_not_equal
from numpy.testing import assert_array_equal
import tempfile
from pathlib import Path
import os

from .schema_external import Attach


def test_attach_attributes():
    """ test saving files in attachments """
    # create a mock file
    table = Attach()
    source_folder = tempfile.mkdtemp()
    for i in range(2):
        attach1 = Path(source_folder, 'attach1.img')
        data1 = os.urandom(100)
        with attach1.open('wb') as f:
            f.write(data1)
        attach2 = Path(source_folder, 'attach2.txt')
        data2 = os.urandom(200)
        with attach2.open('wb') as f:
            f.write(data2)
        table.insert1(dict(attach=i, img=attach1, txt=attach2))

    download_folder = Path(tempfile.mkdtemp())
    keys, path1, path2 = table.fetch("KEY", 'img', 'txt', download_path=download_folder, order_by="KEY")

    # verify that different attachment are renamed if their filenames collide
    assert_not_equal(path1[0], path2[0])
    assert_not_equal(path1[0], path1[1])
    assert_equal(Path(path1[0]).parent, download_folder)
    with Path(path1[-1]).open('rb') as f:
        check1 = f.read()
    with Path(path2[-1]).open('rb') as f:
        check2 = f.read()
    assert_equal(data1, check1)
    assert_equal(data2, check2)

    # verify that existing files are not duplicated if their filename matches issue #592
    p1, p2 = (Attach & keys[0]).fetch1('img', 'txt', download_path=download_folder)
    assert_equal(p1, path1[0])
    assert_equal(p2, path2[0])


def test_return_string():
    """ test returning string on fetch """
    # create a mock file
    table = Attach()
    source_folder = tempfile.mkdtemp()

    attach1 = Path(source_folder, 'attach1.img')
    data1 = os.urandom(100)
    with attach1.open('wb') as f:
        f.write(data1)
    attach2 = Path(source_folder, 'attach2.txt')
    data2 = os.urandom(200)
    with attach2.open('wb') as f:
        f.write(data2)
    table.insert1(dict(attach=2, img=attach1, txt=attach2))

    download_folder = Path(tempfile.mkdtemp())
    keys, path1, path2 = table.fetch(
            "KEY", 'img', 'txt', download_path=download_folder, order_by="KEY")

    assert_true(isinstance(path1[0], str))
