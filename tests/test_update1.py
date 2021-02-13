from nose.tools import assert_true, assert_false, assert_equal, raises
import os
import numpy as np
from pathlib import Path
import tempfile
import datajoint as dj
from . import PREFIX, CONN_INFO
from datajoint import DataJointError

schema = dj.Schema(PREFIX + '_update1', connection=dj.conn(**CONN_INFO))

dj.config['stores']['update_store'] = dict(
    protocol='file',
    location=tempfile.mkdtemp())

dj.config['stores']['update_repo'] = dict(
    stage=tempfile.mkdtemp(),
    protocol='file',
    location=tempfile.mkdtemp())


scratch_folder = tempfile.mkdtemp()

dj.errors._switch_filepath_types(True)


@schema
class Thing(dj.Manual):
    definition = """
    thing   :   int    
    ---
    number=0  : int   
    frac    : float
    picture = null    :   attach@update_store
    params = null  : longblob 
    img_file = null: filepath@update_repo
    timestamp = CURRENT_TIMESTAMP :   datetime
    """


def test_update1():
    """test normal updates"""

    dj.errors._switch_filepath_types(True)
    # CHECK 1 -- initial insert
    key = dict(thing=1)
    Thing.insert1(dict(key, frac=0.5))
    check1 = Thing.fetch1()

    # CHECK 2 -- some updates
    # numbers and datetimes
    Thing.update1(dict(key, number=3, frac=30, timestamp="2020-01-01 10:00:00"))
    # attachment
    attach_file = Path(scratch_folder, 'attach1.dat')
    buffer1 = os.urandom(100)
    attach_file.write_bytes(buffer1)
    Thing.update1(dict(key, picture=attach_file))
    attach_file.unlink()
    assert_false(attach_file.is_file())

    # filepath
    stage_path = dj.config['stores']['update_repo']['stage']
    relpath, filename = 'one/two/three', 'picture.dat'
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    original_file_data = os.urandom(3000)
    with managed_file.open('wb') as f:
        f.write(original_file_data)
    Thing.update1(dict(key, img_file=managed_file))
    managed_file.unlink()
    assert_false(managed_file.is_file())

    check2 = Thing.fetch1(download_path=scratch_folder)
    buffer2 = Path(check2['picture']).read_bytes()   # read attachment
    final_file_data = managed_file.read_bytes()    # read filepath

    # CHECK 3 -- reset to default values using None
    Thing.update1(dict(key, number=None, timestamp=None, picture=None, img_file=None, params=np.random.randn(3, 3)))
    check3 = Thing.fetch1()

    assert_true(check1['number'] == 0 and
                check1['picture'] is None and
                check1['params'] is None)

    assert_true(check2['number'] == 3 and
                check2['frac'] == 30.0 and
                check2['picture'] is not None and
                check2['params'] is None and buffer1==buffer2)

    assert_true(check3['number'] == 0 and
                check3['frac'] == 30.0 and
                check3['picture'] is None and
                check3['img_file'] is None and
                isinstance(check3['params'], np.ndarray))

    assert_true(check3['timestamp'] > check2['timestamp'])
    assert_equal(buffer1, buffer2)
    assert_equal(original_file_data, final_file_data)


@raises(DataJointError)
def test_update1_nonexistent():
    Thing.update1(dict(thing=100, frac=0.5))   # updating a non-existent entry


@raises(DataJointError)
def test_update1_noprimary():
    Thing.update1(dict(number=None))  # missing primary key


@raises(DataJointError)
def test_update1_misspelled_attribute():
    key = dict(thing=17)
    Thing.insert1(dict(key, frac=1.5))
    Thing.update1(dict(key, numer=3))    # misspelled attribute
