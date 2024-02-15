import pytest
import os
import numpy as np
from pathlib import Path
import tempfile
import datajoint as dj
from datajoint import DataJointError


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


@pytest.fixture(scope="module")
def mock_stores_update(tmpdir_factory):
    og_stores_config = dj.config.get("stores")
    if "stores" not in dj.config:
        dj.config["stores"] = {}
    dj.config["stores"]["update_store"] = dict(
        protocol="file", location=tmpdir_factory.mktemp("store")
    )
    dj.config["stores"]["update_repo"] = dict(
        stage=tmpdir_factory.mktemp("repo_stage"),
        protocol="file",
        location=tmpdir_factory.mktemp("repo_loc"),
    )
    yield
    if og_stores_config is None:
        del dj.config["stores"]
    else:
        dj.config["stores"] = og_stores_config


@pytest.fixture
def schema_update1(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_update1", context=dict(Thing=Thing), connection=connection_test
    )
    schema(Thing)
    yield schema
    schema.drop()


def test_update1(tmpdir, enable_filepath_feature, schema_update1, mock_stores_update):
    """Test normal updates"""
    # CHECK 1 -- initial insert
    key = dict(thing=1)
    Thing.insert1(dict(key, frac=0.5))
    check1 = Thing.fetch1()

    # CHECK 2 -- some updates
    # numbers and datetimes
    Thing.update1(dict(key, number=3, frac=30, timestamp="2020-01-01 10:00:00"))
    # attachment
    attach_file = Path(tmpdir, "attach1.dat")
    buffer1 = os.urandom(100)
    attach_file.write_bytes(buffer1)
    Thing.update1(dict(key, picture=attach_file))
    attach_file.unlink()
    assert not attach_file.is_file()

    # filepath
    stage_path = dj.config["stores"]["update_repo"]["stage"]
    relpath, filename = "one/two/three", "picture.dat"
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    original_file_data = os.urandom(3000)
    with managed_file.open("wb") as f:
        f.write(original_file_data)
    Thing.update1(dict(key, img_file=managed_file))
    managed_file.unlink()
    assert not managed_file.is_file()

    check2 = Thing.fetch1(download_path=tmpdir)
    buffer2 = Path(check2["picture"]).read_bytes()  # read attachment
    final_file_data = managed_file.read_bytes()  # read filepath

    # CHECK 3 -- reset to default values using None
    Thing.update1(
        dict(
            key,
            number=None,
            timestamp=None,
            picture=None,
            img_file=None,
            params=np.random.randn(3, 3),
        )
    )
    check3 = Thing.fetch1()

    assert (
        check1["number"] == 0 and check1["picture"] is None and check1["params"] is None
    )

    assert (
        check2["number"] == 3
        and check2["frac"] == 30.0
        and check2["picture"] is not None
        and check2["params"] is None
        and buffer1 == buffer2
    )

    assert (
        check3["number"] == 0
        and check3["frac"] == 30.0
        and check3["picture"] is None
        and check3["img_file"] is None
        and isinstance(check3["params"], np.ndarray)
    )

    assert check3["timestamp"] > check2["timestamp"]
    assert buffer1 == buffer2
    assert original_file_data == final_file_data


def test_update1_nonexistent(
    enable_filepath_feature, schema_update1, mock_stores_update
):
    with pytest.raises(DataJointError):
        # updating a non-existent entry
        Thing.update1(dict(thing=100, frac=0.5))


def test_update1_noprimary(enable_filepath_feature, schema_update1, mock_stores_update):
    with pytest.raises(DataJointError):
        # missing primary key
        Thing.update1(dict(number=None))


def test_update1_misspelled_attribute(
    enable_filepath_feature, schema_update1, mock_stores_update
):
    key = dict(thing=17)
    Thing.insert1(dict(key, frac=1.5))
    with pytest.raises(DataJointError):
        # misspelled attribute
        Thing.update1(dict(key, numer=3))
