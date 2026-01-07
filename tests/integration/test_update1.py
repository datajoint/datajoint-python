import os
from pathlib import Path

import numpy as np
import pytest

import datajoint as dj
from datajoint import DataJointError


class Thing(dj.Manual):
    definition = """
    thing   :   int
    ---
    number=0  : int
    frac    : float
    picture = null    :   <attach@update_store>
    params = null  : <blob>
    img_file = null: <filepath@update_repo>
    timestamp = CURRENT_TIMESTAMP :   datetime
    """


@pytest.fixture(scope="module")
def mock_stores_update(tmpdir_factory):
    """Configure object storage stores for update tests."""
    og_project_name = dj.config.object_storage.project_name
    og_stores = dict(dj.config.object_storage.stores)

    # Configure stores
    dj.config.object_storage.project_name = "djtest"
    store_location = str(tmpdir_factory.mktemp("store"))
    repo_stage = str(tmpdir_factory.mktemp("repo_stage"))
    repo_location = str(tmpdir_factory.mktemp("repo_loc"))
    dj.config.object_storage.stores["update_store"] = dict(
        protocol="file",
        location=store_location,
    )
    dj.config.object_storage.stores["update_repo"] = dict(
        stage=repo_stage,
        protocol="file",
        location=repo_location,
    )
    yield {"update_store": {"location": store_location}, "update_repo": {"stage": repo_stage, "location": repo_location}}

    # Restore original
    dj.config.object_storage.project_name = og_project_name
    dj.config.object_storage.stores.clear()
    dj.config.object_storage.stores.update(og_stores)


@pytest.fixture
def schema_update1(connection_test, prefix):
    schema = dj.Schema(prefix + "_update1", context=dict(Thing=Thing), connection=connection_test)
    schema(Thing)
    yield schema
    schema.drop()


def test_update1(tmpdir, schema_update1, mock_stores_update):
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

    # filepath - note: <filepath> stores a reference, doesn't move the file
    store_location = mock_stores_update["update_repo"]["location"]
    relpath, filename = "one/two/three", "picture.dat"
    managed_file = Path(store_location, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    original_file_data = os.urandom(3000)
    with managed_file.open("wb") as f:
        f.write(original_file_data)
    # Insert the relative path within the store
    Thing.update1(dict(key, img_file=f"{relpath}/{filename}"))

    with dj.config.override(download_path=str(tmpdir)):
        check2 = Thing.fetch1()
    buffer2 = Path(check2["picture"]).read_bytes()  # read attachment
    # For filepath, fetch returns ObjectRef - read the file through it
    filepath_ref = check2["img_file"]
    final_file_data = filepath_ref.read() if filepath_ref else managed_file.read_bytes()

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

    assert check1["number"] == 0 and check1["picture"] is None and check1["params"] is None

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


def test_update1_nonexistent(schema_update1, mock_stores_update):
    with pytest.raises(DataJointError):
        # updating a non-existent entry
        Thing.update1(dict(thing=100, frac=0.5))


def test_update1_noprimary(schema_update1, mock_stores_update):
    with pytest.raises(DataJointError):
        # missing primary key
        Thing.update1(dict(number=None))


def test_update1_misspelled_attribute(schema_update1, mock_stores_update):
    key = dict(thing=17)
    Thing.insert1(dict(key, frac=1.5))
    with pytest.raises(DataJointError):
        # misspelled attribute
        Thing.update1(dict(key, numer=3))
