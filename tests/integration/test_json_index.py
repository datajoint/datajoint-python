"""A JSON-path index declares on both backends (#1672)."""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_json_index_{backend}_{test_id}"[:64]
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def test_declare_json_path_index(schema_by_backend):
    @schema_by_backend
    class Indexed(dj.Manual):
        definition = """
        item_id : int32
        ---
        data = null : json
        index(data.size:decimal(4,1))
        unique index(item_id, data.name:char(20))
        """

    Indexed.insert([dict(item_id=1, data={"size": 3, "name": "a"}), dict(item_id=2, data=None)])
    assert len(Indexed & "item_id = 1") == 1
