"""
Renaming an attribute with ``alter()`` keeps its values.

A comment that opens with ``{old_name}`` renames that attribute. On MySQL the
rename is a CHANGE, which keeps the values; PostgreSQL refuses the rename
instead of dropping the attribute and adding an empty one.
"""

import time

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    schema_name = f"djtest_alter_rename_{backend}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


@pytest.mark.parametrize("nullable", [False, True], ids=["required", "nullable"])
def test_rename_keeps_values(schema_by_backend, db_creds_by_backend, nullable):
    """Rename a core-type attribute of a table that holds rows."""
    default = " = null" if nullable else ""

    @schema_by_backend
    class Measurement(dj.Manual):
        definition = f"""
        measurement_id : int32
        ---
        value{default} : float32   # the value
        """

    Measurement.insert([(1, 2.5), (2, 4.0)])
    Measurement.definition = f"""
    measurement_id : int32
    ---
    reading{default} : float32   # {{value}} the value
    """

    if db_creds_by_backend["backend"] == "mysql":
        Measurement.alter(prompt=False)
        assert Measurement.heading.names == ["measurement_id", "reading"]
        assert Measurement.to_dicts(order_by="measurement_id") == [
            {"measurement_id": 1, "reading": 2.5},
            {"measurement_id": 2, "reading": 4.0},
        ]
    else:
        with pytest.raises(DataJointError, match="Renaming attribute `value` to `reading`"):
            Measurement.alter(prompt=False)
        assert dj.FreeTable(schema_by_backend.connection, Measurement.full_table_name).to_dicts(order_by="measurement_id") == [
            {"measurement_id": 1, "value": 2.5},
            {"measurement_id": 2, "value": 4.0},
        ]
