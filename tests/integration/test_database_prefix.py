"""
The database_prefix deprecation warning names no release that has already shipped (#1675).

``database.database_prefix`` is deprecated. ``Schema.activate()`` warns when it is set, and the
prefix is not applied to schema names.
"""

import time
import warnings

import pytest

import datajoint as dj


@pytest.fixture
def schema_name(connection_by_backend, db_creds_by_backend):
    """A fresh schema name, dropped after the test."""
    name = f"djtest_prefix_{db_creds_by_backend['backend']}_{str(int(time.time() * 1000))[-8:]}"
    yield name
    drop = f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(name)}"
    if connection_by_backend.is_connected:
        connection_by_backend.query(drop)


def test_prefix_warning_names_no_shipped_release(connection_by_backend, schema_name):
    with dj.config.override(database__database_prefix="myproject_"):
        with pytest.warns(DeprecationWarning, match="database_prefix") as record:
            schema = dj.Schema(schema_name, connection=connection_by_backend)
    messages = [str(w.message) for w in record if "database_prefix" in str(w.message)]
    assert messages
    for message in messages:
        assert "removed in a future release" in message
        assert "2.3" not in message
    assert schema.database == schema_name  # the prefix is not applied


def test_no_warning_without_prefix(connection_by_backend, schema_name):
    with dj.config.override(database__database_prefix=""):
        with warnings.catch_warnings(record=True) as record:
            warnings.simplefilter("always")
            dj.Schema(schema_name, connection=connection_by_backend)
    assert not [w for w in record if "database_prefix" in str(w.message)]
