"""A primary key cannot silently take a constant default (#1712)."""

import pytest

from datajoint.adapters import MySQLAdapter, PostgreSQLAdapter
from datajoint.declare import compile_attribute, prepare_declare
from datajoint.errors import DataJointError


@pytest.mark.parametrize("adapter_type", [MySQLAdapter, PostgreSQLAdapter], ids=["mysql", "postgresql"])
@pytest.mark.parametrize(
    "line",
    [
        "probe_id = 1 : int32",
        "probe_id = 'fixed' : varchar(16)",
        'probe_id = "fixed" : varchar(16)',
    ],
)
def test_constant_primary_key_defaults_are_rejected(adapter_type, line):
    """Reject constants during declaration, before touching a database."""
    with pytest.raises(DataJointError, match=r"Primary key attribute 'probe_id'.*constant default"):
        prepare_declare(f"{line}\n---\nlabel : varchar(16)", {}, adapter_type())


@pytest.mark.parametrize("adapter_type", [MySQLAdapter, PostgreSQLAdapter], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("default", ["CURRENT_TIMESTAMP", "current_timestamp", "CURRENT_TIMESTAMP(3)"])
def test_generated_timestamp_default_is_allowed_on_key(adapter_type, default):
    """CURRENT_TIMESTAMP is generated per insert, unlike a fixed literal."""
    name, sql, _store, _comment = compile_attribute(
        f"visit_ts = {default} : datetime", in_key=True, foreign_key_sql=[], context={}, adapter=adapter_type()
    )
    assert name == "visit_ts"
    assert "DEFAULT " in sql
    assert default in sql


@pytest.mark.parametrize("adapter_type", [MySQLAdapter, PostgreSQLAdapter], ids=["mysql", "postgresql"])
def test_primary_key_without_default_is_unchanged(adapter_type):
    name, sql, _store, _comment = compile_attribute(
        "probe_id : int32", in_key=True, foreign_key_sql=[], context={}, adapter=adapter_type()
    )
    assert name == "probe_id"
    assert "DEFAULT" not in sql


@pytest.mark.parametrize("adapter_type", [MySQLAdapter, PostgreSQLAdapter], ids=["mysql", "postgresql"])
@pytest.mark.parametrize("default", ["1", "'fixed'"])
def test_secondary_attribute_constant_defaults_are_unchanged(adapter_type, default):
    name, sql, _store, _comment = compile_attribute(
        f"label = {default} : varchar(16)", in_key=False, foreign_key_sql=[], context={}, adapter=adapter_type()
    )
    assert name == "label"
    assert "DEFAULT " in sql
