"""
Unit tests for :mod:`datajoint.deploy`.

These tests do not require a live PostgreSQL connection — they cover dispatch,
validation, and DDL string generation against the actual ``PostgreSQLAdapter``
and a stub adapter for the non-PG path.
"""

from __future__ import annotations

import pytest

import datajoint as dj
from datajoint.deploy import set_replica_identity
from datajoint.errors import DataJointError


class _FakeAdapter:
    """Bare-minimum adapter stub for testing dispatch (PostgreSQL-shaped)."""

    backend = "postgresql"

    def make_full_table_name(self, schema: str, table: str) -> str:
        return f'"{schema}"."{table}"'

    def replica_identity_ddl(self, full_table_name: str, mode: str) -> str:
        return f"ALTER TABLE {full_table_name} REPLICA IDENTITY {mode.upper()}"


class _MySQLLikeAdapter:
    """Adapter without ``replica_identity_ddl`` (MySQL-shaped)."""

    backend = "mysql"

    def make_full_table_name(self, schema: str, table: str) -> str:
        return f"`{schema}`.`{table}`"


class _FakeConnection:
    """Connection stub that records queries instead of executing them."""

    def __init__(self, adapter: object) -> None:
        self.adapter = adapter
        self.queries: list[str] = []

    def query(self, sql: str) -> None:
        self.queries.append(sql)


class _FakeSchema(dj.schemas._Schema):
    """Schema stub bypassing __init__ wiring; sets just what set_replica_identity uses."""

    def __init__(self, database: str, table_names: list[str], adapter: object) -> None:
        # Skip dj.Schema.__init__ — fabricate the minimal attributes.
        self.database = database
        self._tables = table_names
        self.connection = _FakeConnection(adapter)

    def list_tables(self) -> list[str]:
        return self._tables


def test_set_replica_identity_rejects_invalid_mode():
    schema = _FakeSchema("ms", ["t1"], _FakeAdapter())
    with pytest.raises(DataJointError, match="mode must be 'default' or 'full'"):
        set_replica_identity(schema, mode="nothing")


def test_set_replica_identity_rejects_bad_target():
    with pytest.raises(DataJointError, match="must be a Schema or Table"):
        set_replica_identity("not a schema", mode="full")


def test_set_replica_identity_rejects_non_postgresql():
    schema = _FakeSchema("ms", ["t1", "t2"], _MySQLLikeAdapter())
    with pytest.raises(DataJointError, match="PostgreSQL-only"):
        set_replica_identity(schema, mode="full")


def test_set_replica_identity_dry_run_no_execute():
    schema = _FakeSchema("ms", ["t1", "t2"], _FakeAdapter())
    result = set_replica_identity(schema, mode="full", dry_run=True)
    assert result["tables_analyzed"] == 2
    assert result["tables_modified"] == 0
    assert result["ddl"] == [
        'ALTER TABLE "ms"."t1" REPLICA IDENTITY FULL',
        'ALTER TABLE "ms"."t2" REPLICA IDENTITY FULL',
    ]
    assert schema.connection.queries == []


def test_set_replica_identity_apply_runs_alters():
    schema = _FakeSchema("ms", ["t1", "t2"], _FakeAdapter())
    result = set_replica_identity(schema, mode="full", dry_run=False)
    assert result["tables_analyzed"] == 2
    assert result["tables_modified"] == 2
    assert schema.connection.queries == result["ddl"]


def test_set_replica_identity_default_mode_emits_default_ddl():
    schema = _FakeSchema("ms", ["t1"], _FakeAdapter())
    result = set_replica_identity(schema, mode="default", dry_run=True)
    assert result["ddl"] == ['ALTER TABLE "ms"."t1" REPLICA IDENTITY DEFAULT']


def test_set_replica_identity_empty_schema():
    schema = _FakeSchema("ms", [], _FakeAdapter())
    result = set_replica_identity(schema, mode="full", dry_run=False)
    assert result == {"tables_analyzed": 0, "tables_modified": 0, "ddl": []}
