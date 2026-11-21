"""
Unit tests for ``dependencies.extract_master``: a part table's master is found
whatever characters the schema name holds, on both identifier-quoting styles.
"""

import pytest

from datajoint.adapters.mysql import MySQLAdapter
from datajoint.adapters.postgres import PostgreSQLAdapter
from datajoint.dependencies import extract_master

SCHEMA_NAMES = ["lab", "parts-check", "lab.v2", "lab-2.v3", "lab$x", "lab check"]


@pytest.mark.parametrize("q", ["`", '"'])
@pytest.mark.parametrize("schema", SCHEMA_NAMES)
@pytest.mark.parametrize(
    "master, part",
    [
        ("session", "session__trial"),
        ("#stimulus", "#stimulus__frame"),
        ("_scan", "_scan__field"),
        ("__analysis", "__analysis__unit"),
        ("session__trial", "session__trial__spike"),
    ],
)
def test_extract_master_finds_master(q, schema, master, part):
    full = f"{q}{schema}{q}.{q}{part}{q}"
    assert extract_master(full) == f"{q}{schema}{q}.{q}{master}{q}"


@pytest.mark.parametrize("q", ["`", '"'])
@pytest.mark.parametrize("schema", SCHEMA_NAMES)
@pytest.mark.parametrize("table", ["session", "#stimulus", "_scan", "__analysis", "~lineage"])
def test_extract_master_ignores_non_part_tables(q, schema, table):
    assert extract_master(f"{q}{schema}{q}.{q}{table}{q}") is None


def test_extract_master_requires_matching_quotes():
    assert extract_master('`lab`."session__trial"') is None
    assert extract_master('"lab`.`session__trial"') is None


@pytest.mark.parametrize(
    "adapter, q",
    [(MySQLAdapter.__new__(MySQLAdapter), "`"), (PostgreSQLAdapter.__new__(PostgreSQLAdapter), '"')],
)
def test_adapter_get_master_table_name_matches_extract_master(adapter, q):
    full = f"{q}parts-check{q}.{q}session__trial{q}"
    assert adapter.get_master_table_name(full) == f"{q}parts-check{q}.{q}session{q}"
    assert adapter.get_master_table_name(f"{q}parts-check{q}.{q}session{q}") is None
