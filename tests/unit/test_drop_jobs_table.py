"""
Unit tests: drop_quick() drops the jobs table of a Computed or Imported table,
and of no other table (#1601). The database is a mock that records each query.
"""

from unittest.mock import MagicMock, patch

import pytest

from datajoint.adapters import MySQLAdapter, PostgreSQLAdapter
from datajoint.table import FreeTable


def _connection(adapter, declared):
    """A mock connection whose tables in ``declared`` exist; records every query."""
    conn = MagicMock()
    conn.adapter = adapter
    info_sql = {adapter.get_table_info_sql("s", name) for name in declared}

    def query(sql, args=None, **kwargs):
        result = MagicMock()
        result.rowcount = 1 if sql in info_sql else 0
        result.fetchall.return_value = []
        return result

    conn.query.side_effect = query
    return conn


def _dropped(adapter, table_name, declared):
    conn = _connection(adapter, declared)
    with patch("datajoint.lineage.delete_table_lineages"):
        FreeTable(conn, adapter.make_full_table_name("s", table_name)).drop_quick()
    return [c.args[0] for c in conn.query.call_args_list if c.args[0].startswith("DROP TABLE")]


@pytest.fixture(params=["mysql", "postgresql"])
def adapter(request):
    if request.param == "mysql":
        return MySQLAdapter()
    pytest.importorskip("psycopg2")
    return PostgreSQLAdapter()


@pytest.mark.parametrize("table_name", ["__result", "_recording"])
def test_auto_populated_table_drops_its_jobs_table(adapter, table_name):
    jobs_name = "~~" + table_name.lstrip("_")
    dropped = _dropped(adapter, table_name, declared={table_name, jobs_name})
    assert dropped == [
        f"DROP TABLE {adapter.make_full_table_name('s', table_name)}",
        f"DROP TABLE {adapter.make_full_table_name('s', jobs_name)}",
    ]


@pytest.mark.parametrize("table_name", ["result", "#result", "__result__part"])
def test_other_tiers_leave_jobs_tables(adapter, table_name):
    """A Manual, Lookup or Part table never drops a jobs table, even one its name maps to."""
    dropped = _dropped(adapter, table_name, declared={table_name, "~~result", "~~result__part"})
    assert dropped == [f"DROP TABLE {adapter.make_full_table_name('s', table_name)}"]


def test_missing_jobs_table_is_skipped(adapter):
    """A Computed table whose jobs table was never declared drops only itself."""
    dropped = _dropped(adapter, "__result", declared={"__result"})
    assert dropped == [f"DROP TABLE {adapter.make_full_table_name('s', '__result')}"]
