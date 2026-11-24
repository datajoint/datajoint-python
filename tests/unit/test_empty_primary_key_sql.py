"""
SQL generated for expressions with an empty primary key, without a database.

``dj.U() & table`` and ``dj.U().aggr(...)`` have no primary key attributes, so they
hold at most one row. ``len()`` must not generate ``count(DISTINCT )`` (#1581), and
a restricted ``dj.U()`` aggregation must keep its restriction (#1580).
"""

from types import SimpleNamespace

import pytest

from datajoint.adapters import get_adapter
from datajoint.condition import AndList
from datajoint.expression import Aggregation, QueryExpression


class _Heading:
    """The parts of a Heading that SQL generation reads."""

    def __init__(self, names, primary_key):
        self.names = names
        self.primary_key = primary_key

    def as_sql(self, fields, include_aliases=True, adapter=None):
        return ", ".join(adapter.quote_identifier(name) for name in fields)


class _RecordingConnection:
    """Records each query and answers it with a single row holding ``1``."""

    def __init__(self, backend):
        self.adapter = get_adapter(backend)
        self.queries = []

    def query(self, sql, *args, **kwargs):
        self.queries.append(sql)
        return SimpleNamespace(fetchone=lambda: (1,))


def _universal_aggregation(backend, restriction=()):
    """``dj.U().aggr(T, n="count(*)")`` over a table T, optionally restricted."""
    aggr = Aggregation()
    aggr._connection = _RecordingConnection(backend)
    aggr._heading = _Heading(names=["n"], primary_key=[])
    aggr._support = ["t"]
    aggr._joins = []
    aggr._left_restrict = AndList()
    aggr._grouping_attributes = []
    aggr._restriction = AndList(restriction)
    return aggr


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_restricted_universal_aggregation_keeps_restriction(backend):
    sql = _universal_aggregation(backend, ["n > 1"]).make_sql()
    assert "GROUP BY" not in sql
    if backend == "mysql":
        assert sql.endswith(" HAVING (n > 1)")
    else:
        assert sql.endswith(" WHERE (n > 1)")


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_unrestricted_universal_aggregation(backend):
    sql = _universal_aggregation(backend).make_sql()
    assert "GROUP BY" not in sql
    assert "HAVING" not in sql
    assert "WHERE" not in sql


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_aggregation_restricted_by_false(backend):
    """A restriction by False is stored as a bool and must not break the join of conditions."""
    sql = _universal_aggregation(backend, [False]).make_sql()
    assert sql.endswith("(False)")


def _expression(backend, names, distinct):
    """A query expression over a table t with an empty primary key."""
    expr = QueryExpression()
    expr._connection = _RecordingConnection(backend)
    expr._heading = _Heading(names=names, primary_key=[])
    expr._support = ["t"]
    expr._joins = []
    expr._distinct = distinct
    return expr


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_len_with_empty_primary_key_checks_existence(backend):
    """``dj.U() & table``: deduplicated, no attributes, at most one row."""
    expr = _expression(backend, names=[], distinct=True)
    assert len(expr) == 1
    assert all("DISTINCT" not in sql for sql in expr.connection.queries)
    assert expr.connection.queries[-1].startswith("SELECT EXISTS(")


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
@pytest.mark.parametrize("names", [["msg"], []])
def test_len_without_primary_key_counts_every_row(backend, names):
    """A table without a primary key, or its projection onto no attributes, counts every row."""
    expr = _expression(backend, names=names, distinct=False)
    assert len(expr) == 1
    assert expr.connection.queries == ["SELECT count(*) FROM t"]
