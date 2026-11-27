"""
Unit tests for nullability in derived headings and the dj.U non-null key check (issue #1708).

The checks are static, so the tests build headings directly and need no database.
Modules are resolved at run time because other unit tests re-import datajoint from scratch.
"""

import importlib
from types import SimpleNamespace

import pytest


def module(name):
    return importlib.import_module(name)


@pytest.fixture
def dj():
    return module("datajoint")


def make_heading(pk, secondary=(), nullable=()):
    heading = module("datajoint.heading")
    props = heading.default_attribute_properties
    return heading.Heading(
        [dict(props, name=n, type="int", in_key=True) for n in pk]
        + [dict(props, name=n, type="int", nullable=n in nullable) for n in secondary]
    )


_connections = {}


def make_query(pk, secondary=(), nullable=()):
    adapters = module("datajoint.adapters")
    if adapters not in _connections:  # one shared connection per import of datajoint
        _connections[adapters] = SimpleNamespace(adapter=adapters.MySQLAdapter())
    query = module("datajoint.expression").QueryExpression()
    query._connection = _connections[adapters]
    query._heading = make_heading(pk, secondary, nullable)
    query._support = ["t"]
    return query


def test_computed_attributes_are_nullable():
    heading = make_heading(["a"], ["b"]).select(["a", "b"], compute_map={"c": "max(b)"})
    assert heading.attributes["c"].nullable
    assert not heading.attributes["b"].nullable


def test_left_join_pads_right_attributes():
    heading = make_heading(["a", "t"], ["x"]).join(make_heading(["a"], ["y"]), left=True)
    assert heading.attributes["y"].nullable
    assert not heading.attributes["x"].nullable
    inner = make_heading(["a", "t"], ["x"]).join(make_heading(["a"], ["y"]))
    assert not inner.attributes["y"].nullable


def test_union_pads_secondary_attributes():
    q1, q2 = make_query(["a"], ["x"]), make_query(["a"], ["y"])
    heading = module("datajoint.expression").Union.create(q1, q2).heading
    assert heading.attributes["x"].nullable and heading.attributes["y"].nullable
    assert not heading.attributes["a"].nullable


@pytest.mark.parametrize("attrs", [("x",), ("a", "x")])
def test_universal_set_refuses_nullable_attributes(dj, attrs):
    query = make_query(["a"], ["x"], nullable=["x"])
    with pytest.raises(dj.DataJointError, match="IS NOT NULL"):
        dj.U(*attrs) & query
    with pytest.raises(dj.DataJointError, match="IS NOT NULL"):
        dj.U(*attrs).aggr(query, n="count(*)")


def test_universal_set_refuses_computed_attribute(dj):
    query = make_query(["a"], ["x"]).proj(c="x + 1")
    with pytest.raises(dj.DataJointError, match="'c'"):
        dj.U("c") & query


@pytest.mark.parametrize("condition", ["x IS NOT NULL", "`x` is not null", ["a > 1", "x IS NOT NULL"]])
def test_is_not_null_restriction_clears_nullable(dj, condition):
    if isinstance(condition, list):
        condition = dj.AndList(condition)
    query = make_query(["a"], ["x"], nullable=["x"]) & condition
    assert not query.heading.attributes["x"].nullable
    assert (dj.U("x") & query).primary_key == ["x"]
    assert dj.U("x").aggr(query, n="count(*)").primary_key == ["x"]


def test_restriction_by_query_clears_nullable(dj):
    query = make_query(["a"], ["x"], nullable=["x"])
    matched = query.restrict(make_query(["x"]), semantic_check=False)
    assert not matched.heading.attributes["x"].nullable
    unmatched = query.restrict(dj.Not(make_query(["x"])), semantic_check=False)
    assert unmatched.heading.attributes["x"].nullable


def test_universal_set_accepts_non_nullable_attributes(dj):
    query = make_query(["a"], ["x"])
    assert (dj.U("x") & query).primary_key == ["x"]
    assert dj.U().aggr(query, n="count(*)").primary_key == []
