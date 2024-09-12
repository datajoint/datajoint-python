"""
Regression tests for issues 386, 449, 484, and 558 — all related to processing complex aggregations and projections.
"""

import pytest
import datajoint as dj
import uuid
from .schema_uuid import Topic, Item, top_level_namespace_id
from .schema_aggr_regress import R, Q, S, A, B, X, LOCALS_AGGR_REGRESS


@pytest.fixture(scope="function")
def schema_aggr_reg(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_aggr_regress",
        context=LOCALS_AGGR_REGRESS,
        connection=connection_test,
    )
    schema(R)
    schema(Q)
    schema(S)
    yield schema
    schema.drop()


@pytest.fixture(scope="function")
def schema_aggr_reg_with_abx(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_aggr_regress_with_abx",
        context=LOCALS_AGGR_REGRESS,
        connection=connection_test,
    )
    schema(R)
    schema(Q)
    schema(S)
    schema(A)
    schema(B)
    schema(X)
    yield schema
    schema.drop()


def test_issue386(schema_aggr_reg):
    """
    --------------- ISSUE 386 -------------------
    Issue 386 resulted from the loss of aggregated attributes when the aggregation was used as the restrictor
        Q & (R.aggr(S, n='count(*)') & 'n=2')
        Error: Unknown column 'n' in HAVING
    """
    result = R.aggr(S, n="count(*)") & "n=10"
    result = Q & result
    result.fetch()


def test_issue449(schema_aggr_reg):
    """
    ---------------- ISSUE 449 ------------------
    Issue 449 arises from incorrect group by attributes after joining with a dj.U()
    """
    result = dj.U("n") * R.aggr(S, n="max(s)")
    result.fetch()


def test_issue484(schema_aggr_reg):
    """
    ---------------- ISSUE 484 -----------------
    Issue 484
    """
    q = dj.U().aggr(S, n="max(s)")
    n = q.fetch("n")
    n = q.fetch1("n")
    q = dj.U().aggr(S, n="avg(s)")
    result = dj.U().aggr(q, m="max(n)")
    result.fetch()


def test_union_join(schema_aggr_reg_with_abx):
    """
    This test fails if it runs after TestIssue558.

    https://github.com/datajoint/datajoint-python/issues/930
    """
    A.insert(zip([100, 200, 300, 400, 500, 600]))
    B.insert([(100, 11), (200, 22), (300, 33), (400, 44)])
    q1 = B & "id < 300"
    q2 = B & "id > 300"

    expected_data = [
        {"id": 0, "id2": 5},
        {"id": 1, "id2": 6},
        {"id": 2, "id2": 7},
        {"id": 3, "id2": 8},
        {"id": 4, "id2": 9},
        {"id": 100, "id2": 11},
        {"id": 200, "id2": 22},
        {"id": 400, "id2": 44},
    ]

    assert ((q1 + q2) * A).fetch(as_dict=True) == expected_data


class TestIssue558:
    """
    ---------------  ISSUE 558 ------------------
    Issue 558 resulted from the fact that DataJoint saves subqueries and often combines a restriction followed
    by a projection into a single SELECT statement, which in several unusual cases produces unexpected results.
    """

    def test_issue558_part1(self, schema_aggr_reg_with_abx):
        q = (A - B).proj(id2="3")
        assert len(A - B) == len(q)

    def test_issue558_part2(self, schema_aggr_reg_with_abx):
        d = dict(id=3, id2=5)
        assert len(X & d) == len((X & d).proj(id2="3"))


def test_left_join_len(schema_uuid):
    Topic().add("jeff")
    Item.populate()
    Topic().add("jeff2")
    Topic().add("jeff3")
    q = Topic.join(
        Item - dict(topic_id=uuid.uuid5(top_level_namespace_id, "jeff")), left=True
    )
    qf = q.fetch()
    assert len(q) == len(qf)
