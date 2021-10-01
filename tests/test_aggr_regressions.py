"""
Regression tests for issues 386, 449, 484, and 558 — all related to processing complex aggregations and projections.
"""

import itertools
from nose.tools import assert_equal, raises
import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.Schema(PREFIX + '_aggr_regress', connection=dj.conn(**CONN_INFO))

# --------------- ISSUE 386 -------------------
# Issue 386 resulted from the loss of aggregated attributes when the aggregation was used as the restrictor
#     Q & (R.aggr(S, n='count(*)') & 'n=2')
#     Error: Unknown column 'n' in HAVING


@schema
class R(dj.Lookup):
    definition = """
    r : char(1)
    """
    contents = zip('ABCDFGHIJKLMNOPQRST')


@schema
class Q(dj.Lookup):
    definition = """
    -> R
    """
    contents = zip('ABCDFGH')


@schema
class S(dj.Lookup):
    definition = """
    -> R
    s : int
    """
    contents = itertools.product('ABCDF', range(10))


def test_issue386():
    result = R.aggr(S, n='count(*)') & 'n=10'
    result = Q & result
    result.fetch()

# ---------------- ISSUE 449 ------------------
# Issue 449 arises from incorrect group by attributes after joining with a dj.U()


def test_issue449():
    result = dj.U('n') * R.aggr(S, n='max(s)')
    result.fetch()


# ---------------- ISSUE 484 -----------------
# Issue 484
def test_issue484():
    q = dj.U().aggr(S, n='max(s)')
    n = q.fetch('n')
    n = q.fetch1('n')
    q = dj.U().aggr(S, n='avg(s)')
    result = dj.U().aggr(q, m='max(n)')
    result.fetch()

# ---------------  ISSUE 558 ------------------
#  Issue 558 resulted from the fact that DataJoint saves subqueries and often combines a restriction followed
#  by a projection into a single SELECT statement, which in several unusual cases produces unexpected results.


@schema
class A(dj.Lookup):
    definition = """
    id: int
    """
    contents = zip(range(10))


@schema
class B(dj.Lookup):
    definition = """
    -> A
    id2: int
    """
    contents = zip(range(5), range(5, 10))


@schema
class X(dj.Lookup):
    definition = """
    id: int
    """
    contents = zip(range(10))


def test_issue558_part1():
    q = (A-B).proj(id2='3')
    assert_equal(len(A - B), len(q))


def test_issue558_part2():
    d = dict(id=3, id2=5)
    assert_equal(len(X & d), len((X & d).proj(id2='3')))


def test_union_join():
    # https://github.com/datajoint/datajoint-python/issues/930
    A.insert(zip([100, 200, 300, 400, 500, 600]))
    B.insert([(100, 11), (200, 22), (300, 33), (400, 44)])
    q1 = B & 'id < 300'
    q2 = B & 'id > 300'

    expected_data = [{'id': 0, 'id2': 5}, {'id': 1, 'id2': 6}, {'id': 2, 'id2': 7},
                     {'id': 3, 'id2': 8}, {'id': 4, 'id2': 9}, {'id': 100, 'id2': 11},
                     {'id': 200, 'id2': 22}, {'id': 400, 'id2': 44}]

    assert ((q1 + q2) * A).fetch(as_dict=True) == expected_data
