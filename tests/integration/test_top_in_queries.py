"""
dj.Top is honored when the limited expression is an operand of another query (#1656).

An expression restricted by dj.Top carries its ORDER BY/LIMIT in ``_top``. Joins (on either
side), aggr, dj.U.aggr, dj.U restriction, union, and restriction by a limited expression must
apply that limit to the operand itself, which takes a subquery. dj.Top cannot be negated; the
rows after the first n are ``dj.Top(None, order_by=..., offset=n)``.
"""

import time

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_top_{backend}_{test_id}"[:64]
    drop = f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass


@pytest.fixture(scope="function")
def tables(schema_by_backend):
    schema = schema_by_backend

    @schema
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        ---
        label : varchar(8)
        """

    @schema
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        ---
        color : varchar(8)
        """

    @schema
    class ParentInfo(dj.Manual):
        definition = """
        -> Parent
        ---
        info : varchar(8)
        """

    Parent.insert(
        [
            {"parent_id": 1, "label": "a"},
            {"parent_id": 2, "label": "b"},
            {"parent_id": 3, "label": "a"},
        ]
    )
    Child.insert(
        [
            {"parent_id": 1, "child_id": 1, "color": "red"},
            {"parent_id": 1, "child_id": 2, "color": "blue"},
            {"parent_id": 2, "child_id": 1, "color": "blue"},
        ]
    )
    ParentInfo.insert(
        [
            {"parent_id": 1, "info": "x"},
            {"parent_id": 2, "info": "y"},
            {"parent_id": 3, "info": "z"},
        ]
    )
    return Parent, Child, ParentInfo


# Limits used below, with the rows they select on their own.
def top_child():  # child (1, 2)
    return dj.Top(1, order_by="child_id DESC, parent_id")


def top2_child():  # children (1, 2), (1, 1)
    return dj.Top(2, order_by="child_id DESC, parent_id")


def rest_child():  # children (1, 1), (2, 1): all but the first
    return dj.Top(None, order_by="child_id DESC, parent_id", offset=1)


def top_parent():  # parent 1
    return dj.Top(1, order_by="parent_id")


def rest_parent():  # parents 2, 3
    return dj.Top(None, order_by="parent_id", offset=1)


def rows(query, *attributes):
    result = [tuple(row[a] for a in attributes) for row in query.to_dicts()]
    assert len(result) == len(set(result)), "duplicate rows"
    return set(result)


def counts(query, key, value="n"):
    result = {row[key]: row[value] for row in query.to_dicts()}
    return result


def test_limits_on_their_own(tables):
    Parent, Child, _ = tables
    assert rows(Child & top_child(), "parent_id", "child_id") == {(1, 2)}
    assert rows(Child & top2_child(), "parent_id", "child_id") == {(1, 2), (1, 1)}
    assert rows(Child & rest_child(), "parent_id", "child_id") == {(1, 1), (2, 1)}
    assert rows(Parent & top_parent(), "parent_id") == {(1,)}
    assert rows(Parent & rest_parent(), "parent_id") == {(2,), (3,)}


# --- joins: a limited operand on either side keeps its limit ---


@pytest.mark.parametrize(
    "make, expected",
    [
        (lambda P, C: P * (C & top_child()), {(1, 2)}),
        (lambda P, C: (P & top_parent()) * C, {(1, 1), (1, 2)}),
        (lambda P, C: (P & top_parent()) * (C & top_child()), {(1, 2)}),
        (lambda P, C: P * ((C & "color='blue'") & dj.Top(1, order_by="parent_id DESC")), {(2, 1)}),
        (lambda P, C: P * (C & rest_child()), {(1, 1), (2, 1)}),
        (lambda P, C: (P & rest_parent()) * C, {(2, 1)}),
        (lambda P, C: P * (C & top_child()).proj(), {(1, 2)}),
    ],
    ids=["right", "left", "both", "with-condition", "right-offset", "left-offset", "projected"],
)
def test_join_keeps_limit(tables, make, expected):
    Parent, Child, _ = tables
    assert rows(make(Parent, Child), "parent_id", "child_id") == expected


def test_join_without_limit_has_no_subquery(tables):
    Parent, Child, _ = tables
    query = Parent * (Child & "color='blue'")
    assert rows(query, "parent_id", "child_id") == {(1, 2), (2, 1)}
    assert "(SELECT" not in query.from_clause()


def test_extend_with_limited_left_operand(tables):
    Parent, Child, _ = tables
    query = (Child & top_child()).extend(Parent)
    assert rows(query, "parent_id", "child_id", "label") == {(1, 2, "a")}


# --- aggregation ---


def test_aggr_with_limited_grouping_expression(tables):
    Parent, Child, _ = tables
    assert counts((Parent & top_parent()).aggr(Child, n="count(child_id)"), "parent_id") == {1: 2}
    assert counts((Parent & rest_parent()).aggr(Child, n="count(child_id)"), "parent_id") == {2: 1, 3: 0}


@pytest.mark.parametrize(
    "limit, expected",
    [(top_child, {1: 1}), (rest_child, {1: 1, 2: 1})],
    ids=["top", "offset"],
)
def test_inner_aggr_with_limited_group(tables, limit, expected):
    Parent, Child, _ = tables
    query = Parent.aggr(Child & limit(), n="count(child_id)", exclude_nonmatching=True)
    assert counts(query, "parent_id") == expected


def test_left_join_with_limited_right_operand(tables):
    Parent, Child, _ = tables
    query = Child.extend(Parent & top_parent())
    assert rows(query, "parent_id", "child_id", "label") == {(1, 1, "a"), (1, 2, "a"), (2, 1, None)}
    query = Parent.aggr(Child & rest_child(), n="count(child_id)")
    assert counts(query, "parent_id") == {1: 1, 2: 1, 3: 0}


@pytest.mark.parametrize(
    "limit, expected",
    [(top_child, {"blue": 1}), (rest_child, {"red": 1, "blue": 1})],
    ids=["top", "offset"],
)
def test_universal_aggr_with_limited_group(tables, limit, expected):
    _, Child, _ = tables
    assert counts(dj.U("color").aggr(Child & limit(), n="count(*)"), "color") == expected


def test_universal_aggr_without_limit_has_no_subquery(tables):
    _, Child, _ = tables
    query = dj.U("color").aggr(Child, n="count(*)")
    assert counts(query, "color") == {"red": 1, "blue": 2}
    assert "(SELECT" not in query.from_clause()


# --- restriction by a limited expression (MySQL rejects LIMIT inside IN) ---


@pytest.mark.parametrize(
    "limit, matching, rest",
    [
        (top_child, {(1,)}, {(2,), (3,)}),
        (top2_child, {(1,)}, {(2,), (3,)}),
        (rest_child, {(1,), (2,)}, {(3,)}),
    ],
    ids=["top", "top2", "offset"],
)
def test_restriction_by_limited_expression(tables, limit, matching, rest):
    Parent, Child, _ = tables
    assert rows(Parent & (Child & limit()), "parent_id") == matching
    assert rows(Parent - (Child & limit()), "parent_id") == rest


def test_restriction_by_limit_on_computed_attribute(tables):
    """The limit applies before the projection to the common attributes."""
    Parent, Child, _ = tables
    # score: (1, 1) -> 11, (1, 2) -> 21, (2, 1) -> 12
    limited = Child.proj(score="child_id * 10 + parent_id") & dj.Top(1, order_by="score DESC")
    assert rows(limited, "parent_id", "child_id", "score") == {(1, 2, 21)}
    assert rows(Parent & limited, "parent_id") == {(1,)}
    assert rows(Parent - limited, "parent_id") == {(2,), (3,)}


def test_restriction_by_limited_union(tables):
    Parent, Child, ParentInfo = tables
    first = (Parent.proj() + ParentInfo.proj()) & dj.Top(1, order_by="parent_id")
    assert rows(Child & first, "parent_id", "child_id") == {(1, 1), (1, 2)}
    assert rows(Child - first, "parent_id", "child_id") == {(2, 1)}


def test_restriction_without_limit_has_no_derived_table(tables):
    Parent, Child, _ = tables
    query = Parent & (Child & "color='blue'")
    assert rows(query, "parent_id") == {(1,), (2,)}
    assert query.where_clause().count("SELECT") == 1


@pytest.mark.parametrize(
    "limit",
    [top2_child, lambda: dj.Top(2, order_by="parent_id")],
    ids=["order-by-dropped-attribute", "order-by-kept-attribute"],
)
def test_universal_restriction_by_limited_expression(tables, limit):
    """The limit applies to Child's rows before dj.U takes the distinct parent_id values."""
    _, Child, _ = tables
    assert rows(dj.U("parent_id") & (Child & limit()), "parent_id") == {(1,)}


def test_universal_restriction_without_limit_has_no_subquery(tables):
    _, Child, _ = tables
    query = dj.U("parent_id") & Child
    assert rows(query, "parent_id") == {(1,), (2,)}
    assert "(SELECT" not in query.from_clause()


# --- projection of a limited expression ---


def test_keys_of_limit_on_aggregate(tables):
    """The ORDER BY names a computed attribute that keys() drops."""
    Parent, Child, _ = tables
    limited = Parent.aggr(Child, n="count(child_id)") & dj.Top(1, order_by="n DESC")
    assert limited.keys() == [{"parent_id": 1}]
    assert rows(Child * limited.proj(), "parent_id", "child_id") == {(1, 1), (1, 2)}


def test_projection_drops_computed_ordering_attribute(tables):
    Parent, _, _ = tables
    limited = Parent.proj(neg="-parent_id") & dj.Top(1, order_by="neg")  # parent 3
    assert rows(limited.proj(), "parent_id") == {(3,)}
    assert len(limited.proj()) == 1


@pytest.mark.parametrize(
    "make, attributes, expected",
    [
        (
            lambda P, C: (P & dj.Top(1, order_by="label DESC, parent_id")).proj(label="-parent_id"),
            ("parent_id", "label"),
            {(2, -2)},
        ),
        (
            lambda P, C: (C & top_child()).proj(child_id="parent_id", parent_id="child_id"),
            ("parent_id", "child_id"),
            {(2, 1)},
        ),
    ],
    ids=["recomputed", "swapped"],
)
def test_projection_redefines_ordering_attribute(tables, make, attributes, expected):
    """A new attribute that takes an ORDER BY name must not change which rows the limit selects."""
    Parent, Child, _ = tables
    assert rows(make(Parent, Child), *attributes) == expected


def test_projection_dropping_table_column_keeps_order_in_place(tables):
    """ORDER BY on a table column that the projection drops needs no subquery."""
    _, Child, _ = tables
    query = (Child & dj.Top(1, order_by="color DESC")).proj()
    assert rows(query, "parent_id", "child_id") == {(1, 1)}
    assert "(SELECT" not in query.from_clause()


# --- merged limits, emptiness, diagrams ---


@pytest.mark.parametrize(
    "second",
    [
        lambda: dj.Top(None, order_by="parent_id", offset=1),
        lambda: dj.Top(5, order_by=None, offset=1),
    ],
    ids=["offset", "offset-inherit"],
)
def test_second_limit_stays_within_first(tables, second):
    Parent, _, _ = tables
    first_two = Parent & dj.Top(2, order_by="parent_id")  # parents 1, 2
    assert rows(first_two & second(), "parent_id") == {(2,)}
    assert [row["parent_id"] for row in first_two.to_dicts(limit=5, offset=1)] == [2]


def test_bool_of_limited_expression(tables):
    Parent, _, _ = tables
    assert not (Parent & dj.Top(None, order_by="parent_id", offset=3))
    assert not (Parent & dj.Top(0))
    assert Parent & rest_parent()


def test_diagram_restrict_by_limited_expression(tables, schema_by_backend):
    Parent, Child, ParentInfo = tables
    restricted = dj.Diagram(schema_by_backend).restrict(Parent & top_parent())
    expected = {Parent.full_table_name: 1, Child.full_table_name: 2, ParentInfo.full_table_name: 1}
    assert restricted.counts() == expected


# --- union ---


def test_union_restricted_by_top(tables):
    Parent, _, ParentInfo = tables
    keys_only = (Parent.proj() + ParentInfo.proj()) & dj.Top(1, order_by="parent_id DESC")
    assert rows(keys_only, "parent_id") == {(3,)}
    with_attributes = (Parent + ParentInfo) & dj.Top(1, order_by="parent_id DESC")
    assert rows(with_attributes, "parent_id", "label", "info") == {(3, "a", "z")}


def test_union_of_limited_operands(tables):
    Parent, _, ParentInfo = tables
    last_info = ParentInfo & dj.Top(1, order_by="parent_id DESC")
    keys_only = (Parent & top_parent()).proj() + last_info.proj()
    assert rows(keys_only, "parent_id") == {(1,), (3,)}
    with_attributes = (Parent & top_parent()) + last_info
    assert rows(with_attributes, "parent_id", "label", "info") == {(1, "a", None), (3, None, "z")}


# --- dj.Top cannot be negated; the error names the offset form ---


@pytest.mark.parametrize(
    "make",
    [
        lambda P: P - top_parent(),
        lambda P: P & dj.Not(top_parent()),
        lambda P: P & [top_parent()],
        lambda P: P - [top_parent()],
        lambda P: P & dj.AndList([top_parent(), "parent_id > 1"]),
    ],
    ids=["minus", "not", "list", "minus-list", "andlist"],
)
def test_negated_or_nested_top_is_rejected(tables, make):
    Parent, _, _ = tables
    with pytest.raises(DataJointError, match="offset=n"):
        make(Parent)
