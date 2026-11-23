"""
Left joins keep every row of the left operand when the right operand is restricted (#1650).

aggr (with the default exclude_nonmatching=False), extend, and union of operands with secondary
attributes use a left join. A restriction on the right operand, including dj.Top, must be
applied before the join, in a subquery. In the outer WHERE it removes the left rows that have
no match. An unrestricted right operand is joined without a subquery, and inner joins keep
their restrictions in the WHERE clause.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_aggr_left_{backend}_{test_id}"[:64]
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
    class Tag(dj.Manual):
        definition = """
        -> Child
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
    Tag.insert([{"parent_id": 1, "child_id": 1}])
    ParentInfo.insert(
        [
            {"parent_id": 1, "info": "x"},
            {"parent_id": 2, "info": "y"},
            {"parent_id": 3, "info": "z"},
        ]
    )
    return Parent, Child, Tag, ParentInfo


def counts(expression):
    rows = expression.to_dicts()
    result = {row["parent_id"]: row["n"] for row in rows}
    assert len(result) == len(rows), "duplicate rows"
    return result


def right_operand_in_subquery(query):
    return "JOIN (SELECT" in query.from_clause()


# --- aggr: the restricted group keeps every row of the grouping expression ---


def test_unrestricted_group_keeps_all_rows_without_subquery(tables):
    Parent, Child, _, _ = tables
    query = Parent.aggr(Child, n="count(child_id)")
    assert counts(query) == {1: 2, 2: 1, 3: 0}
    assert not right_operand_in_subquery(query)


def test_group_restricted_by_string_keeps_all_rows(tables):
    """The reproduction from #1650."""
    Parent, Child, _, _ = tables
    query = Parent.aggr(Child & "color='red'", n="count(child_id)")
    assert counts(query) == {1: 1, 2: 0, 3: 0}
    assert len(query) == 3
    assert right_operand_in_subquery(query)


@pytest.mark.parametrize(
    "restriction, expected",
    [
        ({"color": "blue"}, {1: 1, 2: 1, 3: 0}),
        ({"parent_id": 1, "color": "blue"}, {1: 1, 2: 0, 3: 0}),
        ([{"color": "red"}, {"parent_id": 2}], {1: 1, 2: 1, 3: 0}),
        ("parent_id > 1", {1: 0, 2: 1, 3: 0}),
    ],
    ids=["mapping", "mapping-and", "list-or", "join-attribute"],
)
def test_group_restricted_by_condition_keeps_all_rows(tables, restriction, expected):
    Parent, Child, _, _ = tables
    assert counts(Parent.aggr(Child & restriction, n="count(child_id)")) == expected


def test_group_restricted_by_table_keeps_all_rows(tables):
    Parent, Child, Tag, _ = tables
    assert counts(Parent.aggr(Child & Tag, n="count(child_id)")) == {1: 1, 2: 0, 3: 0}


def test_group_restricted_by_negation_keeps_all_rows(tables):
    Parent, Child, Tag, _ = tables
    Tag.insert1({"parent_id": 2, "child_id": 1})  # every child of parent 2 is tagged
    assert counts(Parent.aggr(Child - Tag, n="count(child_id)")) == {1: 1, 2: 0, 3: 0}
    assert counts(Parent.aggr(Child - "color='red'", n="count(child_id)")) == {1: 1, 2: 1, 3: 0}


def test_group_restricted_by_top_keeps_all_rows(tables):
    """dj.Top is applied with & like any restriction; its limit applies to the group."""
    Parent, Child, _, _ = tables
    query = Parent.aggr(Child & dj.Top(1, order_by="color DESC"), n="count(child_id)")
    assert counts(query) == {1: 1, 2: 0, 3: 0}
    assert right_operand_in_subquery(query)


def test_restricted_and_projected_group_keeps_all_rows(tables):
    Parent, Child, _, _ = tables
    assert counts(Parent.aggr((Child & "color='red'").proj(), n="count(child_id)")) == {1: 1, 2: 0, 3: 0}


def test_restricted_join_as_group_keeps_all_rows(tables):
    """Aggregates other than count are NULL for the rows with no match."""
    Parent, Child, Tag, _ = tables
    query = Parent.aggr((Child * Tag) & "color='red'", n="count(child_id)", top="max(child_id)")
    rows = {row["parent_id"]: (row["n"], row["top"]) for row in query.to_dicts()}
    assert rows == {1: (1, 1), 2: (0, None), 3: (0, None)}


def test_restriction_on_grouping_side_stays_outside(tables):
    """A restriction on the left operand still filters the result."""
    Parent, Child, _, _ = tables
    query = (Parent & "parent_id > 1").aggr(Child & "color='red'", n="count(child_id)")
    assert counts(query) == {2: 0, 3: 0}


def test_restricting_the_aggregation_result(tables):
    """Restrictions on computed attributes (HAVING, or the PostgreSQL subquery) see the zero counts."""
    Parent, Child, _, _ = tables
    query = Parent.aggr(Child & "color='red'", n="count(child_id)") & "n = 0"
    assert set(counts(query)) == {2, 3}


# --- inner joins are unchanged: the restriction stays in WHERE, without a subquery ---


def test_inner_aggregation_keeps_restriction_in_where(tables):
    Parent, Child, _, _ = tables
    query = Parent.aggr(Child & "color='red'", n="count(child_id)", exclude_nonmatching=True)
    assert counts(query) == {1: 1}
    assert not right_operand_in_subquery(query)
    assert "red" in query.where_clause()


def test_universal_set_aggregation_unchanged(tables):
    """dj.U aggregation always excludes non-matching rows (inner join)."""
    _, Child, _, _ = tables
    query = dj.U("color").aggr(Child & "parent_id = 1", n="count(*)")
    assert {row["color"]: row["n"] for row in query.to_dicts()} == {"red": 1, "blue": 1}
    assert "(SELECT" not in query.from_clause()


def test_inner_join_with_restricted_operand_has_no_subquery(tables):
    Parent, Child, _, _ = tables
    query = Parent * (Child & "color='red'")
    assert len(query) == 1
    assert not right_operand_in_subquery(query)
    assert "red" in query.where_clause()


# --- extend and union use the same left join ---


def test_extend_with_restricted_operand_keeps_all_rows(tables):
    Parent, Child, _, _ = tables
    query = Child.extend(Parent & "label='a'")
    rows = {(row["parent_id"], row["child_id"]): row["label"] for row in query.to_dicts()}
    assert rows == {(1, 1): "a", (1, 2): "a", (2, 1): None}
    assert len(query) == 3
    assert right_operand_in_subquery(query)


def test_extend_with_unrestricted_operand_has_no_subquery(tables):
    Parent, Child, _, _ = tables
    query = Child.extend(Parent)
    assert len(query) == 3
    assert not right_operand_in_subquery(query)


def test_union_with_restricted_operand_keeps_all_rows(tables):
    """Union of operands with secondary attributes left-joins the second operand."""
    Parent, _, _, ParentInfo = tables
    query = Parent + (ParentInfo & "info in ('x', 'z')")
    rows = {row["parent_id"]: (row["label"], row["info"]) for row in query.to_dicts()}
    assert rows == {1: ("a", "x"), 2: ("b", None), 3: ("a", "z")}
