"""
Regression tests for cascade restrictions that reached rows they should not.

A restriction may be copied verbatim across a foreign key only when it names
attributes the foreign key carries and the foreign key lies in the child's
primary key. The part-to-master lift never copies: a master joins the cascade
only if one of its part rows did. A ``Top`` seed restricts the seed to the keys
it selects.
"""

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a fresh schema per test (mirrors test_cascade_integrity.py)."""
    backend = db_creds_by_backend["backend"]
    import time

    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_overdelete_{backend}_{test_id}"[:64]

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


def test_top_seed_deletes_only_the_top_rows(schema_by_backend):
    """``(T & dj.Top(n)).delete()`` and ``delete_quick()`` delete n rows, not
    every row of T."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Item
        child_id : int32
        """

    Item.insert([(i,) for i in range(5)])
    Child.insert([(i, 0) for i in range(5)])

    seed = Item & dj.Top(2, order_by="item_id")
    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Item.full_table_name] == 2
    assert counts[Child.full_table_name] == 2

    seed.delete(prompt=False)
    assert sorted(Item.to_arrays("item_id")) == [2, 3, 4]
    assert sorted(Child.to_arrays("item_id")) == [2, 3, 4]

    (Child & dj.Top(1, order_by="item_id")).delete_quick()
    assert sorted(Child.to_arrays("item_id")) == [3, 4]


def test_nullable_fk_sharing_a_primary_key_column(schema_by_backend):
    """A child whose nullable foreign key shares a primary-key column with the
    parent keeps the rows whose foreign key is NULL: they reference nothing."""

    @schema_by_backend
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    @schema_by_backend
    class Surgery(dj.Manual):
        definition = """
        -> Subject
        surgery_id : int32
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        -> [nullable] Surgery
        """

    Subject.insert([(1,)])
    Surgery.insert([(1, 1)])
    Session.insert(
        [
            {"subject_id": 1, "session_id": 1, "surgery_id": 1},
            {"subject_id": 1, "session_id": 2, "surgery_id": None},
        ]
    )

    seed = Surgery & {"subject_id": 1}
    assert dj.Diagram.cascade(seed).counts()[Session.full_table_name] == 1

    seed.delete(prompt=False)
    assert len(Surgery()) == 0
    assert Session.to_dicts() == [{"subject_id": 1, "session_id": 2, "surgery_id": None}]


def test_diamond_child_is_reached_through_its_secondary_fk(schema_by_backend):
    """D references B directly (primary) and C (secondary). Deleting B=1 must
    delete the D rows that reference the deleted C rows, whatever D's own b_id."""

    @schema_by_backend
    class B(dj.Manual):
        definition = """
        b_id : int32
        """

    @schema_by_backend
    class C(dj.Manual):
        definition = """
        c_id : int32
        ---
        -> B
        """

    @schema_by_backend
    class D(dj.Manual):
        definition = """
        -> B
        d_id : int32
        ---
        -> C
        """

    B.insert([(1,), (2,)])
    C.insert([{"c_id": 10, "b_id": 1}])
    D.insert([{"b_id": 2, "d_id": 1, "c_id": 10}])

    seed = B & {"b_id": 1}
    assert dj.Diagram.cascade(seed).counts()[D.full_table_name] == 1

    seed.delete(prompt=False)
    assert B.to_arrays("b_id").tolist() == [2]
    assert len(C()) == 0
    assert len(D()) == 0


def test_part_cascade_lifts_only_masters_that_have_a_matching_part(schema_by_backend):
    """With part_integrity="cascade", a part seed lifts to the masters of the
    matching part rows. A master that has no matching part row is untouched."""

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

        class P(dj.Part):
            definition = """
            -> master
            p_id : int32
            """

    @schema_by_backend
    class Downstream(dj.Manual):
        definition = """
        -> Master
        d_id : int32
        """

    Master.insert([(1,), (2,)])
    Master.P.insert([(1, 1)])  # master 2 has no parts
    Downstream.insert([(1, 1), (2, 1)])

    seed = Master.P & [{"master_id": 1}, {"master_id": 2}]
    assert dj.Diagram.cascade(seed, part_integrity="cascade").counts()[Master.full_table_name] == 1

    seed.delete(prompt=False, part_integrity="cascade")
    assert Master.to_arrays("master_id").tolist() == [2]
    assert len(Master.P()) == 0
    assert Downstream.to_arrays("master_id").tolist() == [2]


SUBQUERY_SEEDS = {
    # seed -> (subjects deleted, sessions deleted)
    "restricted by a query": (lambda S, T: S & (T & "quality = 0"), {1, 3}, {(1, 1), (1, 2), (3, 1)}),
    "anti-restricted by a query": (lambda S, T: S - (T & "quality = 0"), {2, 4}, {(2, 1)}),
    "restricted by a table": (lambda S, T: S & T, {1, 2, 3}, {(1, 1), (1, 2), (2, 1), (3, 1)}),
}


@pytest.mark.parametrize("seed_kind", list(SUBQUERY_SEEDS))
def test_seed_restricted_by_a_downstream_query(schema_by_backend, seed_kind):
    """A seed restricted by a query on a downstream table deletes the rows that
    query selected when the delete started, with all their descendants."""

    @schema_by_backend
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        quality : int32
        """

    Subject.insert([(1,), (2,), (3,), (4,)])
    Session.insert([(1, 1, 1), (1, 2, 0), (2, 1, 1), (3, 1, 0)])
    all_sessions = {(1, 1), (1, 2), (2, 1), (3, 1)}

    make_seed, subjects, sessions = SUBQUERY_SEEDS[seed_kind]
    seed = make_seed(Subject, Session)

    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Subject.full_table_name] == len(subjects)
    assert counts[Session.full_table_name] == len(sessions)

    seed.delete(prompt=False)
    assert set(Subject.to_arrays("subject_id").tolist()) == {1, 2, 3, 4} - subjects
    assert set(zip(*Session.to_arrays("subject_id", "session_id"))) == all_sessions - sessions


def test_seed_condition_with_a_percent_sign(schema_by_backend):
    """A seed condition containing % selects the same rows in the cascade as
    on its own."""

    @schema_by_backend
    class Label(dj.Manual):
        definition = """
        label_id : int32
        ---
        name : varchar(16)
        """

    @schema_by_backend
    class Use(dj.Manual):
        definition = """
        -> Label
        use_id : int32
        """

    Label.insert([(1, "50%"), (2, "other"), (3, "5%%")])
    Use.insert([(1, 1), (2, 1), (3, 1)])

    for seed, label in [
        (Label & "name = '50%'", 1),
        (Label & {"name": "50%"}, 1),
        (Label & "name = '5%%'", 3),
    ]:
        assert len(seed) == 1
        assert dj.Diagram.cascade(seed).counts()[Use.full_table_name] == 1

    (Label & "name LIKE '50%'").delete(prompt=False)
    assert sorted(Label.to_arrays("label_id").tolist()) == [2, 3]
    assert sorted(Use.to_arrays("label_id").tolist()) == [2, 3]
