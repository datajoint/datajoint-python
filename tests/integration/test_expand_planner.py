"""
Integration tests for ``Diagram.expand`` on MySQL and PostgreSQL.

A handful of shapes from the Traversal Shape Catalog (``traversal-shapes.md`` in
datajoint-docs), declared through DataJoint: the rows ``Diagram.expand`` selects
in each table equal the rows the catalog states. The full catalog, the
differential tests on random schemas, and the delete tests come later.
"""

import time

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """A fresh schema for each test."""
    backend = db_creds_by_backend["backend"]
    name = f"djtest_expand_{backend}_{str(int(time.time() * 1000))[-8:]}"[:64]
    drop = f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(name)}"
    connection_by_backend.query(drop)
    yield dj.Schema(name, connection=connection_by_backend)
    if connection_by_backend.is_connected:
        connection_by_backend.query(drop)


def keys(expr) -> set:
    """The primary keys of an expression's rows, as tuples."""
    pk = expr.primary_key
    return {tuple(row[a] for a in pk) for row in expr.keys()}


def check(diagram, expected: dict) -> None:
    """The diagram's nodes are the tables of ``expected``, and each holds the rows listed."""
    names = {table.full_table_name: table for table in expected}
    assert diagram.nodes_to_show == set(names)
    for table, rows in expected.items():
        assert keys(diagram[table]) == {r if isinstance(r, tuple) else (r,) for r in rows}, table.__name__
    assert diagram.counts() == {table.full_table_name: len(rows) for table, rows in expected.items()}


def test_S1_identity_fk(schema_by_backend):
    """S1: the condition is copied to the child; upward the parent is restricted."""

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
        """

    Subject.insert([(1,), (2,)])
    Session.insert([(1, 1), (1, 2), (2, 1)])

    down = dj.Diagram.expand(Subject & {"subject_id": 1}, "down")
    check(down, {Subject: [1], Session: [(1, 1), (1, 2)]})
    assert "IN (" not in down[Session].where_clause()  # a copy, not a subquery
    check(dj.Diagram.expand(Session & {"session_id": 1}, "up"), {Session: [(1, 1), (2, 1)], Subject: [1, 2]})
    # Iteration is in topological order, parents first; reversed, leaves first.
    assert [t.full_table_name for t in down] == [Subject.full_table_name, Session.full_table_name]
    assert [t.full_table_name for t in reversed(down)] == [Session.full_table_name, Subject.full_table_name]
    # A table can be named by its class name or its full name.
    assert keys(down["Session"]) == keys(down[Session.full_table_name]) == {(1, 1), (1, 2)}


def test_S3_nullable_fk_sharing_a_primary_key_column(schema_by_backend):
    """S3: a foreign key with a NULL column references nothing (A2), in both directions."""

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

    Subject.insert([(1,), (2,)])
    Surgery.insert([(1, 1), (1, 2), (2, 1)])
    Session.insert([(1, 1, 1), (1, 2, None), (1, 3, 2), (2, 1, 1)])

    check(
        dj.Diagram.expand(Surgery & {"subject_id": 1}, "down"),
        {Surgery: [(1, 1), (1, 2)], Session: [(1, 1), (1, 3)]},
    )
    check(
        dj.Diagram.expand(Session & {"subject_id": 1, "session_id": 2}, "up"),
        {Session: [(1, 2)], Subject: [1], Surgery: []},
    )


def test_S7_diamond_reached_through_a_secondary_fk(schema_by_backend):
    """S7: Session (2, 1) reaches the seed only through its rig."""

    @schema_by_backend
    class Lab(dj.Manual):
        definition = """
        lab_id : int32
        """

    @schema_by_backend
    class Rig(dj.Manual):
        definition = """
        rig_id : int32
        ---
        -> Lab
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Lab
        session_id : int32
        ---
        -> Rig
        """

    Lab.insert([(1,), (2,)])
    Rig.insert([(10, 1), (20, 2)])
    Session.insert([(1, 1, 20), (2, 1, 10), (2, 2, 20)])

    check(dj.Diagram.expand(Lab & {"lab_id": 1}, "down"), {Lab: [1], Rig: [10], Session: [(1, 1), (2, 1)]})
    check(
        dj.Diagram.expand(Session & {"lab_id": 2, "session_id": 1}, "up"),
        {Session: [(2, 1)], Rig: [10], Lab: [1, 2]},
    )
    check(dj.Diagram.expand(Rig & {"rig_id": 10}, "both"), {Rig: [10], Session: [(2, 1)], Lab: [1, 2]})


def test_G1_part_to_master_lift(schema_by_backend):
    """G1: the lift is existential; upward, the instance is read whole."""

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        session_id : int32
        """

        class Trial(dj.Part):
            definition = """
            -> master
            trial_id : int32
            """

    @schema_by_backend
    class Review(dj.Manual):
        definition = """
        -> Session
        review_id : int32
        """

    Session.insert([(1,), (2,), (3,)])
    Session.Trial.insert([(1, 1), (1, 2), (2, 1), (3, 1)])
    Review.insert([(1, 1), (2, 1)])

    seed = Session.Trial & {"trial_id": 2}
    down = dj.Diagram.expand(seed, "down")
    check(down, {Session.Trial: [(1, 1), (1, 2)], Session: [1], Review: [(1, 1)]})
    assert keys(down["Session.Trial"]) == {(1, 1), (1, 2)}
    check(dj.Diagram.expand(seed, "up"), {Session.Trial: [(1, 1), (1, 2)], Session: [1]})


def test_G4_group_step_iterates(schema_by_backend):
    """G4: a renamed sibling FK links instances; the master keys grow {1}, {1, 2}, {1, 2, 3}."""

    @schema_by_backend
    class Run(dj.Manual):
        definition = """
        run_id : int32
        """

        class Step(dj.Part):
            definition = """
            -> master
            step_id : int32
            """

        class Carry(dj.Part):
            definition = """
            -> master
            carry_id : int32
            ---
            -> Run.Step.proj(prev_run="run_id", prev_step="step_id")
            """

    Run.insert([(1,), (2,), (3,), (4,)])
    Run.Step.insert([(1, 1), (2, 1), (3, 1), (4, 1)])
    Run.Carry.insert([(2, 1, 1, 1), (3, 1, 2, 1)])

    down = dj.Diagram.expand(Run & {"run_id": 1}, "down")
    check(down, {Run: [1, 2, 3], Run.Step: [(1, 1), (2, 1), (3, 1)], Run.Carry: [(2, 1), (3, 1)]})
    assert [g.iterate for g in down._plan.groups] == [True]
    check(
        dj.Diagram.expand(Run & {"run_id": 2}, "down"),
        {Run: [2, 3], Run.Step: [(2, 1), (3, 1)], Run.Carry: [(2, 1), (3, 1)]},
    )
    check(
        dj.Diagram.expand(Run & {"run_id": 2}, "up"),
        {Run: [1, 2], Run.Step: [(1, 1), (2, 1)], Run.Carry: [(2, 1)]},
    )
    check(
        dj.Diagram.expand(Run & {"run_id": 2}, "both"),
        {Run: [1, 2, 3], Run.Step: [(1, 1), (2, 1), (3, 1)], Run.Carry: [(2, 1), (3, 1)]},
    )


def test_G7_owned_part_of_part_with_renamed_attributes(schema_by_backend):
    """G7: the lift renames the owning attribute src_master onto the master's key."""

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

        class PartA(dj.Part):
            definition = """
            -> master
            part_a_id : int32
            """

        class PartB(dj.Part):
            definition = """
            -> Master.PartA.proj(src_master="master_id", src_part="part_a_id")
            part_b_id : int32
            """

    @schema_by_backend
    class Report(dj.Manual):
        definition = """
        -> Master
        report_id : int32
        """

    Master.insert([(1,), (2,), (3,)])
    Master.PartA.insert([(1, 10), (1, 11), (2, 20)])
    Master.PartB.insert([(1, 10, 100), (1, 11, 110), (2, 20, 200)])
    Report.insert([(1, 1), (2, 1)])

    seed = Master.PartB & {"part_b_id": 100}
    instance = {Master.PartB: [(1, 10, 100), (1, 11, 110)], Master: [1], Master.PartA: [(1, 10), (1, 11)]}
    check(dj.Diagram.expand(seed, "down"), {**instance, Report: [(1, 1)]})
    check(dj.Diagram.expand(seed, "up"), instance)


@pytest.fixture
def k_tables(schema_by_backend):
    """The schema shared by the seed shapes K1 to K6."""

    @schema_by_backend
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        ---
        species : varchar(16)
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        quality : int32
        """

    @schema_by_backend
    class Trial(dj.Manual):
        definition = """
        -> Session
        trial_id : int32
        """

    @schema_by_backend
    class Flag(dj.Manual):
        definition = """
        flag_id : int32
        ---
        -> [nullable] Subject
        """

    Subject.insert([(1, "mouse"), (2, "rat"), (3, "mouse"), (4, "rat")])
    Session.insert([(1, 1, 0), (1, 2, 1), (2, 1, 1), (2, 2, 0), (3, 1, 1)])
    Trial.insert([(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (3, 1, 1)])
    Flag.insert([(1, 1), (2, 3)])
    return Subject, Session, Trial, Flag


def test_K1_seed_restricted_by_a_descendant(k_tables):
    """K1: down takes all sessions of the seed subjects, not only those of quality 0."""
    Subject, Session, Trial, Flag = k_tables
    seed = Subject & (Session & "quality = 0")
    check(
        dj.Diagram.expand(seed, "down"),
        {
            Subject: [1, 2],
            Session: [(1, 1), (1, 2), (2, 1), (2, 2)],
            Trial: [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)],
            Flag: [1],
        },
    )
    check(dj.Diagram.expand(seed, "up"), {Subject: [1, 2]})


def test_K3_seed_kinds_give_the_same_rows(k_tables):
    """K3: a dict, a list of dicts, a string, a query expression, an AndList and a Not select the same rows."""
    Subject, Session, Trial, Flag = k_tables
    for seed in (
        Session & {"subject_id": 2},
        Session & [{"subject_id": 2, "session_id": 1}, {"subject_id": 2, "session_id": 2}],
        Session & "subject_id = 2",
        Session & (Subject & {"species": "rat"}),
        Session & dj.AndList([{"subject_id": 2}, "quality >= 0"]),
        Session - Flag,
    ):
        check(dj.Diagram.expand(seed, "down"), {Session: [(2, 1), (2, 2)], Trial: [(2, 1, 1), (2, 2, 1)]})
        check(dj.Diagram.expand(seed, "up"), {Session: [(2, 1), (2, 2)], Subject: [2]})


def test_K4_top_seed(k_tables):
    """K4: a dj.Top seed is replaced by the keys it selects."""
    Subject, Session, Trial, Flag = k_tables
    seed = Session & "quality = 1" & dj.Top(2)
    check(dj.Diagram.expand(seed, "down"), {Session: [(1, 2), (2, 1)], Trial: [(1, 2, 1), (2, 1, 1)]})
    check(dj.Diagram.expand(seed, "up"), {Session: [(1, 2), (2, 1)], Subject: [1, 2]})


def test_K5_empty_seed_and_unknown_node(k_tables):
    """K5: an empty seed gives nodes with zero rows; indexing a table outside the diagram raises."""
    Subject, Session, Trial, Flag = k_tables
    down = dj.Diagram.expand(Session & {"subject_id": 4}, "down")
    check(down, {Session: [], Trial: []})
    with pytest.raises(DataJointError, match="not a node"):
        down[Flag]


def test_K6_unrestricted_seed(k_tables):
    """K6: an unrestricted seed selects every row; upward, subject 4 is not reached."""
    Subject, Session, Trial, Flag = k_tables
    sessions = [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1)]
    trials = [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (3, 1, 1)]
    for seed in (Session, Session()):
        check(dj.Diagram.expand(seed, "down"), {Session: sessions, Trial: trials})
        check(dj.Diagram.expand(seed, "up"), {Session: sessions, Subject: [1, 2, 3]})


def test_refused_seeds(k_tables):
    """A projection, a join, an aggregation and a union are refused as seeds; the direction is checked."""
    Subject, Session, Trial, Flag = k_tables
    for seed, kind in (
        (Session.proj(), "projection"),
        (Session * Trial, "join"),
        (Subject.aggr(Session, n="count(*)"), "aggregation"),
        ((Session & "quality = 0").proj() + (Session & "quality = 1").proj(), "union"),
        (dj.U("subject_id") & Session, "projection"),
    ):
        with pytest.raises(DataJointError, match=f"(?i){kind}"):
            dj.Diagram.expand(seed, "down")
    with pytest.raises(ValueError, match="direction"):
        dj.Diagram.expand(Session, "sideways")
