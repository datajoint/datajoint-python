"""
Integration tests for ``Diagram.trace()`` — upstream restriction propagation.

The upstream mirror of ``Diagram.cascade()``. Walks the FK graph from a
restricted seed to every ancestor with OR convergence. Reuses the upward
propagation rules (U1/U2/U3 in cascade.md) added by #1468.
"""

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a fresh schema for each trace test."""
    backend = db_creds_by_backend["backend"]
    import time

    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_trace_{backend}_{test_id}"[:64]

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


def test_trace_single_hop(schema_by_backend):
    """trace(Child & key)[Parent] returns Parent restricted via the FK."""

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        ---
        name : varchar(64)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        """

    Parent.insert([(1, "alice"), (2, "bob")])
    Child.insert([(1, 10), (1, 11), (2, 20)])

    trace = dj.Diagram.trace(Child & {"parent_id": 1, "child_id": 10})

    # Seed itself
    assert len(trace[Child]) == 1

    # Ancestor: Parent restricted to the rows that contributed to the seed
    assert len(trace[Parent]) == 1
    assert trace[Parent].fetch1("parent_id") == 1


def test_trace_multi_hop(schema_by_backend):
    """trace walks through intermediate ancestors (Grandparent ← Parent ← Child)."""

    @schema_by_backend
    class Grandparent(dj.Manual):
        definition = """
        gp_id : int32
        """

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        -> Grandparent
        parent_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        """

    Grandparent.insert([(1,), (2,)])
    Parent.insert([(1, 10), (1, 11), (2, 20)])
    Child.insert([(1, 10, 100), (1, 11, 110), (2, 20, 200)])

    trace = dj.Diagram.trace(Child & {"gp_id": 1, "parent_id": 10, "child_id": 100})

    # All three ancestors restricted to the one contributing tuple per level
    assert len(trace[Child]) == 1
    assert len(trace[Parent]) == 1
    assert len(trace[Grandparent]) == 1
    assert trace[Grandparent].fetch1("gp_id") == 1


def test_trace_renamed_fk(schema_by_backend):
    """Renamed FK (.proj(...)) — the upward rule reverses the rename."""

    @schema_by_backend
    class Animal(dj.Manual):
        definition = """
        animal_id : int32
        ---
        species : varchar(64)
        """

    @schema_by_backend
    class Observation(dj.Manual):
        definition = """
        obs_id : int32
        ---
        -> Animal.proj(subject_id='animal_id')
        measurement : float64
        """

    Animal.insert([(1, "Mouse"), (2, "Rat")])
    Observation.insert([(10, 1, 1.5), (11, 1, 2.5), (20, 2, 3.0)])

    # Observation columns: obs_id, subject_id (renamed), measurement.
    # No `animal_id` column on Observation — the upward walk must reverse the rename.
    trace = dj.Diagram.trace(Observation & {"obs_id": 10})

    assert len(trace[Animal]) == 1
    assert trace[Animal].fetch1("animal_id") == 1
    assert trace[Animal].fetch1("species") == "Mouse"


def test_trace_or_convergence_two_paths(schema_by_backend):
    """Two FK paths from child to the same ancestor → OR (union) at the ancestor."""

    @schema_by_backend
    class Source(dj.Manual):
        definition = """
        source_id : int32
        """

    @schema_by_backend
    class Downstream(dj.Manual):
        definition = """
        downstream_id : int32
        ---
        -> Source
        -> Source.proj(comparison_src='source_id')
        """

    Source.insert([(1,), (2,), (3,)])
    # Downstream rows reference Source via two columns; OR convergence means the
    # ancestor is restricted to the UNION of contributors across both FK paths.
    Downstream.insert(
        [
            (100, 1, 2),  # primary source=1, comparison_src=2
            (101, 3, 3),  # primary source=3, comparison_src=3
        ]
    )

    trace = dj.Diagram.trace(Downstream & {"downstream_id": 100})

    # Source is restricted via BOTH FK paths from row 100 → {1, 2}
    contributing = set(trace[Source].fetch("source_id"))
    assert contributing == {1, 2}


def test_trace_rejects_non_ancestor(schema_by_backend):
    """Indexing into a table that isn't in the trace's subgraph raises."""

    @schema_by_backend
    class A(dj.Manual):
        definition = """
        a_id : int32
        """

    @schema_by_backend
    class B(dj.Manual):
        definition = """
        b_id : int32
        """

    @schema_by_backend
    class C(dj.Manual):
        definition = """
        -> A
        c_id : int32
        """

    A.insert([(1,)])
    B.insert([(99,)])
    C.insert([(1, 10)])

    trace = dj.Diagram.trace(C & {"a_id": 1, "c_id": 10})

    # A is an ancestor — OK
    assert len(trace[A]) == 1

    # B is unrelated — should raise
    with pytest.raises(DataJointError, match="not in this trace"):
        trace[B]


def test_trace_string_indexing_returns_freetable(schema_by_backend):
    """trace[str] returns a FreeTable (no class needed in caller scope)."""
    from datajoint.table import FreeTable

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        """

    Parent.insert([(1,), (2,)])
    Child.insert([(1, 10), (2, 20)])

    trace = dj.Diagram.trace(Child & {"parent_id": 1, "child_id": 10})

    # String accepts the SQL-quoted full name
    parent_via_string = trace[Parent.full_table_name]
    assert isinstance(parent_via_string, FreeTable)
    assert len(parent_via_string) == 1


def test_trace_counts(schema_by_backend):
    """trace.counts() reports per-ancestor row counts under the seed's restriction."""

    @schema_by_backend
    class Grandparent(dj.Manual):
        definition = """
        gp_id : int32
        """

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        -> Grandparent
        parent_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        """

    Grandparent.insert([(1,), (2,)])
    Parent.insert([(1, 10), (1, 11), (2, 20)])
    Child.insert([(1, 10, 100), (1, 11, 110), (2, 20, 200)])

    trace = dj.Diagram.trace(Child & {"gp_id": 1})
    counts = trace.counts()

    assert counts[Grandparent.full_table_name] == 1
    assert counts[Parent.full_table_name] == 2
    assert counts[Child.full_table_name] == 2


def test_trace_multi_hop_diamond_or_convergence(schema_by_backend):
    """Diamond: an ancestor reached via two MULTI-HOP paths → OR-union across
    both arms. Unlike test_trace_or_convergence_two_paths (adjacent two-edge
    case), this forces the reverse-topo walk to accumulate a contributor for the
    same ancestor across separate multi-pass arms. A regression that dropped an
    OR arm would yield a subset here."""

    @schema_by_backend
    class Root(dj.Manual):
        definition = """
        root_id : int32
        """

    @schema_by_backend
    class Left(dj.Manual):
        definition = """
        -> Root
        left_id : int32
        """

    @schema_by_backend
    class Right(dj.Manual):
        # renamed FK avoids the root_id name collision when Leaf reconverges
        definition = """
        -> Root.proj(root_id2='root_id')
        right_id : int32
        """

    @schema_by_backend
    class Leaf(dj.Manual):
        definition = """
        -> Left
        -> Right
        leaf_id : int32
        """

    Root.insert([(1,), (2,), (3,)])
    Left.insert([(1, 10)])  # Left row → Root 1
    Right.insert([(2, 20)])  # Right row (root_id2=2) → Root 2
    # Leaf PK order: root_id, left_id, root_id2, right_id, leaf_id
    Leaf.insert([(1, 10, 2, 20, 100)])

    trace = dj.Diagram.trace(Leaf & {"leaf_id": 100})

    # Root reached via Leaf→Left→Root (root_id=1) OR Leaf→Right→Root
    # (root_id2=2 reversed to root_id=2). Union = {1, 2}; Root 3 excluded.
    contributing = set(trace[Root].fetch("root_id"))
    assert contributing == {1, 2}


def test_trace_cross_schema_ancestor(schema_by_backend, connection_by_backend):
    """Ancestor in a DIFFERENT schema than the seed → load_all_upstream must
    discover the unloaded ancestor schema via reverse FK-schema lookup."""
    import time

    backend = connection_by_backend.adapter
    other_name = f"djtest_trace_other_{str(int(time.time() * 1000))[-8:]}"[:64]
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(f"DROP DATABASE IF EXISTS {backend.quote_identifier(other_name)}")
        except Exception:
            pass
    other = dj.Schema(other_name, connection=connection_by_backend)

    try:

        @schema_by_backend
        class Upstream(dj.Manual):
            definition = """
            up_id : int32
            ---
            label : varchar(32)
            """

        @other
        class Downstream(dj.Manual):
            # cross-schema FK: Downstream lives in `other`, Upstream in schema_by_backend
            definition = """
            -> Upstream
            down_id : int32
            """

        Upstream.insert([(1, "a"), (2, "b")])
        Downstream.insert([(1, 10), (2, 20)])

        trace = dj.Diagram.trace(Downstream & {"up_id": 1, "down_id": 10})

        assert len(trace[Upstream]) == 1
        assert trace[Upstream].fetch1("up_id") == 1
        assert trace[Upstream].fetch1("label") == "a"
    finally:
        if connection_by_backend.is_connected:
            try:
                connection_by_backend.query(f"DROP DATABASE IF EXISTS {backend.quote_identifier(other_name)}")
            except Exception:
                pass


def test_trace_seed_with_no_ancestors(schema_by_backend):
    """Tracing from a table with no FK parents → trace contains only the seed."""

    @schema_by_backend
    class Standalone(dj.Manual):
        definition = """
        std_id : int32
        """

    Standalone.insert([(1,), (2,)])

    trace = dj.Diagram.trace(Standalone & {"std_id": 1})

    # Only the seed is in the trace
    assert len(trace[Standalone]) == 1
    counts = trace.counts()
    assert counts == {Standalone.full_table_name: 1}


def test_trace_stops_at_master_no_part_down_collection(schema_by_backend):
    """Pins shipped merge-table semantics: trace walks ancestor FK edges only.
    It does NOT descend from an ancestor Master into that Master's Parts — an
    ancestor's Part is included only when the Part itself lies on an FK path
    to the seed. In the merge shape Parent -> Master.P -> Master -> Child,
    trace(Child & key) reaches Master but neither Master.P nor Parent.

    This corrects the design comment on datajoint/datajoint-python discussion
    1232, which described a Master->Parts down-collection that was never
    implemented; the spec (provenance.md, Allowed table set) matches this
    test. If down-collection is ever added deliberately, this test must be
    revised alongside the spec — it exists so the semantics cannot drift
    silently."""

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        """

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

        class P(dj.Part):
            definition = """
            -> master
            -> Parent
            """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Master
        child_id : int32
        """

    Parent.insert([(5,)])
    Master.insert([(10,)])
    Master.P.insert([(10, 5)])
    Child.insert([(10, 1000)])

    trace = dj.Diagram.trace(Child & {"master_id": 10, "child_id": 1000})

    # The Master is a true ancestor — reachable and correctly restricted.
    assert len(trace[Master]) == 1
    assert trace[Master].fetch1("master_id") == 10

    # The Master's Part and the Part's parent are NOT in the trace: the Part
    # is a descendant of Master, not on an ancestor path from Child.
    with pytest.raises(DataJointError, match="not in this trace"):
        trace[Master.P]
    with pytest.raises(DataJointError, match="not in this trace"):
        trace[Parent]
