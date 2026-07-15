"""
Integration tests for cascade delete on multiple backends.
"""

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a schema for cascade delete tests."""
    backend = db_creds_by_backend["backend"]
    # Use unique schema name for each test
    import time

    test_id = str(int(time.time() * 1000))[-8:]  # Last 8 digits of timestamp
    schema_name = f"djtest_cascade_{backend}_{test_id}"[:64]  # Limit length

    # Drop schema if exists (cleanup from any previous failed runs)
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass  # Ignore errors during cleanup

    # Create fresh schema
    schema = dj.Schema(schema_name, connection=connection_by_backend)

    yield schema

    # Cleanup after test
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass  # Ignore errors during cleanup


def test_simple_cascade_delete(schema_by_backend):
    """Test basic cascade delete with foreign keys."""

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(255)
        """

    # Insert test data
    Parent.insert1((1, "Parent1"))
    Parent.insert1((2, "Parent2"))
    Child.insert1((1, 1, "Child1-1"))
    Child.insert1((1, 2, "Child1-2"))
    Child.insert1((2, 1, "Child2-1"))

    assert len(Parent()) == 2
    assert len(Child()) == 3

    # Delete parent with cascade
    (Parent & {"parent_id": 1}).delete()

    # Check cascade worked
    assert len(Parent()) == 1
    assert len(Child()) == 1

    # Verify remaining data (using to_dicts for DJ 2.0)
    remaining = Child().to_dicts()
    assert len(remaining) == 1
    assert remaining[0]["parent_id"] == 2
    assert remaining[0]["child_id"] == 1
    assert remaining[0]["data"] == "Child2-1"


def test_multi_level_cascade_delete(schema_by_backend):
    """Test cascade delete through multiple levels of foreign keys."""

    @schema_by_backend
    class GrandParent(dj.Manual):
        definition = """
        gp_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        -> GrandParent
        parent_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(255)
        """

    # Insert test data
    GrandParent.insert1((1, "GP1"))
    Parent.insert1((1, 1, "P1"))
    Parent.insert1((1, 2, "P2"))
    Child.insert1((1, 1, 1, "C1"))
    Child.insert1((1, 1, 2, "C2"))
    Child.insert1((1, 2, 1, "C3"))

    assert len(GrandParent()) == 1
    assert len(Parent()) == 2
    assert len(Child()) == 3

    # Delete grandparent - should cascade through parent to child
    (GrandParent & {"gp_id": 1}).delete()

    # Check everything is deleted
    assert len(GrandParent()) == 0
    assert len(Parent()) == 0
    assert len(Child()) == 0

    # Verify all tables are empty
    assert len(GrandParent().to_dicts()) == 0
    assert len(Parent().to_dicts()) == 0
    assert len(Child().to_dicts()) == 0


def test_cascade_delete_with_renamed_attrs(schema_by_backend):
    """Test cascade delete when foreign key renames attributes."""

    @schema_by_backend
    class Animal(dj.Manual):
        definition = """
        animal_id : int
        ---
        species : varchar(255)
        """

    @schema_by_backend
    class Observation(dj.Manual):
        definition = """
        obs_id : int
        ---
        -> Animal.proj(subject_id='animal_id')
        measurement : float
        """

    # Insert test data
    Animal.insert1((1, "Mouse"))
    Animal.insert1((2, "Rat"))
    Observation.insert1((1, 1, 10.5))
    Observation.insert1((2, 1, 11.2))
    Observation.insert1((3, 2, 15.3))

    assert len(Animal()) == 2
    assert len(Observation()) == 3

    # Delete animal - should cascade to observations
    (Animal & {"animal_id": 1}).delete()

    # Check cascade worked
    assert len(Animal()) == 1
    assert len(Observation()) == 1

    # Verify remaining data
    remaining_animals = Animal().to_dicts()
    assert len(remaining_animals) == 1
    assert remaining_animals[0]["animal_id"] == 2

    remaining_obs = Observation().to_dicts()
    assert len(remaining_obs) == 1
    assert remaining_obs[0]["obs_id"] == 3
    assert remaining_obs[0]["subject_id"] == 2
    assert remaining_obs[0]["measurement"] == 15.3


def test_delete_preview_with_counts(schema_by_backend):
    """Diagram.cascade().counts() previews affected rows without deleting."""

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(255)
        """

    Parent.insert1((1, "P1"))
    Parent.insert1((2, "P2"))
    Child.insert1((1, 1, "C1-1"))
    Child.insert1((1, 2, "C1-2"))
    Child.insert1((2, 1, "C2-1"))

    # Preview restricted cascade via Diagram
    counts = dj.Diagram.cascade(Parent & {"parent_id": 1}).counts()

    assert isinstance(counts, dict)
    assert counts[Parent.full_table_name] == 1
    assert counts[Child.full_table_name] == 2

    # Data must still be intact
    assert len(Parent()) == 2
    assert len(Child()) == 3


def test_cascade_discovers_downstream_schema(connection_by_backend, db_creds_by_backend):
    """Cascade delete discovers and includes tables in unloaded downstream schemas."""
    import time

    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]

    upstream_name = f"djtest_upstream_{backend}_{test_id}"[:64]
    downstream_name = f"djtest_downstream_{backend}_{test_id}"[:64]

    qi = connection_by_backend.adapter.quote_identifier

    # Clean up any previous runs
    for name in (downstream_name, upstream_name):
        try:
            connection_by_backend.query(f"DROP DATABASE IF EXISTS {qi(name)}")
        except Exception:
            pass

    # Create upstream schema and table
    upstream = dj.Schema(upstream_name, connection=connection_by_backend)

    @upstream
    class Parent(dj.Manual):
        definition = """
        parent_id : int
        ---
        name : varchar(100)
        """

    # Create downstream schema with FK to upstream — separate schema object
    downstream = dj.Schema(downstream_name, connection=connection_by_backend)

    @downstream
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(100)
        """

    # Insert data
    Parent.insert1(dict(parent_id=1, name="Alice"))
    Child.insert1(dict(parent_id=1, child_id=1, data="row1"))
    Child.insert1(dict(parent_id=1, child_id=2, data="row2"))

    # Verify cascade preview discovers the downstream schema
    counts = dj.Diagram.cascade(Parent & "parent_id=1").counts()
    assert Parent.full_table_name in counts
    assert Child.full_table_name in counts
    assert counts[Child.full_table_name] == 2

    # Verify actual delete cascades across schemas
    (Parent & "parent_id=1").delete()
    assert len(Parent()) == 0
    assert len(Child()) == 0

    # Clean up
    for name in (downstream_name, upstream_name):
        try:
            connection_by_backend.query(f"DROP DATABASE IF EXISTS {qi(name)}")
        except Exception:
            pass


# =========================================================================
# Issue #1429: cascade with part_integrity="cascade" must traverse the FK
# chain through intermediate Parts (and renamed FKs), not assume that the
# Part shares PK attribute names with its Master.
# =========================================================================


def test_cascade_part_of_part_no_master_reference(schema_by_backend):
    """
    Case 2 from #1429: PartB references PartA directly (no -> Master).
    Restricting PartB with part_integrity="cascade" must restrict both
    PartA and Master (PartA via the direct FK, Master via the master-part
    FK chained through PartA).
    """

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
            -> Master.PartA
            part_b_id : int32
            """

    Master.insert([(1,), (2,)])
    Master.PartA.insert([(1, 10), (1, 11), (2, 20)])
    Master.PartB.insert([(1, 10, 100), (1, 10, 101), (1, 11, 110), (2, 20, 200)])

    # Cascade preview: deleting one PartB row must propagate up to PartA and Master.
    counts = dj.Diagram.cascade(
        Master.PartB & {"master_id": 1, "part_a_id": 10, "part_b_id": 100},
        part_integrity="cascade",
    ).counts()

    # Master row (1,) is the originating Part's master — must appear with count 1
    assert counts.get(Master.full_table_name, 0) == 1, (
        f"Master restricted by 1 row; got {counts.get(Master.full_table_name)}. "
        "Indicates the Part→Master upward propagation did not reach the Master "
        "through the intermediate PartA."
    )
    # Master cascades back down to ALL of master_id=1's Parts
    assert counts.get(Master.PartA.full_table_name, 0) == 2  # rows 10, 11
    assert counts.get(Master.PartB.full_table_name, 0) == 3  # rows under master_id=1


def test_cascade_part_of_part_renamed_fk(schema_by_backend):
    """
    Case 1 from #1429: PartB references PartA via a renamed FK (`.proj()`).
    PartB has no attribute named `master_id` (renamed to `src_master`). The
    upward propagation must use the FK metadata, not assume shared attribute
    names.
    """

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
            -> Master.PartA.proj(src_master='master_id', src_part='part_a_id')
            part_b_id : int32
            """

    Master.insert([(1,), (2,)])
    Master.PartA.insert([(1, 10), (2, 20)])
    Master.PartB.insert([(1, 10, 100), (2, 20, 200)])

    # PartB has columns: src_master, src_part, part_b_id — NOT master_id.
    counts = dj.Diagram.cascade(
        Master.PartB & {"src_master": 1, "src_part": 10, "part_b_id": 100},
        part_integrity="cascade",
    ).counts()

    assert counts.get(Master.full_table_name, 0) == 1, (
        f"Master restricted by 1 row; got {counts.get(Master.full_table_name)}. "
        "Renamed FK was not reversed when propagating up to Master."
    )
    assert counts.get(Master.PartA.full_table_name, 0) == 1
    assert counts.get(Master.PartB.full_table_name, 0) == 1


def test_cascade_three_level_part_chain(schema_by_backend):
    """
    Three-hop chain (#1429 follow-up review): PartC → PartB → PartA → Master.
    Verify intermediate Parts (PartA, PartB) are restricted at every hop, not
    just the first, and the master cascades back down to all siblings.
    """

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
            -> Master.PartA
            part_b_id : int32
            """

        class PartC(dj.Part):
            definition = """
            -> Master.PartB
            part_c_id : int32
            """

    Master.insert([(1,), (2,)])
    Master.PartA.insert([(1, 10), (1, 11), (2, 20)])
    Master.PartB.insert([(1, 10, 100), (1, 11, 110), (2, 20, 200)])
    Master.PartC.insert([(1, 10, 100, 1000), (1, 11, 110, 1100), (2, 20, 200, 2000)])

    counts = dj.Diagram.cascade(
        Master.PartC & {"master_id": 1, "part_a_id": 10, "part_b_id": 100, "part_c_id": 1000},
        part_integrity="cascade",
    ).counts()

    # Master pulled in via the 3-hop upward walk
    assert counts.get(Master.full_table_name, 0) == 1, (
        "Master restriction lost across 3-hop chain — the per-edge upward walk " "did not reach Master through PartA + PartB."
    )
    # Master forward-cascades back down to all rows under master_id=1
    assert counts.get(Master.PartA.full_table_name, 0) == 2  # both PartA rows under master 1
    assert counts.get(Master.PartB.full_table_name, 0) == 2  # both PartB rows under master 1
    assert counts.get(Master.PartC.full_table_name, 0) == 2  # both PartC rows under master 1


def test_cascade_part_of_part_actual_delete(schema_by_backend):
    """
    End-to-end: actually run delete() with part_integrity="cascade" through
    a Part-of-Part chain. Verifies the upward propagation produces SQL that
    executes (no MySQL 1093 self-reference; correct row removal).
    """

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
            -> Master.PartA
            part_b_id : int32
            """

    Master.insert([(1,), (2,)])
    Master.PartA.insert([(1, 10), (2, 20)])
    Master.PartB.insert([(1, 10, 100), (2, 20, 200)])

    (Master.PartB & {"master_id": 1}).delete(part_integrity="cascade")

    # master_id=1 chain is entirely gone; master_id=2 chain intact.
    assert len(Master()) == 1
    assert Master().fetch1("master_id") == 2
    assert len(Master.PartA()) == 1
    assert len(Master.PartB()) == 1


# =========================================================================
# Post-2.3 audit: with part_integrity="cascade", EVERY restricted Part of a
# master must contribute its rows to the master's restriction. The upward
# walk used to be deduplicated per master (first Part wins), silently
# under-restricting the master when a second Part of the same master was
# reached via a different FK path.
# =========================================================================


def test_cascade_two_parts_same_master_both_restrict(schema_by_backend):
    """Two Parts of one master, each referencing an external table. Deleting
    the external row reaches PartA (under master 1) and PartB (under master 2);
    the cascade must restrict BOTH masters, not just the first Part's."""

    @schema_by_backend
    class Ext(dj.Manual):
        definition = """
        ext_id : int32
        """

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

        class PartA(dj.Part):
            definition = """
            -> master
            -> Ext
            """

        class PartB(dj.Part):
            definition = """
            -> master
            -> Ext
            """

    Ext.insert([(1,), (2,)])
    Master.insert([(1,), (2,), (3,)])
    Master.PartA.insert([(1, 1), (3, 2)])  # ext 1 under master 1; control under master 3
    Master.PartB.insert([(2, 1), (3, 2)])  # ext 1 under master 2; control under master 3

    counts = dj.Diagram.cascade(Ext & {"ext_id": 1}, part_integrity="cascade").counts()

    # Both masters must be in the cascade — master 1 (via PartA) AND master 2
    # (via PartB). The old once-per-master dedup returned 1 here.
    assert counts.get(Master.full_table_name, 0) == 2, (
        f"both masters must be restricted (PartA -> master 1, PartB -> master 2); "
        f"got {counts.get(Master.full_table_name)}: the upward walk fired for "
        f"only one Part of the master."
    )

    # End-to-end delete: both compositional units removed; master 3 untouched.
    (Ext & {"ext_id": 1}).delete(prompt=False, part_integrity="cascade")

    remaining_masters = {r["master_id"] for r in Master.to_dicts()}
    assert remaining_masters == {3}, f"masters 1 and 2 must be deleted; got {remaining_masters}"
    assert len(Master.PartA()) == 1  # only the control row (3, 2)
    assert len(Master.PartB()) == 1
    assert len(Ext & {"ext_id": 2}) == 1  # unrelated ext row intact


def test_diagram_cascade_rejects_invalid_part_integrity(schema_by_backend):
    """Diagram.cascade validates part_integrity eagerly (mirrors Table.delete)
    — a typo must raise, not silently degrade to enforce/ignore behavior."""

    @schema_by_backend
    class Simple(dj.Manual):
        definition = """
        sid : int32
        """

    Simple.insert([(1,)])

    with pytest.raises(ValueError, match="part_integrity must be"):
        dj.Diagram.cascade(Simple & {"sid": 1}, part_integrity="casade")


def test_drop_rejects_invalid_part_integrity(schema_by_backend):
    """Table.drop validates part_integrity: 'cascade' is delete-only and typos
    must raise instead of silently behaving as 'ignore'. The table survives."""

    @schema_by_backend
    class Keeper(dj.Manual):
        definition = """
        kid : int32
        """

    Keeper.insert([(1,)])

    with pytest.raises(ValueError, match="part_integrity must be"):
        Keeper.drop(part_integrity="cascade")
    with pytest.raises(ValueError, match="part_integrity must be"):
        Keeper.drop(part_integrity="enforcee")

    # validation fired before any drop work — table intact
    assert len(Keeper()) == 1
