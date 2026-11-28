"""
Integration tests for cascade part-integrity paths flagged as untested by the
post-2.3 audit: the part_integrity="enforce" check (row-level, before any
row is deleted), the empty-materialization sentinel, and the U3 upward-rule
arm (non-primary master FK).
"""

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a fresh schema per test (mirrors test_cascade_delete.py)."""
    backend = db_creds_by_backend["backend"]
    import time

    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_cintegrity_{backend}_{test_id}"[:64]

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


def test_enforce_refuses_part_without_master(schema_by_backend):
    """A cascade that reaches part rows without their master reaching the
    cascade is refused under enforce, and nothing is deleted."""

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

        class P(dj.Part):
            definition = """
            -> master
            -> Ext
            """

    Ext.insert([(1,)])
    Master.insert([(1,)])
    Master.P.insert([(1, 1)])

    # Cascade from Ext reaches the Part but not the Master -> enforce refuses.
    with pytest.raises(DataJointError, match="before its master"):
        (Ext & {"ext_id": 1}).delete(prompt=False)  # part_integrity="enforce" default

    # Nothing was deleted.
    assert len(Ext()) == 1, "the seed row must survive"
    assert len(Master.P()) == 1, "the part row must survive"
    assert len(Master()) == 1


def test_cascade_empty_materialization_sentinel(schema_by_backend):
    """part_integrity='cascade' with a seed matching ZERO part rows: the
    upward walk materializes an empty master set (always-false sentinel);
    preview and delete complete without error and no master row is touched."""

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

        class P(dj.Part):
            definition = """
            -> master
            -> Ext
            """

    Ext.insert([(1,), (2,)])
    Master.insert([(1,)])
    Master.P.insert([(1, 1)])  # only ext 1 is referenced; ext 2 matches nothing

    counts = dj.Diagram.cascade(Ext & {"ext_id": 2}, part_integrity="cascade").counts()
    assert (
        counts.get(Master.full_table_name, 1) == 0
    ), f"master must appear with zero matches (empty-materialization sentinel); got {counts}"

    (Ext & {"ext_id": 2}).delete(prompt=False, part_integrity="cascade")
    assert len(Ext & {"ext_id": 2}) == 0
    assert len(Master()) == 1, "master row must be untouched"
    assert len(Master.P()) == 1, "part row must be untouched"


def test_upward_u3_nonprimary_master_fk(schema_by_backend):
    """U3 arm of the upward walk: a Part whose `-> master` FK is SECONDARY
    (below the ---). The part's restriction attrs are not a subset of the
    master's PK and the edge is non-aliased, so U3 must project the part onto
    its FK columns (`proj(*attr_map.keys())`) to carry `master_id` upward.
    A bare `proj()` (the pre-#1468 form) would project to the part's PK
    (ext_id, part_id), share no attributes with Master, and restrict BOTH
    masters instead of the correct one — the assertion below distinguishes."""

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

        class P(dj.Part):
            definition = """
            -> Ext
            part_id : int32
            ---
            -> master
            """

    Ext.insert([(1,), (2,)])
    Master.insert([(1,), (2,)])
    # part under master 1 references ext 1; part under master 2 references ext 2
    Master.P.insert([(1, 10, 1), (2, 20, 2)])

    counts = dj.Diagram.cascade(Ext & {"ext_id": 1}, part_integrity="cascade").counts()

    assert counts.get(Master.full_table_name, 0) == 1, (
        f"only master 1 (via the secondary-FK part row) must be restricted; got {counts} — "
        "a bare proj() on the U3 arm would have restricted both masters."
    )


def _declare_probe_groups(schema):
    """Groups whose lead and members both reference Probe.

    Deleting a probe removes the groups it leads (with all their members) and
    the members that reference it, which may belong to groups that are kept.
    """

    @schema
    class Probe(dj.Manual):
        definition = """
        probe : int32
        """

    @schema
    class Group(dj.Manual):
        definition = """
        group_id : int32
        ---
        -> Probe.proj(lead="probe")
        """

        class Member(dj.Part):
            definition = """
            -> master
            -> Probe
            """

    Probe.insert([{"probe": 1}, {"probe": 2}, {"probe": 3}])
    Group.insert([{"group_id": 7, "lead": 2}, {"group_id": 8, "lead": 1}, {"group_id": 9, "lead": 3}])
    Group.Member.insert(
        [
            {"group_id": 7, "probe": 1},
            {"group_id": 7, "probe": 2},
            {"group_id": 8, "probe": 2},
            {"group_id": 9, "probe": 2},
            {"group_id": 9, "probe": 3},
        ]
    )
    return Probe, Group


def test_enforce_refuses_split_master(schema_by_backend):
    """A cascade that removes some rows of the master table must still be
    refused when it removes a part row whose own master row is kept: deleting
    probe 1 removes group 8 but would leave group 7 without its member (7, 1)."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    with pytest.raises(DataJointError, match="before its master"):
        (Probe & {"probe": 1}).delete(prompt=False)

    assert len(Probe()) == 3
    assert len(Group()) == 3
    assert len(Group.Member()) == 5


def test_enforce_preview_refuses_split_master(schema_by_backend):
    """The cascade preview refuses the same delete as Table.delete."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    with pytest.raises(DataJointError, match="before its master"):
        dj.Diagram.cascade(Probe & {"probe": 1})
    with pytest.raises(DataJointError, match="before its master"):
        dj.Diagram.cascade(Probe & {"probe": 1}, part_integrity="enforce")

    counts = dj.Diagram.cascade(Probe & {"probe": 1}, part_integrity="ignore").counts()
    assert counts[Group.full_table_name] == 1
    assert counts[Group.Member.full_table_name] == 2


def test_enforce_allows_master_with_all_parts(schema_by_backend):
    """A cascade whose part rows all belong to master rows it also removes
    is allowed: deleting probe 3 removes group 9 with both of its members."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    (Probe & {"probe": 3}).delete(prompt=False)

    assert len(Probe()) == 2
    assert set(Group.to_arrays("group_id")) == {7, 8}
    assert len(Group.Member()) == 3
    assert len(Group.Member & {"group_id": 9}) == 0


def test_enforce_refuses_part_seed(schema_by_backend):
    """Deleting part rows directly is refused before any row is deleted,
    through the Part class and through a FreeTable of the part table."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    with pytest.raises(DataJointError):
        (Group.Member & {"group_id": 7, "probe": 1}).delete(prompt=False)

    part = dj.FreeTable(schema_by_backend.connection, Group.Member.full_table_name)
    with pytest.raises(DataJointError, match="before its master"):
        (part & {"group_id": 7, "probe": 1}).delete(prompt=False)
    with pytest.raises(DataJointError, match="before its master"):
        dj.Diagram.cascade(part & {"group_id": 7, "probe": 1})

    assert len(Group.Member()) == 5
    assert len(Group()) == 3


def test_ignore_deletes_split_parts(schema_by_backend):
    """part_integrity='ignore' deletes the part rows without their master."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    (Probe & {"probe": 1}).delete(prompt=False, part_integrity="ignore")

    assert set(Probe.to_arrays("probe")) == {2, 3}
    assert set(Group.to_arrays("group_id")) == {7, 9}
    assert len(Group.Member & {"group_id": 7}) == 1
    assert len(Group.Member & {"group_id": 7, "probe": 1}) == 0


def test_cascade_deletes_split_master(schema_by_backend):
    """part_integrity='cascade' removes the master rows of the reached parts,
    with their other parts."""
    Probe, Group = _declare_probe_groups(schema_by_backend)

    (Probe & {"probe": 1}).delete(prompt=False, part_integrity="cascade")

    assert set(Probe.to_arrays("probe")) == {2, 3}
    assert set(Group.to_arrays("group_id")) == {9}
    assert len(Group.Member()) == 2


def test_enforce_checks_part_reached_through_renamed_master_reference(schema_by_backend):
    """A part that references its master a second time under a renamed
    attribute is checked even when every edge into it comes from the master:
    deleting group 2 reaches link (1, 2) of group 1, which is kept."""

    @schema_by_backend
    class Group(dj.Manual):
        definition = """
        group_id : int32
        """

        class Link(dj.Part):
            definition = """
            -> master
            -> Group.proj(target="group_id")
            """

    Group.insert([{"group_id": 1}, {"group_id": 2}])
    Group.Link.insert([{"group_id": 1, "target": 2}, {"group_id": 2, "target": 2}])

    with pytest.raises(DataJointError, match="before its master"):
        (Group & {"group_id": 2}).delete(prompt=False)
    assert len(Group()) == 2
    assert len(Group.Link()) == 2

    # Without a link from a kept group, the master rows go with their parts.
    (Group.Link & {"group_id": 1}).delete(prompt=False, part_integrity="ignore")
    (Group & {"group_id": 2}).delete(prompt=False)
    assert set(Group.to_arrays("group_id")) == {1}
    assert len(Group.Link()) == 0
