"""
Integration tests for cascade part-integrity paths flagged as untested by the
post-2.3 audit: the Table.delete() enforce post-check (data-driven, with
rollback), the empty-materialization sentinel, and the U3 upward-rule arm
(non-primary master FK).
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


def test_enforce_postcheck_raises_and_rolls_back(schema_by_backend):
    """The data-driven post-check in Table.delete (part rows deleted without
    their master reaching the cascade) must raise AND roll the transaction
    back — previously only the direct Part.delete pre-guard had coverage."""

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

    # Cascade from Ext reaches the Part but not the Master -> enforce post-check.
    with pytest.raises(DataJointError, match="before its master"):
        (Ext & {"ext_id": 1}).delete(prompt=False)  # part_integrity="enforce" default

    # Rollback: nothing was deleted.
    assert len(Ext()) == 1, "rollback must restore the seed row"
    assert len(Master.P()) == 1, "rollback must restore the part row"
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
