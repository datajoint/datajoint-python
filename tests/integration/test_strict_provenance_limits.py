"""
Integration tests that PIN the documented enforcement *limits* of
``dj.config["strict_provenance"]`` (see datajoint-docs
``reference/specs/provenance.md`` §"Enforcement model and its limits").

The runtime gate is a best-effort development guardrail, not an airtight
boundary. Several read/write idioms are documented to bypass it, and exactly
one read path (a join with an undeclared table) is documented to block. These
tests lock that best-effort surface so a regression in *either* direction —
a documented bypass silently closing, or a blocking path silently opening —
becomes visible.

Companion to PRs #1480 / #1484 (which touch the existing strict/cascade test
files); placed in a new file to avoid merge conflicts.
"""

import pytest

import datajoint as dj
import datajoint.provenance as provenance
from datajoint import DataJointError


@pytest.fixture
def strict_mode(connection_test):
    """Enable strict_provenance for the duration of one test."""
    config = connection_test._config
    previous = config.get("strict_provenance", False)
    config["strict_provenance"] = True
    try:
        yield
    finally:
        config["strict_provenance"] = previous


def _probe(fn):
    """Run ``fn`` and return ("ok", result) or ("err", exception).

    Used inside make() so populate() completes regardless of what the gate does:
    we record what happened and assert on the record afterwards.
    """
    try:
        return ("ok", fn())
    except Exception as e:  # noqa: BLE001 - we intentionally capture anything
        return ("err", e)


# =========================================================================
# A1-A6: read-gate documented bypasses and the single documented block.
# All probes run inside one make(); results recorded per-probe.
# =========================================================================


def test_read_gate_documented_limits(prefix, connection_test, strict_mode):
    """Pin the documented read-gate surface (provenance.md §Enforcement limits):

    - len(Unrelated & key)                     -> NO error (COUNT bypasses gate)
    - bool(Unrelated & cond)                   -> NO error (EXISTS bypasses gate)
    - {..} in (Unrelated & restriction)        -> NO error (__contains__ -> bool)
    - {..} in Unrelated  (CLASS form)          -> RAISES  (metaclass __iter__ -> gated cursor)
    - (DeclaredAncestor & Unrelated).fetch()   -> NO error (restriction-by-table not analyzed)
    - (Unrelated * DeclaredAncestor).fetch()   -> RAISES  (join extends support: the one gated read path)
    - len(dj.U(..).aggr(Unrelated, ..))        -> NO error (Aggregation.__len__ bypasses gate)
    - dj.U(..).aggr(Unrelated, ..).fetch()     -> RAISES  (aggregation fetch is gated)
    """
    schema = dj.Schema(f"{prefix}_sp_read_limits", connection=connection_test)

    @schema
    class DeclaredAncestor(dj.Lookup):
        definition = """
        anc_id : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Unrelated(dj.Lookup):
        # References DeclaredAncestor so `anc_id` shares lineage (semantic
        # matching requires it for the semijoin probe below), but Unrelated is
        # a DESCENDANT of the ancestor — not an ancestor of Derived — so it is
        # outside the strict-mode allowed set.
        definition = """
        -> DeclaredAncestor
        ---
        note : varchar(32)
        """
        contents = [(1, "x"), (2, "y")]

    captured: dict = {}

    @schema
    class Derived(dj.Computed):
        definition = """
        -> DeclaredAncestor
        ---
        val : int32
        """

        def make(self, key):
            # 1. len() of a restriction on an undeclared table
            captured["len_restrict"] = _probe(lambda: len(Unrelated & key))
            # 2. bool() of a restriction on an undeclared table
            captured["bool_restrict"] = _probe(lambda: bool(Unrelated & {"anc_id": 1}))
            # 3a. `item in query_expression` (query-expression __contains__ -> bool)
            captured["in_qexpr"] = _probe(lambda: {"anc_id": 1} in (Unrelated & {"anc_id": 1}))
            # 3b. `item in Unrelated` (CLASS form -> metaclass __iter__ -> gated cursor)
            captured["in_class"] = _probe(lambda: {"anc_id": 1} in Unrelated)
            # 4. restriction-by-table: undeclared table used only as a semijoin operand
            captured["restrict_by_table"] = _probe(lambda: (DeclaredAncestor & Unrelated).fetch())
            # 5. join with an undeclared table (extends query support -> gated)
            captured["join"] = _probe(lambda: (Unrelated * DeclaredAncestor).fetch())
            # 6a. aggregation length bypasses the gate
            captured["aggr_len"] = _probe(lambda: len(dj.U("anc_id").aggr(Unrelated, n="count(*)")))
            # 6b. aggregation fetch is gated
            captured["aggr_fetch"] = _probe(lambda: dj.U("anc_id").aggr(Unrelated, n="count(*)").fetch())
            self.insert1({**key, "val": 0})

    Derived.populate()

    # --- documented bypasses: NO error ---
    assert captured["len_restrict"][0] == "ok", captured["len_restrict"]
    assert captured["bool_restrict"][0] == "ok", captured["bool_restrict"]
    assert captured["in_qexpr"][0] == "ok", captured["in_qexpr"]
    assert captured["restrict_by_table"][0] == "ok", captured["restrict_by_table"]
    assert captured["aggr_len"][0] == "ok", captured["aggr_len"]

    # --- documented / gated blocks: RAISES DataJointError ---
    assert captured["in_class"][0] == "err", captured["in_class"]
    assert isinstance(captured["in_class"][1], DataJointError)

    assert captured["join"][0] == "err", captured["join"]
    assert isinstance(captured["join"][1], DataJointError)
    assert "undeclared" in str(captured["join"][1]).lower()

    assert captured["aggr_fetch"][0] == "err", captured["aggr_fetch"]
    assert isinstance(captured["aggr_fetch"][1], DataJointError)


# =========================================================================
# A7: deletes are ungated inside make() (documented limit).
# =========================================================================


def test_delete_quick_ungated_inside_make(prefix, connection_test, strict_mode):
    """delete_quick() on another table / a restriction on an undeclared table is
    NOT intercepted inside a strict make() — deletes are ungated (documented)."""
    schema = dj.Schema(f"{prefix}_sp_delete_ungated", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Other(dj.Manual):
        definition = """
        oid : int32
        ---
        v : int32
        """

    @schema
    class Unrelated(dj.Lookup):
        definition = """
        u_id : int32
        """
        contents = [(1,), (2,)]

    Other.insert([(1, 10), (2, 20)])

    captured: dict = {}

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            captured["other_delete"] = _probe(lambda: (Other & {"oid": 1}).delete_quick())
            captured["unrelated_delete"] = _probe(lambda: (Unrelated & {"u_id": 1}).delete_quick())
            self.insert1({**key, "val": 0})

    Derived.populate()

    assert captured["other_delete"][0] == "ok", captured["other_delete"]
    assert captured["unrelated_delete"][0] == "ok", captured["unrelated_delete"]
    # deletes actually took effect (ungated => really deleted)
    assert len(Other()) == 1
    assert len(Unrelated()) == 1


# =========================================================================
# A8: INSERT ... SELECT (insert from a QueryExpression) passes the target gate
# and does NOT apply per-row key consistency — server-side path.
# =========================================================================


def test_insert_from_query_bypasses_key_consistency(prefix, connection_test, strict_mode):
    """self.insert(SomeTable.proj(...)) is a server-side INSERT...SELECT: the
    target gate passes (destination is self) but the per-row key-consistency
    check does NOT apply, so rows with a key DIFFERENT from the current make()
    key land. Pin the documented key-consistency exception."""
    schema = dj.Schema(f"{prefix}_sp_insert_select", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Seed(dj.Lookup):
        definition = """
        subject_id : int32
        ---
        val : int32
        """
        contents = [(1, 100), (2, 200)]

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            # projection heading == self heading (subject_id, val); rows carry
            # subject_id 1 AND 2 even though the current key is a single subject.
            self.insert(Seed.proj("val"), skip_duplicates=True)

    # Populate ONLY subject_id=1. If per-row key consistency applied, only the
    # subject_id=1 row could land; the subject_id=2 row proves it did not.
    Derived.populate({"subject_id": 1})

    landed = {row["subject_id"]: row["val"] for row in Derived().to_dicts()}
    assert landed == {1: 100, 2: 200}, landed


# =========================================================================
# A9: self.upstream[SelfPart] raises (own Parts not in the upstream trace),
# while reading self.PartName() directly is allowed under strict.
# =========================================================================


def test_upstream_selfpart_vs_direct_part_read(prefix, connection_test, strict_mode):
    """Asymmetry: self.upstream[<own Part>] raises (upstream is the ANCESTOR
    graph; a Part is a descendant, not an ancestor) but a direct read of the
    own Part (self.PartName().fetch()) is allowed by the read gate."""
    schema = dj.Schema(f"{prefix}_sp_selfpart", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    captured: dict = {}

    @schema
    class Master(dj.Computed):
        definition = """
        -> Subject
        ---
        summary : varchar(16)
        """

        class Bin(dj.Part):
            definition = """
            -> master
            bin_id : int32
            """

        def make(self, key):
            self.insert1({**key, "summary": "ok"})
            self.Bin.insert1({**key, "bin_id": 0})
            captured["upstream_selfpart"] = _probe(lambda: self.upstream[self.Bin])
            captured["direct_part_read"] = _probe(lambda: self.Bin().fetch())

    Master.populate()

    # own Part is NOT reachable via self.upstream
    assert captured["upstream_selfpart"][0] == "err", captured["upstream_selfpart"]
    assert isinstance(captured["upstream_selfpart"][1], DataJointError)
    # but a direct read of the own Part is allowed under strict
    assert captured["direct_part_read"][0] == "ok", captured["direct_part_read"]


# =========================================================================
# A10: exception-path context cleanup — a make() that raises leaves no active
# strict context behind.
# =========================================================================


def test_context_cleared_after_make_raises(prefix, connection_test, strict_mode):
    """If make() raises under strict (populate suppress_errors=False), the
    active strict-make context is popped in the finally block: afterwards
    provenance.get_active_context() is None."""
    schema = dj.Schema(f"{prefix}_sp_ctx_cleanup", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Bad(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            raise DataJointError("intentional make() failure")

    assert provenance.get_active_context() is None
    with pytest.raises(DataJointError, match="intentional make"):
        Bad.populate(suppress_errors=False)
    assert provenance.get_active_context() is None


# =========================================================================
# A11: reentrancy of push/pop with token-based restore (no DB needed).
# =========================================================================


def test_strict_context_reentrancy_token_restore():
    """Two nested push/pop contexts restore exactly via tokens: inner pop
    restores the outer context; outer pop restores None."""
    from datajoint.provenance import (
        get_active_context,
        pop_strict_make_context,
        push_strict_make_context,
    )

    outer_target = object()
    inner_target = object()

    assert get_active_context() is None

    tok_outer = push_strict_make_context(outer_target, frozenset({"a"}), {"k": 1})
    ctx_outer = get_active_context()
    assert ctx_outer[0] is outer_target

    tok_inner = push_strict_make_context(inner_target, frozenset({"b"}), {"k": 2})
    assert get_active_context()[0] is inner_target

    # inner pop restores the outer context exactly
    pop_strict_make_context(tok_inner)
    assert get_active_context() == ctx_outer
    assert get_active_context()[0] is outer_target

    # outer pop restores None
    pop_strict_make_context(tok_outer)
    assert get_active_context() is None
