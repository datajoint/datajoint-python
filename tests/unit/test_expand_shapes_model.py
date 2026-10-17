"""
The Traversal Shape Catalog, checked against the reference model.

Each test mirrors one shape of ``traversal-shapes.md`` (datajoint-docs,
``src/reference/specs/``) under the same ID: the same schema, rows and seeds,
and the rows the catalog states, copied from it by hand. The node set of each
result (every table the catalog lists, including those marked *none*) is
checked as the table-level reach. Where the catalog's "Why" makes a claim the
model can express (a copy or a relabeling that would select other rows, the
delete modes, the rounds of an iterating group), that claim is checked too.

A seed is written as the rows its condition selects, by hand. Where the model
can evaluate the condition (a dict, a list of dicts, a query expression, a
``Not``), the test also checks that it selects those rows.

Left to the integration suite, because they need a database:

- K3's string seeds (with and without a subquery) and the ``AndList`` with a
  string; the seed kinds the model can express are checked here;
- K4's ``dj.Top`` evaluation; the expansion of the two rows it keeps is checked
  here;
- K6's warning for a dict that names no attribute;
- X1, the unloaded downstream schema;
- X2's refusal at declaration; the cycle in Q that it refuses is checked here.

Table names are DataJoint's: ``session__trial`` is ``Session.Trial``.
"""

import networkx as nx
import pytest

from .expand_model import (
    FK,
    Not,
    PartIntegrityError,
    Relation,
    RowGraph,
    Schema,
    TableSpec,
    apply_delete,
    copy_allowed,
    dependency_graph,
    group_whole,
    make_db,
    one_pass,
    q_acyclic,
    r1_down,
    relabel_down_allowed,
    relabel_up,
    renames_master_key,
    restrict,
    ri_ok,
    subdatabase,
    table_reach,
)


def rows(spec: dict) -> frozenset:
    """Row identifiers from ``{table: [key, ...]}``; a bare value is a one-attribute key."""
    return frozenset((t, k if isinstance(k, tuple) else (k,)) for t, keys in spec.items() for k in keys)


class Shape:
    """One shape: its schema and rows, and the operations on them."""

    def __init__(self, tables, fks, data):
        self.schema = Schema(tables, fks)
        self.db = make_db(self.schema, data)
        assert ri_ok(self.schema, self.db) and group_whole(self.schema, self.db), "the shape must be consistent"
        self.g = RowGraph(self.schema, self.db)

    def seed(self, table, cond, selected):
        """The seed rows, written by hand, after checking that ``table & cond`` selects them."""
        seed = rows({table: selected})
        if cond is not None:
            assert restrict(self.schema, self.db, table, cond) == seed
        return seed

    def check(self, table, seed, direction, expected):
        """``expand(seed, direction)`` returns ``expected``: its rows, and a node for every table it lists."""
        op = {"down": self.g.D, "up": self.g.U, "both": self.g.both}[direction]
        assert op(seed) == rows(expected)
        assert table_reach(self.schema, {table}, direction) == set(expected)

    def fk(self, child, parent, *attrs):
        """The FK from ``child`` to ``parent``, chosen by its child attributes when there are several."""
        (fk,) = [
            f
            for f in self.schema.fks
            if f.child == child and f.parent == parent and (not attrs or set(attrs) == set(f.attr_map))
        ]
        return fk


# =============================================================================
# Foreign-key shapes
# =============================================================================


def test_S1_identity_fk_in_primary_key():
    """traversal-shapes.md, S1. Identity foreign key in the primary key."""
    s = Shape(
        [TableSpec("subject", ("subject_id",)), TableSpec("session", ("subject_id", "session_id"))],
        [FK("session", "subject", {"subject_id": "subject_id"})],
        {"subject": [(1,), (2,)], "session": [(1, 1), (1, 2), (2, 1)]},
    )
    seed = s.seed("subject", {"subject_id": 1}, [1])
    s.check("subject", seed, "down", {"subject": [1], "session": [(1, 1), (1, 2)]})
    seed = s.seed("session", {"session_id": 1}, [(1, 1), (2, 1)])
    s.check("session", seed, "up", {"session": [(1, 1), (2, 1)], "subject": [1, 2]})
    # Why: the condition subject_id = 1 is copied to Session unchanged.
    fk, cond = s.fk("session", "subject"), {"subject_id": 1}
    assert copy_allowed(s.schema, fk, cond)
    assert restrict(s.schema, s.db, "session", cond) == r1_down(s.schema, s.db, fk, rows({"subject": [1]}))


def test_S2_secondary_fk():
    """traversal-shapes.md, S2. Secondary foreign key."""
    s = Shape(
        [TableSpec("#rig", ("rig_id",)), TableSpec("session", ("session_id",), ("rig_id",))],
        [FK("session", "#rig", {"rig_id": "rig_id"})],
        {"#rig": [(1,), (2,), (3,)], "session": [(1, 1), (2, 1), (3, 2)]},
    )
    seed = s.seed("#rig", {"rig_id": 1}, [1])
    s.check("#rig", seed, "down", {"#rig": [1], "session": [1, 2]})
    seed = s.seed("session", {"session_id": 3}, [3])
    s.check("session", seed, "up", {"session": [3], "#rig": [2]})
    # Why: downward, rig_id = 1 names only the FK's own non-null attribute, so it is copied.
    fk = s.fk("session", "#rig")
    assert copy_allowed(s.schema, fk, {"rig_id": 1})
    # Why: relabeling the dict upward would drop session_id and select every rig.
    relabeled = relabel_up(fk, [{"session_id": 3}])
    assert relabeled == [{}]
    assert restrict(s.schema, s.db, "#rig", relabeled) == rows({"#rig": [1, 2, 3]})


def test_S3_nullable_fk_sharing_a_primary_key_column():
    """traversal-shapes.md, S3. Nullable foreign key sharing a primary-key column."""
    s = Shape(
        [
            TableSpec("subject", ("subject_id",)),
            TableSpec("surgery", ("subject_id", "surgery_id")),
            TableSpec("session", ("subject_id", "session_id"), ("surgery_id",)),
        ],
        [
            FK("surgery", "subject", {"subject_id": "subject_id"}),
            FK("session", "subject", {"subject_id": "subject_id"}),
            FK("session", "surgery", {"subject_id": "subject_id", "surgery_id": "surgery_id"}, nullable=True),
        ],
        {
            "subject": [(1,), (2,)],
            "surgery": [(1, 1), (1, 2), (2, 1)],
            "session": [(1, 1, 1), (1, 2, None), (1, 3, 2), (2, 1, 1)],
        },
    )
    assert s.schema.nullable["session"] == {"surgery_id"}  # only surgery_id can be NULL
    seed = s.seed("surgery", {"subject_id": 1}, [(1, 1), (1, 2)])
    s.check("surgery", seed, "down", {"surgery": [(1, 1), (1, 2)], "session": [(1, 1), (1, 3)]})
    seed = s.seed("session", {"subject_id": 1, "session_id": 2}, [(1, 2)])
    s.check("session", seed, "up", {"session": [(1, 2)], "subject": [1], "surgery": []})
    # Why: the FK has a nullable column, so subject_id = 1 is not copied; a copy
    # would also delete session (1, 2) (#1590). Relabeling the keys is refused too.
    fk, cond = s.fk("session", "surgery"), {"subject_id": 1}
    assert not copy_allowed(s.schema, fk, cond)
    assert ("session", (1, 2)) in restrict(s.schema, s.db, "session", cond)
    assert not relabel_down_allowed(s.schema, fk, [cond])


def test_S4_renamed_fk():
    """traversal-shapes.md, S4. Renamed foreign key, with an unrelated attribute named neuron_id."""
    s = Shape(
        [TableSpec("neuron", ("neuron_id",)), TableSpec("input", ("source_id", "input_id"), ("neuron_id",))],
        [FK("input", "neuron", {"source_id": "neuron_id"})],
        {"neuron": [(1,), (2,), (3,)], "input": [(1, 1, 2), (1, 2, 3), (2, 1, 1), (3, 1, 1)]},
    )
    seed = s.seed("neuron", {"neuron_id": 1}, [1])
    s.check("neuron", seed, "down", {"neuron": [1], "input": [(1, 1), (1, 2)]})
    seed = s.seed("input", {"source_id": 2, "input_id": 1}, [(2, 1)])
    s.check("input", seed, "up", {"input": [(2, 1)], "neuron": [2]})
    # Why: copying neuron_id = 1 to Input would match the unrelated label.
    cond = {"neuron_id": 1}
    assert not copy_allowed(s.schema, s.fk("input", "neuron"), cond)
    assert restrict(s.schema, s.db, "input", cond) == rows({"input": [(2, 1), (3, 1)]})


def test_S5_parallel_fks():
    """traversal-shapes.md, S5. Parallel foreign keys."""
    s = Shape(
        [TableSpec("neuron", ("neuron_id",)), TableSpec("synapse", ("neuron_id", "target_id"))],
        [FK("synapse", "neuron", {"neuron_id": "neuron_id"}), FK("synapse", "neuron", {"target_id": "neuron_id"})],
        {"neuron": [(1,), (2,), (3,)], "synapse": [(1, 2), (2, 3), (3, 1)]},
    )
    seed = s.seed("neuron", {"neuron_id": 1}, [1])
    s.check("neuron", seed, "down", {"neuron": [1], "synapse": [(1, 2), (3, 1)]})
    s.check("neuron", seed, "both", {"neuron": [1, 2, 3], "synapse": [(1, 2), (3, 1)]})
    up_seed = s.seed("synapse", {"neuron_id": 1, "target_id": 2}, [(1, 2)])
    s.check("synapse", up_seed, "up", {"synapse": [(1, 2)], "neuron": [1, 2]})
    # Why: combining the two FKs with AND selects only synapses from neuron 1 to itself: none.
    per_fk = [r1_down(s.schema, s.db, fk, seed) for fk in s.schema.fks]
    assert per_fk[0] & per_fk[1] == set()
    # Why (and diagram.md, "Why both is U(D(S))"): D(S) ∪ U(S) leaves out neurons 2 and 3,
    # so it carries synapse (3, 1) without neuron 3.
    union = s.g.D(seed) | s.g.U(seed)
    assert union == rows({"neuron": [1], "synapse": [(1, 2), (3, 1)]})
    assert not ri_ok(s.schema, subdatabase(s.db, union))


def test_S6_diamond_with_a_shared_key():
    """traversal-shapes.md, S6. Diamond with a shared key."""
    s = Shape(
        [
            TableSpec("subject", ("subject_id",)),
            TableSpec("session", ("subject_id", "session_id")),
            TableSpec("implant", ("subject_id", "implant_id")),
            TableSpec("recording", ("subject_id", "session_id", "implant_id")),
        ],
        [
            FK("session", "subject", {"subject_id": "subject_id"}),
            FK("implant", "subject", {"subject_id": "subject_id"}),
            FK("recording", "session", {"subject_id": "subject_id", "session_id": "session_id"}),
            FK("recording", "implant", {"subject_id": "subject_id", "implant_id": "implant_id"}),
        ],
        {
            "subject": [(1,), (2,)],
            "session": [(1, 1), (1, 2), (2, 1)],
            "implant": [(1, 1), (1, 2), (2, 1)],
            "recording": [(1, 1, 1), (1, 2, 2), (2, 1, 1)],
        },
    )
    seed = s.seed("subject", {"subject_id": 1}, [1])
    s.check(
        "subject",
        seed,
        "down",
        {
            "subject": [1],
            "session": [(1, 1), (1, 2)],
            "implant": [(1, 1), (1, 2)],
            "recording": [(1, 1, 1), (1, 2, 2)],
        },
    )
    seed = s.seed("implant", {"subject_id": 1, "implant_id": 1}, [(1, 1)])
    s.check("implant", seed, "down", {"implant": [(1, 1)], "recording": [(1, 1, 1)]})
    s.check(
        "implant",
        seed,
        "both",
        {"implant": [(1, 1)], "recording": [(1, 1, 1)], "subject": [1], "session": [(1, 1)]},
    )
    seed = s.seed("recording", {"subject_id": 1, "session_id": 2, "implant_id": 2}, [(1, 2, 2)])
    s.check(
        "recording",
        seed,
        "up",
        {"recording": [(1, 2, 2)], "session": [(1, 2)], "implant": [(1, 2)], "subject": [1]},
    )


def test_S7_diamond_reached_through_a_secondary_fk():
    """traversal-shapes.md, S7. Diamond reached through a secondary foreign key."""
    s = Shape(
        [
            TableSpec("lab", ("lab_id",)),
            TableSpec("rig", ("rig_id",), ("lab_id",)),
            TableSpec("session", ("lab_id", "session_id"), ("rig_id",)),
        ],
        [
            FK("rig", "lab", {"lab_id": "lab_id"}),
            FK("session", "lab", {"lab_id": "lab_id"}),
            FK("session", "rig", {"rig_id": "rig_id"}),
        ],
        {"lab": [(1,), (2,)], "rig": [(10, 1), (20, 2)], "session": [(1, 1, 20), (2, 1, 10), (2, 2, 20)]},
    )
    seed = s.seed("lab", {"lab_id": 1}, [1])
    s.check("lab", seed, "down", {"lab": [1], "rig": [10], "session": [(1, 1), (2, 1)]})
    seed = s.seed("session", {"lab_id": 2, "session_id": 1}, [(2, 1)])
    s.check("session", seed, "up", {"session": [(2, 1)], "rig": [10], "lab": [1, 2]})
    seed = s.seed("rig", {"rig_id": 10}, [10])
    s.check("rig", seed, "both", {"rig": [10], "session": [(2, 1)], "lab": [1, 2]})
    # Why: Rig's condition lab_id = 1 names an attribute that Session's FK to Rig does not
    # carry, so it is not copied. A copy would miss session (2, 1), and the delete would
    # then break its FK to rig 10.
    cond = {"lab_id": 1}
    assert not copy_allowed(s.schema, s.fk("session", "rig"), cond)
    copied = restrict(s.schema, s.db, "session", cond)
    assert copied == rows({"session": [(1, 1)]})
    assert not ri_ok(s.schema, apply_delete(s.db, rows({"lab": [1], "rig": [10]}) | copied))


# =============================================================================
# Group shapes
# =============================================================================


def test_G1_part_to_master_lift():
    """traversal-shapes.md, G1. Part-to-master lift, and the delete modes."""
    s = Shape(
        [
            TableSpec("session", ("session_id",)),
            TableSpec("session__trial", ("session_id", "trial_id"), master="session"),
            TableSpec("review", ("session_id", "review_id")),
        ],
        [
            FK("session__trial", "session", {"session_id": "session_id"}),
            FK("review", "session", {"session_id": "session_id"}),
        ],
        {
            "session": [(1,), (2,), (3,)],
            "session__trial": [(1, 1), (1, 2), (2, 1), (3, 1)],
            "review": [(1, 1), (2, 1)],
        },
    )
    seed = s.seed("session__trial", {"trial_id": 2}, [(1, 2)])
    down = {"session__trial": [(1, 1), (1, 2)], "session": [1], "review": [(1, 1)]}
    s.check("session__trial", seed, "down", down)
    s.check("session__trial", seed, "up", {"session__trial": [(1, 1), (1, 2)], "session": [1]})
    # Why: the lift is Session & (Session.Trial & {"trial_id": 2}), only session 1. The
    # old upward copy of trial_id = 2 named no attribute of Session and selected every session.
    trials = Relation(("session_id", "trial_id"), frozenset({(1, 2)}))
    assert restrict(s.schema, s.db, "session", trials) == rows({"session": [1]})
    assert restrict(s.schema, s.db, "session", {"trial_id": 2}) == rows({"session": [1, 2, 3]})
    # The delete modes.
    assert s.g.delete(seed, "cascade") == rows(down)
    assert s.g.delete(seed, "ignore") == rows({"session__trial": [(1, 2)]})
    with pytest.raises(PartIntegrityError, match="session__trial"):
        s.g.delete(seed, "enforce")


def test_G2_part_of_part_with_two_chains():
    """traversal-shapes.md, G2. Part-of-part with two chains."""
    s = Shape(
        [
            TableSpec("ext", ("ext_id",)),
            TableSpec("master", ("master_id",)),
            TableSpec("master__q", ("master_id", "q_id"), ("ext_id",), master="master"),
            TableSpec("master__p", ("master_id", "p_id"), ("q_id", "ext_id"), master="master"),
        ],
        [
            FK("master__q", "master", {"master_id": "master_id"}),
            FK("master__q", "ext", {"ext_id": "ext_id"}),
            FK("master__p", "master", {"master_id": "master_id"}),
            FK("master__p", "master__q", {"master_id": "master_id", "q_id": "q_id"}),
            FK("master__p", "ext", {"ext_id": "ext_id"}),
        ],
        {
            "ext": [(1,), (2,)],
            "master": [(1,), (2,), (3,)],
            "master__q": [(1, 10, 1), (2, 20, 2), (3, 30, 2)],
            "master__p": [(1, 100, 10, 2), (2, 200, 20, 1), (3, 300, 30, 2)],
        },
    )
    seed = s.seed("ext", {"ext_id": 1}, [1])
    s.check(
        "ext",
        seed,
        "down",
        {"ext": [1], "master__q": [(1, 10), (2, 20)], "master__p": [(1, 100), (2, 200)], "master": [1, 2]},
    )
    seed = s.seed("master__p", {"master_id": 1, "p_id": 100}, [(1, 100)])
    s.check(
        "master__p",
        seed,
        "up",
        {"master__p": [(1, 100)], "master": [1], "master__q": [(1, 10)], "ext": [1, 2]},
    )
    # Why: the lift is one step on master_id, so the group needs no iteration.
    assert not any(renames_master_key(s.schema, fk) for fk in s.schema.intra_group_fks("master"))


def test_G3_second_renamed_fk_to_its_own_master():
    """traversal-shapes.md, G3. A second, renamed foreign key to its own master."""
    s = Shape(
        [
            TableSpec("session", ("session_id",)),
            TableSpec("session__link", ("session_id", "link_id"), ("other_session",), master="session"),
        ],
        [
            FK("session__link", "session", {"session_id": "session_id"}),
            FK("session__link", "session", {"other_session": "session_id"}),
        ],
        {"session": [(1,), (2,), (3,)], "session__link": [(1, 1, 2), (3, 1, 1)]},
    )
    seed = s.seed("session__link", {"session_id": 1}, [(1, 1)])
    s.check("session__link", seed, "down", {"session__link": [(1, 1), (3, 1)], "session": [1, 3]})
    s.check("session__link", seed, "up", {"session__link": [(1, 1)], "session": [1, 2]})
    # Why: the renamed FK links one instance to another, so this group iterates. One
    # pass alone would stop at session 1; with the iteration it reaches session 3.
    assert renames_master_key(s.schema, s.fk("session__link", "session", "other_session"))
    assert one_pass(s.schema, s.db, seed, "down", iterate=None)[0] == rows({"session__link": [(1, 1)], "session": [1]})
    assert one_pass(s.schema, s.db, seed, "down", iterate="spec")[0] == s.g.D(seed)
    # With part_integrity="enforce", deleting session 1 raises: link (3, 1) would go without session 3.
    with pytest.raises(PartIntegrityError, match=r"session__link.*\(3, 1\)"):
        s.g.delete(rows({"session": [1]}), "enforce")


def test_G4_renamed_sibling_fk_chained_across_three_instances():
    """traversal-shapes.md, G4. Renamed sibling foreign key, chained across three instances."""
    s = Shape(
        [
            TableSpec("run", ("run_id",)),
            TableSpec("run__step", ("run_id", "step_id"), master="run"),
            TableSpec("run__carry", ("run_id", "carry_id"), ("prev_run", "prev_step"), master="run"),
        ],
        [
            FK("run__step", "run", {"run_id": "run_id"}),
            FK("run__carry", "run", {"run_id": "run_id"}),
            FK("run__carry", "run__step", {"prev_run": "run_id", "prev_step": "step_id"}),
        ],
        {
            "run": [(1,), (2,), (3,), (4,)],
            "run__step": [(1, 1), (2, 1), (3, 1), (4, 1)],
            "run__carry": [(2, 1, 1, 1), (3, 1, 2, 1)],
        },
    )
    run1 = s.seed("run", {"run_id": 1}, [1])
    s.check(
        "run",
        run1,
        "down",
        {"run": [1, 2, 3], "run__step": [(1, 1), (2, 1), (3, 1)], "run__carry": [(2, 1), (3, 1)]},
    )
    run2 = s.seed("run", {"run_id": 2}, [2])
    s.check("run", run2, "down", {"run": [2, 3], "run__step": [(2, 1), (3, 1)], "run__carry": [(2, 1), (3, 1)]})
    s.check("run", run2, "up", {"run": [1, 2], "run__step": [(1, 1), (2, 1)], "run__carry": [(2, 1)]})
    s.check(
        "run",
        run2,
        "both",
        {"run": [1, 2, 3], "run__step": [(1, 1), (2, 1), (3, 1)], "run__carry": [(2, 1), (3, 1)]},
    )
    # Why: one pass over the group would stop at run 1. Iterating, the master keys grow
    # as {1}, {1, 2}, {1, 2, 3}, and a fourth round adds nothing. Upward the chain runs
    # the other way, from run 2 back to run 1.
    assert one_pass(s.schema, s.db, run1, "down", iterate=None)[0] == rows({"run": [1], "run__step": [(1, 1)]})
    found, rounds = one_pass(s.schema, s.db, run1, "down", iterate="spec")
    assert found == s.g.D(run1)
    assert rounds["run"] == [rows({"run": [1]}), rows({"run": [1, 2]}), rows({"run": [1, 2, 3]})]
    found, rounds = one_pass(s.schema, s.db, run2, "up", iterate="spec")
    assert found == s.g.U(run2)
    assert rounds["run"] == [rows({"run": [2]}), rows({"run": [1, 2]})]


def test_G5_merge_table():
    """traversal-shapes.md, G5. Merge table."""
    s = Shape(
        [
            TableSpec("ephys_source", ("ephys_id",)),
            TableSpec("imaging_source", ("imaging_id",)),
            TableSpec("recording", ("recording_id",)),
            TableSpec("recording__ephys", ("recording_id",), ("ephys_id",), master="recording"),
            TableSpec("recording__imaging", ("recording_id",), ("imaging_id",), master="recording"),
            TableSpec("analysis", ("recording_id",)),
        ],
        [
            FK("recording__ephys", "recording", {"recording_id": "recording_id"}),
            FK("recording__ephys", "ephys_source", {"ephys_id": "ephys_id"}),
            FK("recording__imaging", "recording", {"recording_id": "recording_id"}),
            FK("recording__imaging", "imaging_source", {"imaging_id": "imaging_id"}),
            FK("analysis", "recording", {"recording_id": "recording_id"}),
        ],
        {
            "ephys_source": [(1,), (2,)],
            "imaging_source": [(1,)],
            "recording": [(1,), (2,), (3,)],
            "recording__ephys": [(1, 1), (2, 2)],
            "recording__imaging": [(3, 1)],
            "analysis": [(1,), (2,), (3,)],
        },
    )
    seed = s.seed("analysis", {"recording_id": 1}, [1])
    s.check(
        "analysis",
        seed,
        "up",
        {
            "analysis": [1],
            "recording": [1],
            "recording__ephys": [1],
            "recording__imaging": [],
            "ephys_source": [1],
            "imaging_source": [],
        },
    )
    seed = s.seed("ephys_source", {"ephys_id": 2}, [2])
    s.check(
        "ephys_source",
        seed,
        "down",
        {"ephys_source": [2], "recording__ephys": [2], "recording": [2], "recording__imaging": [], "analysis": [2]},
    )
    # With part_integrity="enforce" that delete raises: the part row would go without its master.
    with pytest.raises(PartIntegrityError, match="recording__ephys"):
        s.g.delete(seed, "enforce")


def test_G6_part_violating_a1():
    """traversal-shapes.md, G6. A part violating A1, traversed as a plain table."""
    s = Shape(
        [TableSpec("probe", ("probe_id",)), TableSpec("probe__channel", ("host_id", "channel_id"), master="probe")],
        [FK("probe__channel", "probe", {"host_id": "probe_id"})],
        {"probe": [(1,), (2,)], "probe__channel": [(1, 1), (1, 2), (2, 1)]},
    )
    assert s.schema.a1_parts == {"probe": ()}
    seed = s.seed("probe__channel", {"host_id": 1, "channel_id": 1}, [(1, 1)])
    s.check("probe__channel", seed, "down", {"probe__channel": [(1, 1)]})
    s.check("probe__channel", seed, "up", {"probe__channel": [(1, 1)], "probe": [1]})
    seed = s.seed("probe", {"probe_id": 1}, [1])
    s.check("probe", seed, "down", {"probe": [1], "probe__channel": [(1, 1), (1, 2)]})


# =============================================================================
# Seed shapes: K1 to K6 share one schema
# =============================================================================


def k_shape(extra_flags=()):
    return Shape(
        [
            TableSpec("subject", ("subject_id",), ("species",)),
            TableSpec("session", ("subject_id", "session_id"), ("quality",)),
            TableSpec("trial", ("subject_id", "session_id", "trial_id")),
            TableSpec("flag", ("flag_id",), ("subject_id",)),
        ],
        [
            FK("session", "subject", {"subject_id": "subject_id"}),
            FK("trial", "session", {"subject_id": "subject_id", "session_id": "session_id"}),
            FK("flag", "subject", {"subject_id": "subject_id"}, nullable=True),
        ],
        {
            "subject": [(1, "mouse"), (2, "rat"), (3, "mouse"), (4, "rat")],
            "session": [(1, 1, 0), (1, 2, 1), (2, 1, 1), (2, 2, 0), (3, 1, 1)],
            "trial": [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (3, 1, 1)],
            "flag": [(1, 1), (2, 3), *extra_flags],
        },
    )


SESSION = Relation(("subject_id", "session_id", "quality"))
#: ``Session & "quality = 0"``, written out: the sessions of quality 0.
QUALITY_0 = Relation(SESSION.heading, frozenset({(1, 1, 0), (2, 2, 0)}))
ALL_SESSIONS = [(1, 1), (1, 2), (2, 1), (2, 2), (3, 1)]
ALL_TRIALS = [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1), (3, 1, 1)]


def test_K1_seed_restricted_by_a_descendant():
    """traversal-shapes.md, K1. Seed restricted by a descendant."""
    s = k_shape()
    seed = s.seed("subject", QUALITY_0, [1, 2])  # Subject & (Session & "quality = 0")
    s.check(
        "subject",
        seed,
        "down",
        {
            "subject": [1, 2],
            "session": [(1, 1), (1, 2), (2, 1), (2, 2)],
            "trial": [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)],
            "flag": [1],
        },
    )
    s.check("subject", seed, "up", {"subject": [1, 2]})
    # diagram.md, R1: copied to Session, the seed's condition would select only the
    # sessions of quality 0; and the seed Subject - (Session & "quality = 0") would
    # select every session of another quality, including sessions of subjects that are
    # not deleted.
    fk = s.fk("session", "subject")
    assert not copy_allowed(s.schema, fk, QUALITY_0)
    assert restrict(s.schema, s.db, "session", QUALITY_0) == rows({"session": [(1, 1), (2, 2)]})
    others = s.seed("subject", Not(QUALITY_0), [3, 4])
    assert s.g.D(others) == rows({"subject": [3, 4], "session": [(3, 1)], "trial": [(3, 1, 1)], "flag": [2]})
    assert restrict(s.schema, s.db, "session", Not(QUALITY_0)) == rows({"session": [(1, 2), (2, 1), (3, 1)]})


def test_K2_self_referencing_seed():
    """traversal-shapes.md, K2. Self-referencing seed: its condition reads its own table."""
    s = k_shape()
    # Session & (dj.U("subject_id") & (Session & "quality = 0")): the subjects with a session of quality 0.
    subjects_with_q0 = Relation(("subject_id",), frozenset({(1,), (2,)}))
    seed = s.seed("session", subjects_with_q0, [(1, 1), (1, 2), (2, 1), (2, 2)])
    s.check(
        "session",
        seed,
        "down",
        {"session": [(1, 1), (1, 2), (2, 1), (2, 2)], "trial": [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)]},
    )
    s.check("session", seed, "up", {"session": [(1, 1), (1, 2), (2, 1), (2, 2)], "subject": [1, 2]})


def test_K3_seed_kinds_give_the_same_rows():
    """
    traversal-shapes.md, K3. Seed kinds give the same rows.

    The kinds the model can evaluate are checked to select sessions (2, 1) and
    (2, 2); the string kinds need a database.
    """
    s = k_shape()
    sessions_of_2 = [(2, 1), (2, 2)]
    rats = Relation(("subject_id", "species"), frozenset({(2, "rat"), (4, "rat")}))
    flags = Relation(("flag_id", "subject_id"), frozenset({(1, 1), (2, 3)}))
    for cond in (
        {"subject_id": 2},
        [{"subject_id": 2, "session_id": 1}, {"subject_id": 2, "session_id": 2}],
        rats,  # Session & (Subject & {"species": "rat"})
        Not(flags),  # Session & dj.Not(Flag), or Session - Flag
    ):
        seed = s.seed("session", cond, sessions_of_2)
    s.check("session", seed, "down", {"session": [(2, 1), (2, 2)], "trial": [(2, 1, 1), (2, 2, 1)]})
    s.check("session", seed, "up", {"session": [(2, 1), (2, 2)], "subject": [2]})
    # The NOT IN NULL trap: after inserting Flag (3; NULL), Session & dj.Not(Flag) selects nothing.
    s = k_shape(extra_flags=[(3, None)])
    flags = Relation(("flag_id", "subject_id"), frozenset({(1, 1), (2, 3), (3, None)}))
    seed = s.seed("session", Not(flags), [])
    s.check("session", seed, "down", {"session": [], "trial": []})
    s.check("session", seed, "up", {"session": [], "subject": []})


def test_K4_top_seed():
    """traversal-shapes.md, K4. ``Session & "quality = 1" & dj.Top(2)`` keeps sessions (1, 2) and (2, 1)."""
    s = k_shape()
    seed = s.seed("session", None, [(1, 2), (2, 1)])
    s.check("session", seed, "down", {"session": [(1, 2), (2, 1)], "trial": [(1, 2, 1), (2, 1, 1)]})
    s.check("session", seed, "up", {"session": [(1, 2), (2, 1)], "subject": [1, 2]})


def test_K5_empty_seeds():
    """traversal-shapes.md, K5. Empty seeds: nothing in, nothing out, and the nodes are present."""
    s = k_shape()
    for cond in ([], {"subject_id": 4}, None):  # None stands for Session & False
        seed = s.seed("session", cond, [])
        s.check("session", seed, "down", {"session": [], "trial": []})
        s.check("session", seed, "up", {"session": [], "subject": []})


def test_K6_unrestricted_seed():
    """traversal-shapes.md, K6. Unrestricted seed; a dict naming no attribute of Session means all rows."""
    s = k_shape()
    for cond in ({}, {"species": "rat"}):
        seed = s.seed("session", cond, ALL_SESSIONS)
        s.check("session", seed, "down", {"session": ALL_SESSIONS, "trial": ALL_TRIALS})
        s.check("session", seed, "up", {"session": ALL_SESSIONS, "subject": [1, 2, 3]})


# =============================================================================
# Graph shapes
# =============================================================================


def test_X2_group_cycle():
    """
    traversal-shapes.md, X2. The cycle in Q that declaring Session.Note would close.

    The refusal itself happens at declaration and is tested with a database.
    """
    tables = [
        TableSpec("session", ("session_id",)),
        TableSpec("session__trial", ("session_id", "trial_id"), master="session"),
        TableSpec("review", ("session_id",)),
    ]
    fks = [
        FK("session__trial", "session", {"session_id": "session_id"}),
        FK("review", "session", {"session_id": "session_id"}),
    ]
    before = Schema(tables, fks)
    assert q_acyclic(before)
    dependency_graph(before).topo_sort()
    # Session.Note: -> master, then -> Review below the line. Review's key is session_id,
    # which Note already holds, so the FK adds no attribute.
    after = Schema(
        [*tables, TableSpec("session__note", ("session_id",), master="session")],
        [
            *fks,
            FK("session__note", "session", {"session_id": "session_id"}),
            FK("session__note", "review", {"session_id": "session_id"}),
        ],
    )
    assert after.a1_parts == {"session": ("session__trial", "session__note")}
    assert not q_acyclic(after)  # Session -> Review -> Session.Note, which belongs to Session's group
    with pytest.raises(nx.NetworkXUnfeasible):
        dependency_graph(after).topo_sort()
