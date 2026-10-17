"""
Shape catalog of ``Diagram.expand``, checked against the reference model.

One small test per shape, with the expected rows written out by hand. The shape
IDs match the examples in the specification (``diagram.md``) one to one:

- S1-S7, foreign-key shapes: identity, secondary, nullable with partial NULLs,
  renamed, parallel, diamond with a shared key, diamond reached through a
  secondary FK.
- G1-G6, master-part groups: the part -> master lift, part-of-part with two
  chains, a second renamed FK to the master, a sibling FK that renames the master
  key, the merge table, a part that violates A1.
- K1, K2, K5, K6, seeds: restricted by a descendant, a self-referencing table,
  empty, unrestricted.

K3, K4, X1 and X2 need a database and are tested in the integration suite.
Rows are written ``r(table, *primary_key_values)``.
"""

import networkx as nx
import pytest

from .expand_model import (
    FK,
    RowGraph,
    Schema,
    TableSpec,
    apply_delete,
    dependency_graph,
    group_whole,
    make_db,
    q_acyclic,
    ri_ok,
)


def r(table, *key):
    """A row identifier written by hand."""
    return (table, key)


def graph(schema, data):
    db = make_db(schema, data)
    assert ri_ok(schema, db) and group_whole(schema, db), "the fixture itself must be consistent"
    return RowGraph(schema, db)


# =============================================================================
# S: foreign-key shapes
# =============================================================================


def test_S1_identity_fk_in_primary_key():
    """An unrenamed FK inside the child's primary key: session -> subject."""
    schema = Schema(
        [TableSpec("subject", ("subject_id",)), TableSpec("session", ("subject_id", "session_id"))],
        [FK("session", "subject", {"subject_id": "subject_id"})],
    )
    g = graph(schema, {"subject": [(1,), (2,)], "session": [(1, 1), (1, 2), (2, 1)]})
    assert g.D({r("subject", 1)}) == {r("subject", 1), r("session", 1, 1), r("session", 1, 2)}
    assert g.U({r("session", 1, 2)}) == {r("session", 1, 2), r("subject", 1)}


def test_S2_secondary_fk():
    """An FK below the --- line: session references rig."""
    schema = Schema(
        [TableSpec("rig", ("rig_id",)), TableSpec("session", ("session_id",), ("rig_id",))],
        [FK("session", "rig", {"rig_id": "rig_id"})],
    )
    g = graph(schema, {"rig": [(1,), (2,)], "session": [(1, 1), (2, 1), (3, 2)]})
    assert g.D({r("rig", 1)}) == {r("rig", 1), r("session", 1), r("session", 2)}
    assert g.U({r("session", 3)}) == {r("session", 3), r("rig", 2)}


def test_S3_nullable_fk_sharing_a_primary_key_column():
    """
    A nullable FK (lab_id, person_id) whose lab_id is also in the primary key.

    Assignment (1, 2) has lab_id = 1 and person_id = NULL: by A2 it references no
    person, although its lab_id matches persons (1, 1) and (1, 2). It is still
    reached from lab 1 through its own FK to lab.
    """
    schema = Schema(
        [
            TableSpec("lab", ("lab_id",)),
            TableSpec("person", ("lab_id", "person_id")),
            TableSpec("assignment", ("lab_id", "task_id"), ("person_id",)),
        ],
        [
            FK("person", "lab", {"lab_id": "lab_id"}),
            FK("assignment", "lab", {"lab_id": "lab_id"}),
            FK("assignment", "person", {"lab_id": "lab_id", "person_id": "person_id"}, nullable=True),
        ],
    )
    assert schema.nullable["assignment"] == {"person_id"}
    g = graph(
        schema,
        {
            "lab": [(1,), (2,)],
            "person": [(1, 1), (1, 2), (2, 1)],
            "assignment": [(1, 1, 1), (1, 2, None), (2, 1, 1)],
        },
    )
    assert g.D({r("person", 1, 1)}) == {r("person", 1, 1), r("assignment", 1, 1)}
    assert g.D({r("lab", 1)}) == {
        r("lab", 1),
        r("person", 1, 1),
        r("person", 1, 2),
        r("assignment", 1, 1),
        r("assignment", 1, 2),
    }
    assert g.U({r("assignment", 1, 2)}) == {r("assignment", 1, 2), r("lab", 1)}
    assert g.U({r("assignment", 2, 1)}) == {r("assignment", 2, 1), r("person", 2, 1), r("lab", 2)}


def test_S4_renamed_fk():
    """litter.mother_id references animal.animal_id. Litter 1 has mother 2, so animal 1 does not reach it."""
    schema = Schema(
        [TableSpec("animal", ("animal_id",)), TableSpec("litter", ("litter_id",), ("mother_id",))],
        [FK("litter", "animal", {"mother_id": "animal_id"})],
    )
    g = graph(schema, {"animal": [(1,), (2,)], "litter": [(1, 2), (2, 1), (3, 1)]})
    assert g.D({r("animal", 1)}) == {r("animal", 1), r("litter", 2), r("litter", 3)}
    assert g.U({r("litter", 1)}) == {r("litter", 1), r("animal", 2)}


def test_S5_parallel_fks_plain_and_renamed():
    """
    synapse references neuron twice: neuron_id (plain) and pre_id (renamed).

    The two FKs are separate edges and combine with OR: neuron 2 reaches
    synapse (1, 2) through pre_id and synapse (2, 3) through neuron_id.
    """
    schema = Schema(
        [TableSpec("neuron", ("neuron_id",)), TableSpec("synapse", ("neuron_id", "pre_id"))],
        [FK("synapse", "neuron", {"neuron_id": "neuron_id"}), FK("synapse", "neuron", {"pre_id": "neuron_id"})],
    )
    g = graph(schema, {"neuron": [(1,), (2,), (3,)], "synapse": [(1, 2), (2, 3), (3, 3)]})
    assert g.D({r("neuron", 2)}) == {r("neuron", 2), r("synapse", 1, 2), r("synapse", 2, 3)}
    assert g.D({r("neuron", 1)}) == {r("neuron", 1), r("synapse", 1, 2)}
    assert g.U({r("synapse", 1, 2)}) == {r("synapse", 1, 2), r("neuron", 1), r("neuron", 2)}
    assert g.U({r("synapse", 3, 3)}) == {r("synapse", 3, 3), r("neuron", 3)}


def test_S6_diamond_with_a_shared_key():
    """recording references session and implant, which share subject_id."""
    schema = Schema(
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
    )
    g = graph(
        schema,
        {
            "subject": [(1,), (2,)],
            "session": [(1, 1), (2, 1)],
            "implant": [(1, 1), (1, 2), (2, 1)],
            "recording": [(1, 1, 1), (1, 1, 2), (2, 1, 1)],
        },
    )
    assert g.D({r("implant", 1, 2)}) == {r("implant", 1, 2), r("recording", 1, 1, 2)}
    assert g.D({r("subject", 2)}) == {
        r("subject", 2),
        r("session", 2, 1),
        r("implant", 2, 1),
        r("recording", 2, 1, 1),
    }
    assert g.U({r("recording", 1, 1, 2)}) == {
        r("recording", 1, 1, 2),
        r("session", 1, 1),
        r("implant", 1, 2),
        r("subject", 1),
    }
    # both adds the other parent of the descendant (session (1, 1)) and its ancestors.
    assert g.both({r("implant", 1, 2)}) == {
        r("implant", 1, 2),
        r("recording", 1, 1, 2),
        r("session", 1, 1),
        r("subject", 1),
    }


def test_S7_diamond_reached_through_a_secondary_fk():
    """
    D references B in its primary key and C as a secondary FK; C references B.

    D (2, 1) has b_id = 2 but references C 10, which references B 1, so deleting
    B 1 must delete it (the Phase 0 diamond: copying ``b_id = 1`` down to D
    missed it). D (1, 2) is reached through its own b_id.
    """
    schema = Schema(
        [
            TableSpec("b", ("b_id",)),
            TableSpec("c", ("c_id",), ("b_id",)),
            TableSpec("d", ("b_id", "d_id"), ("c_id",)),
        ],
        [FK("c", "b", {"b_id": "b_id"}), FK("d", "b", {"b_id": "b_id"}), FK("d", "c", {"c_id": "c_id"})],
    )
    g = graph(
        schema,
        {"b": [(1,), (2,)], "c": [(10, 1), (11, 2)], "d": [(2, 1, 10), (1, 2, 11), (2, 3, 11)]},
    )
    assert g.D({r("b", 1)}) == {r("b", 1), r("c", 10), r("d", 2, 1), r("d", 1, 2)}
    assert g.U({r("d", 2, 1)}) == {r("d", 2, 1), r("b", 2), r("c", 10), r("b", 1)}


# =============================================================================
# G: master-part groups
# =============================================================================


def g1_schema():
    """session is a master; its part trial references a stimulus."""
    return Schema(
        [
            TableSpec("stimulus", ("stim_id",)),
            TableSpec("session", ("session_id",)),
            TableSpec("session__trial", ("session_id", "trial_id"), ("stim_id",), master="session"),
        ],
        [
            FK("session__trial", "session", {"session_id": "session_id"}),
            FK("session__trial", "stimulus", {"stim_id": "stim_id"}),
        ],
    )


G1_DATA = {
    "stimulus": [(1,), (2,)],
    "session": [(1,), (2,), (3,)],  # session 3 has no trials
    "session__trial": [(1, 1, 1), (1, 2, 2), (2, 1, 2)],
}


def test_G1_part_to_master_lift():
    """
    A part row lifts its master, and the master brings its other parts.

    Stimulus 1 reaches trial (1, 1), which lifts session 1 and with it trial
    (1, 2). Sessions 2 and 3 are untouched; session 3 has no part rows at all.
    """
    g = graph(g1_schema(), G1_DATA)
    assert g.D({r("stimulus", 1)}) == {
        r("stimulus", 1),
        r("session__trial", 1, 1),
        r("session", 1),
        r("session__trial", 1, 2),
    }
    # Without the group rule the set is not lift-closed, so "enforce" raises.
    d1 = g.D1({r("stimulus", 1)})
    assert d1 == {r("stimulus", 1), r("session__trial", 1, 1)}
    assert not g.lift_closed(d1)
    assert g.D({r("session", 3)}) == {r("session", 3)}
    # trace includes the master's other parts and their parents (#1481).
    assert g.U({r("session__trial", 1, 1)}) == {
        r("session__trial", 1, 1),
        r("session", 1),
        r("session__trial", 1, 2),
        r("stimulus", 1),
        r("stimulus", 2),
    }
    assert g.U({r("session__trial", 2, 1)}) == {r("session__trial", 2, 1), r("session", 2), r("stimulus", 2)}


def test_G2_part_of_part_with_two_chains():
    """
    Part P references its master directly and through sibling Q (the #1525 shape).

    Ext 1 reaches Q (1, 10) and P (1, 100); both lift master 1. Master 2 is untouched.
    """
    schema = Schema(
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
    )
    g = graph(
        schema,
        {
            "ext": [(1,), (2,)],
            "master": [(1,), (2,)],
            "master__q": [(1, 10, 1), (2, 20, 2)],
            "master__p": [(1, 100, 10, 1), (2, 200, 20, 2)],
        },
    )
    assert g.D({r("ext", 1)}) == {r("ext", 1), r("master__q", 1, 10), r("master__p", 1, 100), r("master", 1)}
    assert g.D({r("master__q", 1, 10)}) == {r("master__q", 1, 10), r("master__p", 1, 100), r("master", 1)}
    assert g.U({r("master__p", 1, 100)}) == {r("master__p", 1, 100), r("master", 1), r("master__q", 1, 10), r("ext", 1)}


def test_G3_part_with_second_renamed_fk_to_its_master():
    """
    The lift follows the owning key, never the renamed second FK.

    cage__move references its master twice: cage_id (owning) and dest_cage
    (renamed). Move (1, 1) belongs to cage 1 and points to cage 2. Seeded alone,
    it lifts cage 1, not cage 2. Seeding cage 2 reaches move (1, 1) as a child
    through dest_cage, which then lifts cage 1.
    """
    schema = Schema(
        [
            TableSpec("cage", ("cage_id",)),
            TableSpec("cage__move", ("cage_id", "move_id"), ("dest_cage",), master="cage"),
        ],
        [FK("cage__move", "cage", {"cage_id": "cage_id"}), FK("cage__move", "cage", {"dest_cage": "cage_id"})],
    )
    g = graph(schema, {"cage": [(1,), (2,), (3,)], "cage__move": [(1, 1, 2), (2, 1, 3), (3, 1, 3)]})
    assert g.D({r("cage__move", 1, 1)}) == {r("cage__move", 1, 1), r("cage", 1)}
    assert g.D({r("cage", 2)}) == {r("cage", 2), r("cage__move", 2, 1), r("cage__move", 1, 1), r("cage", 1)}
    assert g.U({r("cage__move", 1, 1)}) == {
        r("cage__move", 1, 1),
        r("cage", 1),
        r("cage", 2),
        r("cage__move", 2, 1),
        r("cage", 3),
        r("cage__move", 3, 1),
    }


def test_G4_sibling_fk_renaming_the_master_key():
    """
    A sibling FK that renames the master key links different instances of the group.

    A transfer recorded in cohort 2 refers to an animal of cohort 1 (from_cohort),
    and one in cohort 3 to an animal of cohort 2: a chain of three instances.
    This is the case where the engine iterates the group step.
    """
    schema = Schema(
        [
            TableSpec("cohort", ("cohort_id",)),
            TableSpec("cohort__animal", ("cohort_id", "animal_id"), master="cohort"),
            TableSpec("cohort__transfer", ("cohort_id", "transfer_id"), ("from_cohort", "animal_id"), master="cohort"),
        ],
        [
            FK("cohort__animal", "cohort", {"cohort_id": "cohort_id"}),
            FK("cohort__transfer", "cohort", {"cohort_id": "cohort_id"}),
            FK("cohort__transfer", "cohort__animal", {"from_cohort": "cohort_id", "animal_id": "animal_id"}),
        ],
    )
    g = graph(
        schema,
        {
            "cohort": [(1,), (2,), (3,)],
            "cohort__animal": [(1, 1), (2, 1), (3, 1)],
            "cohort__transfer": [(2, 1, 1, 1), (3, 1, 2, 1)],
        },
    )
    everything = {
        r("cohort", 1),
        r("cohort", 2),
        r("cohort", 3),
        r("cohort__animal", 1, 1),
        r("cohort__animal", 2, 1),
        r("cohort__animal", 3, 1),
        r("cohort__transfer", 2, 1),
        r("cohort__transfer", 3, 1),
    }
    assert g.D({r("cohort", 1)}) == everything
    assert g.D({r("cohort", 2)}) == {
        r("cohort", 2),
        r("cohort__animal", 2, 1),
        r("cohort__transfer", 2, 1),
        r("cohort", 3),
        r("cohort__animal", 3, 1),
        r("cohort__transfer", 3, 1),
    }
    assert g.D({r("cohort", 3)}) == {r("cohort", 3), r("cohort__animal", 3, 1), r("cohort__transfer", 3, 1)}
    d1 = g.D1({r("cohort", 1)})
    assert d1 == {r("cohort", 1), r("cohort__animal", 1, 1), r("cohort__transfer", 2, 1)}
    assert not g.lift_closed(d1)
    assert g.U({r("cohort", 1)}) == {r("cohort", 1), r("cohort__animal", 1, 1)}
    assert g.U({r("cohort__transfer", 3, 1)}) == everything


def test_G5_merge_table():
    """
    Each part of a merge table references a different upstream table.

    The upstream of an analysis resolves to the one source that fed its merge entry.
    """
    schema = Schema(
        [
            TableSpec("src_a", ("a_id",)),
            TableSpec("src_b", ("b_id",)),
            TableSpec("merge", ("merge_id",)),
            TableSpec("merge__a", ("merge_id",), ("a_id",), master="merge"),
            TableSpec("merge__b", ("merge_id",), ("b_id",), master="merge"),
            TableSpec("analysis", ("merge_id",)),
        ],
        [
            FK("merge__a", "merge", {"merge_id": "merge_id"}),
            FK("merge__a", "src_a", {"a_id": "a_id"}),
            FK("merge__b", "merge", {"merge_id": "merge_id"}),
            FK("merge__b", "src_b", {"b_id": "b_id"}),
            FK("analysis", "merge", {"merge_id": "merge_id"}),
        ],
    )
    g = graph(
        schema,
        {
            "src_a": [(1,), (2,)],
            "src_b": [(1,)],
            "merge": [(1,), (2,), (3,)],
            "merge__a": [(1, 1), (2, 2)],
            "merge__b": [(3, 1)],
            "analysis": [(1,), (3,)],
        },
    )
    assert g.D({r("src_a", 1)}) == {r("src_a", 1), r("merge__a", 1), r("merge", 1), r("analysis", 1)}
    assert g.U({r("analysis", 3)}) == {r("analysis", 3), r("merge", 3), r("merge__b", 3), r("src_b", 1)}
    assert g.U({r("analysis", 1)}) == {r("analysis", 1), r("merge", 1), r("merge__a", 1), r("src_a", 1)}


def test_G6_part_violating_a1():
    """
    m__note refers to its master through a renamed FK, so it violates A1.

    It is traversed as a plain table: no lift from it, and it is not part of the
    master's instance.
    """
    schema = Schema(
        [
            TableSpec("m", ("m_id",)),
            TableSpec("m__item", ("m_id", "item_id"), master="m"),
            TableSpec("m__note", ("note_id",), ("about_m",), master="m"),
        ],
        [FK("m__item", "m", {"m_id": "m_id"}), FK("m__note", "m", {"about_m": "m_id"})],
    )
    assert schema.a1_parts == {"m": ("m__item",)}
    g = graph(schema, {"m": [(1,), (2,)], "m__item": [(1, 1), (2, 1)], "m__note": [(1, 1), (2, 2)]})
    assert g.D({r("m__note", 1)}) == {r("m__note", 1)}
    assert g.D({r("m", 1)}) == {r("m", 1), r("m__item", 1, 1), r("m__note", 1)}
    assert g.U({r("m__note", 1)}) == {r("m__note", 1), r("m", 1), r("m__item", 1, 1)}
    assert g.U({r("m__item", 1, 1)}) == {r("m", 1), r("m__item", 1, 1)}


# =============================================================================
# K: seeds
# =============================================================================


def test_K1_seed_restricted_by_a_descendant():
    """
    ``subject & (session & "quality = 'bad'")``: the seed rows are named by a descendant (#1496).

    The seed itself must be deleted along with its sessions; the engine used to
    leave it, because its restriction read sessions already deleted.
    """
    schema = Schema(
        [TableSpec("subject", ("subject_id",)), TableSpec("session", ("subject_id", "session_id"), ("quality",))],
        [FK("session", "subject", {"subject_id": "subject_id"})],
    )
    db = make_db(
        schema,
        {"subject": [(1,), (2,)], "session": [(1, 1, "good"), (1, 2, "bad"), (2, 1, "good")]},
    )
    g = RowGraph(schema, db)
    seed = {r("subject", 1)}  # the subjects with a bad session
    deleted = g.D(seed)
    assert deleted == {r("subject", 1), r("session", 1, 1), r("session", 1, 2)}
    remaining = apply_delete(db, deleted)
    assert remaining == {"subject": {(2,): {"subject_id": 2}}, "session": {(2, 1): db["session"][(2, 1)]}}


def test_K2_self_referencing_table():
    """
    employee.manager_id references employee.emp_id (nullable): rows form chains.

    The model follows the chain in both directions. DataJoint's dependency graph
    must be acyclic, so this table cannot be declared: Q has a self-loop and
    ``topo_sort`` raises.
    """
    schema = Schema(
        [TableSpec("employee", ("emp_id",), ("manager_id",))],
        [FK("employee", "employee", {"manager_id": "emp_id"}, nullable=True)],
    )
    g = graph(schema, {"employee": [(1, None), (2, 1), (3, 2), (4, 1)]})
    assert g.D({r("employee", 1)}) == {r("employee", 1), r("employee", 2), r("employee", 3), r("employee", 4)}
    assert g.D({r("employee", 2)}) == {r("employee", 2), r("employee", 3)}
    assert g.U({r("employee", 3)}) == {r("employee", 3), r("employee", 2), r("employee", 1)}
    assert g.U({r("employee", 4)}) == {r("employee", 4), r("employee", 1)}
    assert not q_acyclic(schema)
    with pytest.raises(nx.NetworkXUnfeasible):
        dependency_graph(schema).topo_sort()


def test_K5_empty_seed():
    """An empty seed (``False``, ``[]``, a condition that matches nothing) expands to nothing."""
    g = graph(g1_schema(), G1_DATA)
    assert g.D(set()) == set()
    assert g.D1(set()) == set()
    assert g.U(set()) == set()
    assert g.both(set()) == set()


def test_K6_unrestricted_seed():
    """
    Every stimulus row as the seed.

    Both stimuli reach all trials, which lift sessions 1 and 2. Session 3 has no
    trials and is untouched.
    """
    g = graph(g1_schema(), G1_DATA)
    assert g.D({r("stimulus", 1), r("stimulus", 2)}) == {
        r("stimulus", 1),
        r("stimulus", 2),
        r("session__trial", 1, 1),
        r("session__trial", 1, 2),
        r("session__trial", 2, 1),
        r("session", 1),
        r("session", 2),
    }
    assert g.U({r("session__trial", 1, 1), r("session__trial", 1, 2), r("session__trial", 2, 1)}) == {
        r("session__trial", 1, 1),
        r("session__trial", 1, 2),
        r("session__trial", 2, 1),
        r("session", 1),
        r("session", 2),
        r("stimulus", 1),
        r("stimulus", 2),
    }
