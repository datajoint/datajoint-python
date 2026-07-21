import datajoint as dj
from datajoint.declare import declare

from tests.schema_advanced import (
    Cell,  # noqa: F401 - needed in globals for foreign key resolution
    GlobalSynapse,
    LocalSynapse,
    Parent,
    Person,
)


def test_aliased_fk(schema_adv):
    person = Person()
    parent = Parent()
    person.delete()
    assert not person
    assert not parent
    person.fill()
    parent.fill()
    assert person
    assert parent
    link = person.proj(parent_name="full_name", parent="person_id")
    parents = person * parent * link
    parents &= dict(full_name="May K. Hall")
    assert set(parents.to_arrays("parent_name")) == {"Hanna R. Walters", "Russel S. James"}
    delete_count = person.delete()
    assert delete_count == 16


def test_describe(schema_adv):
    """real_definition should match original definition"""
    for rel in (LocalSynapse, GlobalSynapse):
        describe = rel.describe()
        adapter = rel.connection.adapter
        s1 = declare(rel.full_table_name, rel.definition, schema_adv.context, adapter)[0].split("\n")
        s2 = declare(rel.full_table_name, describe, globals(), adapter)[0].split("\n")
        for c1, c2 in zip(s1, s2):
            assert c1 == c2


def test_delete(schema_adv):
    person = Person()
    parent = Parent()
    person.delete()
    assert not person
    assert not parent
    person.fill()
    parent.fill()
    assert parent
    original_len = len(parent)
    to_delete = len(parent & "11 in (person_id, parent)")
    (person & "person_id=11").delete()
    assert to_delete and len(parent) == original_len - to_delete


def test_parallel_edges_to_same_parent(schema_adv):
    """Two foreign keys from one child to the same parent are preserved as two
    distinct parallel edges (``nx.MultiDiGraph`` migration, #1492 / PR #1508).

    ``LocalSynapse`` references ``Cell`` twice —
    ``-> Cell.proj(presynaptic='cell')`` and ``-> Cell.proj(postsynaptic='cell')``
    — so ``Cell`` must appear as TWO parent edges of ``LocalSynapse``. The
    pre-migration name-keyed dependency dict collapsed these into a single entry;
    this test locks the parallel-edges capability across all three layers that
    consume it: the ``Dependencies`` accessors, the dependency graph itself, and
    the rendered ``Diagram``.
    """
    cell_name = Cell.full_table_name
    local_name = LocalSynapse.full_table_name

    # (a) Table.parents() surfaces BOTH edges to Cell, with distinct rename maps.
    cell_edges = [props for name, props in LocalSynapse().parents(foreign_key_info=True) if name == cell_name]
    assert len(cell_edges) == 2, "LocalSynapse must expose two parallel FK edges to Cell"
    renamed = {child for props in cell_edges for child, parent in props["attr_map"].items() if parent == "cell"}
    assert renamed == {"presynaptic", "postsynaptic"}
    # the two edges must carry distinct keys (``tuple(attr_map)``) — the uniqueness
    # the keyed-edge migration relies on to keep them apart.
    assert len({tuple(sorted(props["attr_map"].items())) for props in cell_edges}) == 2

    # (b) The dependency graph keeps both edges — cascade/trace traverse per edge.
    deps = LocalSynapse.connection.dependencies
    deps.load(force=False)
    assert deps.number_of_edges(cell_name, local_name) == 2
    # symmetric view from the parent side (children direction)
    local_as_child = [name for name, _ in Cell().children(foreign_key_info=True) if name == local_name]
    assert len(local_as_child) == 2

    # (c) The rendered Diagram (a MultiDiGraph over full table names) draws both.
    diagram = dj.Diagram(schema_adv)
    assert diagram.number_of_edges(cell_name, local_name) == 2
