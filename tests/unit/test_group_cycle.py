"""
Unit tests for the master-part group cycle rule, on hand-built dependency graphs.

A part is owned by its master when a foreign key in its primary key references
the master, or another owned part of the same master. An owned part and its
master form one group; every other part is a separate table. ``topo_sort``
orders a master after every table its owned parts reference, so it fails
exactly when the groups depend on each other in a cycle. ``group_cycle`` finds
the cycle a new owned part's references would close, and the declaration of
such a part is refused.
"""

import random

import networkx as nx
import pytest

from datajoint.dependencies import extract_master, group_ancestors, group_cycle, is_owned, owned_parts, topo_sort
from datajoint.errors import DataJointError


def t(name):
    """Full table name in schema `s`."""
    return f"`s`.`{name}`"


def graph(*edges):
    """
    Dependency graph from (parent, child) pairs of short table names.

    A third element False marks a reference that is not in the child's primary key.
    """
    g = nx.MultiDiGraph()
    for parent, child, *primary in edges:
        g.add_edge(t(parent), t(child), primary=primary[0] if primary else True)
    return g


def sorts(g):
    try:
        topo_sort(g)
    except DataJointError:
        return False
    return True


# ---------------------------------------------------------------------------
# owned_parts
# ---------------------------------------------------------------------------


def test_owned_parts():
    g = graph(
        ("m", "m__direct"),  # references the master in its primary key
        ("m__direct", "m__chained"),  # through an owned part (a rename is still one edge)
        ("m", "m__secondary", False),  # only a secondary or nullable reference
        ("m__secondary", "m__via_plain"),  # only through a part that is not owned
        ("x", "m__none"),  # no reference to the master
        ("m", "m__parallel", False),  # parallel references: one of them primary
        ("m", "m__parallel"),
        ("y", "n__orphan"),  # its master is not in the graph
    )
    assert owned_parts(g) == {t("m__direct"), t("m__chained"), t("m__parallel")}


def test_owned_parts_edge_without_flag_counts_as_primary():
    g = nx.MultiDiGraph([(t("m"), t("m__p"))])
    assert owned_parts(g) == {t("m__p")}


def test_owned_parts_does_not_cross_into_another_master():
    # n__q references m__p in its primary key, but m__p belongs to another master.
    g = graph(("m", "m__p"), ("m__p", "n__q"))
    g.add_node(t("n"))
    assert owned_parts(g) == {t("m__p")}


def test_is_owned():
    g = graph(("m", "m__p"), ("m", "m__s", False))
    assert is_owned(g, t("m__new"), [(t("m"), True)])
    assert is_owned(g, t("m__new"), [(t("m__p"), True)])
    assert not is_owned(g, t("m__new"), [(t("m"), False)])
    assert not is_owned(g, t("m__new"), [(t("m__s"), True)])
    assert not is_owned(g, t("m__new"), [])
    assert not is_owned(g, t("z__new"), [(t("m"), True)])


# ---------------------------------------------------------------------------
# group_ancestors
# ---------------------------------------------------------------------------


def test_group_ancestors_plain_chain():
    g = graph(("a", "b"), ("b", "c"))
    assert group_ancestors(g, t("c")) == {t("a"), t("b"), t("c")}
    assert group_ancestors(g, t("a")) == {t("a")}


def test_group_ancestors_master_brings_its_owned_parts_and_their_parents():
    # x is referenced by an owned part of m, so everything downstream of m depends on x.
    g = graph(("m", "m__p"), ("x", "m__p"), ("m", "y"))
    assert group_ancestors(g, t("y")) == {t("y"), t("m"), t("m__p"), t("x")}


def test_group_ancestors_owned_part_reaches_master_through_its_reference():
    g = graph(("m", "m__p"), ("x", "m__q"), ("m", "m__q"))
    assert group_ancestors(g, t("m__p")) == {t("m__p"), t("m"), t("m__q"), t("x")}


def test_group_ancestors_part_that_is_not_owned_is_plain():
    # m__p references m only outside its primary key: m does not bring it in.
    g = graph(("m", "m__p", False), ("x", "m__p"), ("m", "y"))
    assert group_ancestors(g, t("y")) == {t("y"), t("m")}
    assert group_ancestors(g, t("m__p")) == {t("m__p"), t("m"), t("x")}


def test_group_ancestors_across_two_groups():
    g = graph(("b", "a__p"), ("a", "a__p"), ("b", "b__p"), ("z", "b__p"))
    assert group_ancestors(g, t("a")) == {t("a"), t("a__p"), t("b"), t("b__p"), t("z")}


# ---------------------------------------------------------------------------
# group_cycle
# ---------------------------------------------------------------------------


def test_group_cycle_master_x_part():
    g = graph(("m", "x"))
    assert group_cycle(g, t("m__p"), [t("m"), t("x")]) == [t("m"), t("x"), t("m__p"), t("m")]


def test_group_cycle_two_groups():
    # a__p references b; a part of b that references a closes the cycle.
    g = graph(("a", "a__p"), ("b", "a__p"), ("b", "b__p"))
    assert group_cycle(g, t("b__q"), [t("b"), t("a")]) == [t("b"), t("a__p"), t("a"), t("b__q"), t("b")]
    # so does a reference to the other group's part
    assert group_cycle(g, t("b__q"), [t("b"), t("a__p")]) == [t("b"), t("a__p"), t("b__q"), t("b")]


def test_group_cycle_through_a_master_downstream():
    # m -> y -> n__q, an owned part of n; n -> z. A part of m referencing z closes a cycle.
    g = graph(("m", "y"), ("y", "n__q"), ("n", "n__q"), ("n", "z"))
    assert group_cycle(g, t("m__p"), [t("m"), t("z")]) == [t("m"), t("y"), t("n__q"), t("n"), t("z"), t("m__p"), t("m")]


def test_group_cycle_not_through_a_part_that_is_not_owned():
    # Same shape, but n__q references n outside its primary key: it is a separate
    # table, n does not depend on y, and the reference to z closes no cycle.
    g = graph(("m", "y"), ("y", "n__q"), ("n", "n__q", False), ("n", "z"))
    assert group_cycle(g, t("m__p"), [t("m"), t("z")]) is None
    g.add_edge(t("z"), t("m__p"))
    g.add_edge(t("m"), t("m__p"))
    assert sorts(g)


@pytest.mark.parametrize(
    "parents",
    [
        ["m"],  # its master
        ["m", "m__s"],  # an owned sibling part
        ["m", "u"],  # an unrelated upstream table
        ["m", "y"],  # a table downstream of another group
        ["m__s"],  # an owned sibling alone
        ["m", "m__loose"],  # a sibling that is not owned, but does not depend on m
    ],
)
def test_group_cycle_allowed(parents):
    g = graph(("m", "m__s"), ("u", "m"), ("n", "n__q"), ("n", "y"), ("u", "y"), ("u", "m__loose"))
    assert group_cycle(g, t("m__p"), [t(p) for p in parents]) is None


def test_group_cycle_ignores_parents_not_in_graph():
    g = graph(("m", "x"))
    assert group_cycle(g, t("m__p"), [t("m"), t("elsewhere")]) is None


def test_group_cycle_master_not_in_graph():
    g = graph(("a", "b"))
    assert group_cycle(g, t("m__p"), [t("b")]) is None


# ---------------------------------------------------------------------------
# The two counterexamples to grouping parts by name
# ---------------------------------------------------------------------------


def test_part_without_master_reference_is_sorted_as_a_plain_table():
    """
    m; x references m; part m__p references x but not m.

    Grouped by name, m__p would make m come after x (a cycle), and keeping m__p
    next to m would put it before x. m__p is not owned, so it is a plain table.
    """
    g = graph(("m", "x"), ("x", "m__p"))
    assert topo_sort(g) == [t("m"), t("x"), t("m__p")]
    assert not is_owned(graph(("m", "x")), t("m__p"), [(t("x"), True)])


def test_owned_part_referencing_a_sibling_that_is_not_owned_closes_a_cycle():
    """
    m; m__p1 references m only through a nullable foreign key (not owned);
    m__p2 is owned and references m and m__p1.

    m__p1 is a separate table that depends on m, so m__p2's reference to it
    makes m's group depend on itself.
    """
    declared = graph(("m", "m__p1", False))
    references = [(t("m"), True), (t("m__p1"), True)]
    assert is_owned(declared, t("m__p2"), references)
    assert group_cycle(declared, t("m__p2"), [t("m"), t("m__p1")]) == [t("m"), t("m__p1"), t("m__p2"), t("m")]

    legacy = graph(("m", "m__p1", False), ("m", "m__p2"), ("m__p1", "m__p2"))
    with pytest.raises(DataJointError) as error:
        topo_sort(legacy)
    assert f"{t('m')} -> {t('m__p1')} -> {t('m__p2')} -> {t('m')}" in str(error.value)
    assert f"part table {t('m__p2')} references a table that depends on its own master" in str(error.value)


# ---------------------------------------------------------------------------
# topo_sort
# ---------------------------------------------------------------------------


def test_topo_sort_names_a_group_cycle():
    g = graph(("m", "x"), ("x", "m__p"), ("m", "m__p"))
    with pytest.raises(DataJointError, match="cycle") as error:
        topo_sort(g)
    message = str(error.value)
    assert f"{t('m')} -> {t('x')} -> {t('m__p')} -> {t('m')}" in message
    assert f"part table {t('m__p')} references a table that depends on its own master" in message


def test_topo_sort_names_both_parts_of_a_two_group_cycle():
    g = graph(("a", "a__p"), ("b", "a__p"), ("b", "b__p"), ("a", "b__p"))
    with pytest.raises(DataJointError, match="part tables .* each reference") as error:
        topo_sort(g)
    assert t("a__p") in str(error.value) and t("b__p") in str(error.value)


def test_topo_sort_names_a_plain_cycle():
    g = graph(("a", "b"), ("b", "a"))
    with pytest.raises(DataJointError, match="form a cycle") as error:
        topo_sort(g)
    assert "master-part" not in str(error.value)


def test_topo_sort_nodes_skip_a_cycle_elsewhere():
    g = graph(("m", "x"), ("x", "m__p"), ("m", "m__p"), ("u", "v"), ("v", "w"))
    assert topo_sort(g, [t("u"), t("v"), t("w")]) == [t("u"), t("v"), t("w")]
    assert topo_sort(g, [t("m__p")]) == [t("m__p")]
    with pytest.raises(DataJointError, match="cycle"):
        topo_sort(g, [t("m"), t("m__p")])


def test_topo_sort_nodes_keep_order_through_tables_left_out():
    # a -> z -> b: z is not requested, but a must still come before b.
    g = graph(("z", "b"), ("a", "z"))
    g.add_node(t("c"))
    assert topo_sort(g, [t("b"), t("a")]) == [t("a"), t("b")]
    assert topo_sort(g, [t("b"), t("a"), t("missing")]) == [t("a"), t("b")]
    assert topo_sort(g, []) == []


def test_topo_sort_nodes_keep_owned_parts_after_masters():
    g = graph(("a", "m"), ("a", "z"), ("m", "m__part"), ("z", "m__part"))
    assert topo_sort(g, [t("m"), t("m__part"), t("z")]) == [t("z"), t("m"), t("m__part")]


def test_topo_sort_nodes_judge_ownership_on_the_whole_graph():
    # m__q is owned through m__p; sorting m and m__q alone still keeps m__q with m.
    g = graph(("a", "m"), ("m", "m__p"), ("m__p", "m__q"), ("z", "m__q"), ("a", "z"))
    assert topo_sort(g, [t("m"), t("m__q"), t("z")]) == [t("z"), t("m"), t("m__q")]


# ---------------------------------------------------------------------------
# Randomized checks
# ---------------------------------------------------------------------------


def random_tables(rng, count):
    """
    Yield random table declarations in order: (name, kind, references).

    ``kind`` is "plain", "master" or "part"; ``references`` is a list of
    (parent, primary) pairs to tables declared earlier. A part references its
    master (if declared) in most cases, in its primary key or not. Some parts are
    declared before their master, as one left behind by a dropped master would be.
    """
    declared, masters, orphans = [], [], []
    for i in range(count):
        kind = rng.choice(["plain", "master", "part", "part", "part"])
        master = None
        if kind == "part" and masters and rng.random() < 0.9:
            master = rng.choice(masters)
        elif kind == "part":
            master = t(f"m{i}")  # its master is not declared yet
            orphans.append(master)
        if kind == "part":
            table = t(f"{master[5:-1]}__p{i}")
        elif kind == "master" and orphans and rng.random() < 0.5:
            table = orphans.pop()
        else:
            table = t(f"{'m' if kind == 'master' else 't'}{i}")
        parents = rng.sample(declared, k=min(len(declared), rng.randint(0, 3)))
        if master in declared and rng.random() < 0.8:
            parents.append(master)
        references = [(parent, rng.random() < 0.7) for parent in parents]
        if kind == "master":
            masters.append(table)
        declared.append(table)
        yield table, kind, references


def quotient_is_acyclic(g):
    """
    Reference check, independent of ``owned_parts``: contract each master with
    its owned parts and test the result for cycles.
    """
    owned, changed = set(), True
    while changed:  # fixed point over all edges
        changed = False
        for parent, child, primary in g.edges(data="primary", default=True):
            master = extract_master(child)
            if (
                child not in owned
                and master in g
                and primary
                and (parent == master or parent in owned and extract_master(parent) == master)
            ):
                owned.add(child)
                changed = True
    group = {node: extract_master(node) if node in owned else node for node in g.nodes}
    quotient = nx.DiGraph()
    quotient.add_nodes_from(set(group.values()))
    quotient.add_edges_from((group[u], group[v]) for u, v in g.edges() if group[u] != group[v])
    return nx.is_directed_acyclic_graph(quotient), owned


def test_topo_sort_succeeds_iff_the_group_quotient_is_acyclic():
    """
    On random dependency graphs, topo_sort succeeds exactly when the graph with
    each master and its owned parts contracted to one node has no cycle. When it
    succeeds, every table follows the tables it references, and each master is
    followed directly by its owned parts.
    """
    outcomes = {True: 0, False: 0}
    for seed in range(400):
        rng = random.Random(seed)
        g = nx.MultiDiGraph()
        for table, _, references in random_tables(rng, 12):
            g.add_node(table)
            for parent, primary in references:
                g.add_edge(parent, table, primary=primary)
        acyclic, owned = quotient_is_acyclic(g)
        assert sorts(g) == acyclic, seed
        outcomes[acyclic] += 1
        if acyclic:
            order = topo_sort(g)
            position = {node: i for i, node in enumerate(order)}
            assert sorted(order) == sorted(g.nodes)
            assert all(position[u] < position[v] for u, v in g.edges()), seed
            for part in owned:
                master = extract_master(part)
                between = order[position[master] + 1 : position[part]]
                assert all(node in owned and extract_master(node) == master for node in between), seed
    assert all(outcomes.values()), outcomes


def test_group_cycle_is_exact():
    """
    Declare random tables in order, each referencing random earlier tables.

    A new owned part is refused exactly when adding it would make topo_sort fail.
    No other declaration ever makes topo_sort fail: not a plain table, not a
    master, not a part that is not owned, and not a master declared after a part
    of its own.
    """
    counts = {"refused": 0, "owned": 0, "not owned": 0}
    for seed in range(300):
        rng = random.Random(seed)
        g = nx.MultiDiGraph()
        for table, kind, references in random_tables(rng, 14):
            if table in g or any(parent not in g for parent, _ in references):
                continue  # skip a table whose parent was refused
            candidate = nx.MultiDiGraph(g)
            candidate.add_node(table)
            for parent, primary in references:
                candidate.add_edge(parent, table, primary=primary)

            if kind == "part" and is_owned(g, table, references):
                cycle = group_cycle(g, table, [parent for parent, _ in references])
                assert (cycle is None) == sorts(candidate), (seed, table, references, cycle)
                if cycle is not None:
                    counts["refused"] += 1
                    continue
                counts["owned"] += 1
            else:
                assert sorts(candidate), (seed, table, references)
                counts["not owned"] += kind == "part"
            g = candidate
        assert sorts(g)
    # the random declarations exercise every case
    assert all(counts.values()), counts
