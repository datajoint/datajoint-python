"""
Unit tests for the master-part group cycle rule, on hand-built dependency graphs.

A master and its parts form one group. ``topo_sort`` orders a master after
every table its parts reference, so it fails when the groups depend on each
other in a cycle. ``group_cycle`` finds the cycle a new part's references would
close, and the declaration of such a part is refused.
"""

import random

import networkx as nx
import pytest

from datajoint.dependencies import group_ancestors, group_cycle, topo_sort
from datajoint.errors import DataJointError


def t(name):
    """Full table name in schema `s`."""
    return f"`s`.`{name}`"


def graph(*edges):
    """Dependency graph from (parent, child) pairs of short table names."""
    g = nx.MultiDiGraph()
    g.add_edges_from((t(parent), t(child)) for parent, child in edges)
    return g


# ---------------------------------------------------------------------------
# group_ancestors
# ---------------------------------------------------------------------------


def test_group_ancestors_plain_chain():
    g = graph(("a", "b"), ("b", "c"))
    assert group_ancestors(g, t("c")) == {t("a"), t("b"), t("c")}
    assert group_ancestors(g, t("a")) == {t("a")}


def test_group_ancestors_master_brings_its_parts_and_their_parents():
    # x is referenced by a part of m, so everything downstream of m depends on x.
    g = graph(("m", "m__p"), ("x", "m__p"), ("m", "y"))
    assert group_ancestors(g, t("y")) == {t("y"), t("m"), t("m__p"), t("x")}


def test_group_ancestors_part_reaches_master_through_its_reference():
    g = graph(("m", "m__p"), ("x", "m__q"), ("m", "m__q"))
    # m__p references m, and m brings its other part m__q and its parent x.
    assert group_ancestors(g, t("m__p")) == {t("m__p"), t("m"), t("m__q"), t("x")}


def test_group_ancestors_part_without_master_reference_is_plain():
    # m__p does not reference m: reaching it does not reach its master.
    g = graph(("x", "m__p"), ("y", "m"))
    g.add_node(t("m"))
    assert group_ancestors(g, t("m__p")) == {t("m__p"), t("x")}


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
    # m -> y -> n__q, a part of n; n -> z. A part of m referencing z closes a cycle.
    g = graph(("m", "y"), ("y", "n__q"), ("n", "n__q"), ("n", "z"))
    assert group_cycle(g, t("m__p"), [t("m"), t("z")]) == [t("m"), t("y"), t("n__q"), t("n"), t("z"), t("m__p"), t("m")]


@pytest.mark.parametrize(
    "parents",
    [
        ["m"],  # its master
        ["m", "m__s"],  # a sibling part
        ["m", "u"],  # an unrelated upstream table
        ["m", "y"],  # a table downstream of another group
        ["m__s"],  # a sibling alone
    ],
)
def test_group_cycle_allowed(parents):
    g = graph(("m", "m__s"), ("u", "m"), ("n", "n__q"), ("n", "y"), ("u", "y"))
    assert group_cycle(g, t("m__p"), [t(p) for p in parents]) is None


def test_group_cycle_ignores_parents_not_in_graph():
    g = graph(("m", "x"))
    assert group_cycle(g, t("m__p"), [t("m"), t("elsewhere")]) is None


def test_group_cycle_master_not_in_graph():
    g = graph(("a", "b"))
    assert group_cycle(g, t("m__p"), [t("b")]) is None


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


def test_topo_sort_nodes_keep_parts_after_masters():
    g = graph(("a", "m"), ("a", "z"), ("m", "m__part"), ("z", "m__part"))
    assert topo_sort(g, [t("m"), t("m__part"), t("z")]) == [t("z"), t("m"), t("m__part")]


# ---------------------------------------------------------------------------
# Exactness: refusing what group_cycle finds keeps topo_sort working, and
# refuses nothing topo_sort could sort.
# ---------------------------------------------------------------------------


def sorts(g):
    try:
        topo_sort(g)
    except DataJointError:
        return False
    return True


def test_group_cycle_is_exact():
    """
    Declare random tables in order, each referencing random earlier tables.

    A new part is refused exactly when adding it would make topo_sort fail. No
    other declaration ever makes topo_sort fail: not a plain table, not a
    master, and not a master declared after a part of it that does not
    reference it (as a part left behind by a dropped master would be).
    """
    counts = {"refused": 0, "accepted": 0, "late masters": 0}
    for seed in range(300):
        rng = random.Random(seed)
        g = nx.MultiDiGraph()
        masters, orphans = [], []
        for i in range(14):
            existing = list(g)
            kind = rng.choice(["plain", "master", "part", "part", "part"])
            if kind == "part" and masters and rng.random() < 0.9:
                master = rng.choice(masters)
            elif kind == "part":
                master = t(f"m{i}")  # its master is not declared yet
                orphans.append(master)
            if kind == "part":
                table = t(f"{master[5:-1]}__p{i}")
            elif kind == "master" and orphans and rng.random() < 0.5:
                table = orphans.pop()
                counts["late masters"] += 1
            else:
                table = t(f"{'m' if kind == 'master' else 't'}{i}")
            if table in g:
                continue
            parents = rng.sample(existing, k=min(len(existing), rng.randint(0, 3)))
            if kind == "part" and master in g and rng.random() < 0.8:
                parents.append(master)  # `-> master`, usually

            candidate = nx.MultiDiGraph(g)
            candidate.add_node(table)
            candidate.add_edges_from((parent, table) for parent in parents)

            if kind == "part":
                cycle = group_cycle(g, table, parents)
                assert (cycle is None) == sorts(candidate), (seed, table, parents, cycle)
                if cycle is not None:
                    counts["refused"] += 1
                    continue
                counts["accepted"] += 1
            else:
                assert sorts(candidate), (seed, table, parents)
                if kind == "master":
                    masters.append(table)
            g = candidate
        assert sorts(g)
    # the random declarations exercise every case
    assert all(counts.values()), counts
