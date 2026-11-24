"""
Tests of the ``Diagram.expand`` specification, without a database.

The traversal specification (the "Traversal specification" section of
``diagram.md`` and the shape catalog ``traversal-shapes.md`` in datajoint-docs)
is tested in three layers that need no database server:

- ``expand_model.py`` is the reference model: an executable copy of the
  definitions. It holds rows in Python dictionaries and computes ``D``, ``D1``,
  ``U`` and ``both`` as plain closures over row identifiers.
- This module tests the model against the specification's derived results
  T1-T8, on random schemas and databases drawn by Hypothesis, and checks the
  specification's computation rules (the one pass over the group quotient, the
  groups that iterate, R1's copy rule, relabeling) against the definitions.
- ``test_expand_shapes_model.py`` mirrors the shape catalog one to one: each
  shape's schema, rows, seeds and the rows the catalog states, by hand.

Later layers run the engine against the same model on MySQL and PostgreSQL.

Running
-------
``pytest tests/unit/test_expand_spec.py tests/unit/test_expand_shapes_model.py``.
``HYPOTHESIS_PROFILE`` selects the number of examples (``tests/conftest.py``):

- ``dev`` (the default outside CI): 50 examples per property, for quick runs;
- ``ci`` (the default when ``CI`` is set): 500 examples, the same on every run;
- ``thorough``: 2000 random examples; failing examples are saved under the
  gitignored ``.hypothesis/`` directory and replayed first on the next run.

The derived results
-------------------
- T1 closure: D, U and D1 are extensive, monotone and idempotent.
- T2 additivity: ``X(A ∪ B) = X(A) ∪ X(B)`` for D, U, D1 and both.
- T3 duality: ``y ∈ D({x})`` if and only if ``x ∈ U({y})``.
- T4 delete safety: removing D(S) leaves a consistent database consistent, and
  D(S) is the least such superset of S.
- T5 read boundary: U(S) is consistent on its own and is the least such superset.
- T6 both: U(D(S)) contains D(S), is up-closed and group-closed, and is the least
  such superset; ``both`` is extensive and monotone but not idempotent.
- T7 enforce: D1(S) is lift-closed if and only if D1(S) = D(S).
- T8 ordering: ``datajoint.dependencies.topo_sort`` succeeds if and only if the
  group quotient Q is acyclic, and its order is valid.

Drawn schemas may contain group cycles (Q cyclic), which the declaration check
refuses (#1619). The row-level properties are stated for acyclic Q and discard
the other schemas with ``assume``; T8 tests both cases. A claim the engine does
not meet yet is kept as a strict ``xfail`` with a minimal example.
"""

from __future__ import annotations

import itertools

import networkx as nx
import pytest
from hypothesis import assume, find, given, settings
from hypothesis import strategies as st

from datajoint.dependencies import extract_master
from datajoint.errors import DataJointError

from .expand_model import (
    FK,
    PartIntegrityError,
    RowGraph,
    Schema,
    TableSpec,
    apply_delete,
    copy_allowed,
    dependency_graph,
    fk_value,
    full_table_name,
    group_whole,
    iterates,
    make_db,
    one_pass,
    preserves_instance,
    q_acyclic,
    r1_down,
    r1_up,
    relabel_down,
    relabel_down_allowed,
    relabel_up,
    restrict,
    ri_ok,
    subdatabase,
    virtual_upstream,
)
from .expand_strategies import Case, cases, conditions, key_dicts, schemas, seeds

#: Tiny databases for the brute-force minimality checks (at most 2^8 supersets).
tiny_cases = cases(max_tables=4, max_rows=2, max_total=8)


def supersets(rows, seed):
    """Every subset of ``rows`` that contains ``seed``."""
    rest = sorted(set(rows) - set(seed))
    for k in range(len(rest) + 1):
        for extra in itertools.combinations(rest, k):
            yield frozenset(seed) | frozenset(extra)


# =============================================================================
# The generators and the model
# =============================================================================


@given(case=cases(acyclic=False))
def test_generated_databases_are_consistent(case):
    """Every drawn database satisfies P1 and the static half of P2."""
    assert ri_ok(case.schema, case.db)
    assert group_whole(case.schema, case.db)


@given(case=cases(acyclic=False))
def test_up_is_the_converse_of_down(case):
    """``s ∈ down(r)`` if and only if ``r ∈ up(s)``."""
    g = case.graph
    for r in g.rows:
        for s in g.down(r):
            assert r in g.up(s)
        for s in g.up(r):
            assert r in g.down(s)


def _has_partial_null(case: Case) -> bool:
    for fk in case.schema.fks:
        for row in case.db[fk.child].values():
            values = [row[c] for c in fk.attr_map]
            if None in values and any(v is not None for v in values):
                return True
    return False


def _crosses_instances(case: Case) -> bool:
    # A part row references a row of its own group in another master instance.
    g = case.graph
    for r in g.rows:
        for p in g.parent(r):
            mr, mp = g.master(r), g.master(p)
            if mr and mp and mr[0] == mp[0] and mr != mp:
                return True
    return False


def _owned(s: Schema):
    return [t for t in s.tables if t.master and t.name in s.group]


def _leaves_instance(s: Schema, fk: FK) -> bool:
    master = s.group.get(fk.child)
    return master is not None and fk.child != master and fk in s.intra_group_fks(master) and not preserves_instance(s, fk)


SCHEMA_FEATURES = {
    "group cycle": lambda s: not q_acyclic(s),
    "part its master does not own": lambda s: any(t.master and t.name not in s.group for t in s.tables),
    "part owned through a sibling": lambda s: any(
        not any(fk.parent == t.master and s.is_primary(fk) for fk in s.fks_of(t.name)) for t in _owned(s)
    ),
    "renamed owning attributes": lambda s: any(s.owning_attributes(t.name) != s.table(t.master).pk for t in _owned(s)),
    "FK to the own master that leaves the instance": lambda s: any(
        _leaves_instance(s, fk) and fk.parent == s.group[fk.child] for fk in s.fks
    ),
    "FK to a sibling that leaves the instance": lambda s: any(
        _leaves_instance(s, fk) and fk.parent != s.group[fk.child] for fk in s.fks
    ),
    "part -> part across groups": lambda s: any(
        s.table(fk.child).master and s.table(fk.parent).master and s.table(fk.child).master != s.table(fk.parent).master
        for fk in s.fks
    ),
    "parallel FKs": lambda s: any(a.child == b.child and a.parent == b.parent for a, b in itertools.combinations(s.fks, 2)),
    "diamond sharing an attribute": lambda s: any(
        a.child == b.child and a.parent != b.parent and set(a.attr_map) & set(b.attr_map)
        for a, b in itertools.combinations(s.fks, 2)
    ),
    "nullable FK": lambda s: any(fk.nullable for fk in s.fks),
    "namesake attribute": lambda s: any(
        a in u.pk
        for t in s.tables
        for a in t.secondary
        if not any(a in fk.attr_map for fk in s.fks_of(t.name))
        for u in s.tables
        if u is not t
    ),
}

DATA_FEATURES = {
    "partial NULL": _has_partial_null,
    "reference across master instances": _crosses_instances,
}

_FIND_SETTINGS = settings(max_examples=5000, database=None, derandomize=True, deadline=None)


@pytest.mark.parametrize("feature", sorted(SCHEMA_FEATURES))
def test_generator_draws_schema_feature(feature):
    """The schema generator reaches every shape that the properties rely on."""
    find(schemas(), SCHEMA_FEATURES[feature], settings=_FIND_SETTINGS)


@pytest.mark.parametrize("feature", sorted(DATA_FEATURES))
def test_generator_draws_data_feature(feature):
    """The data generator reaches partial NULLs and references across master instances."""
    find(cases(acyclic=False), DATA_FEATURES[feature], settings=_FIND_SETTINGS)


# =============================================================================
# T1-T3
# =============================================================================


@given(case=cases(), data=st.data())
def test_t1_closure(case, data):
    """T1: D, U and D1 are extensive, monotone and idempotent."""
    g = case.graph
    b = data.draw(seeds(g.rows), label="B")
    a = data.draw(seeds(b), label="A ⊆ B")
    for op in (g.D, g.U, g.D1):
        assert a <= op(a)
        assert op(a) <= op(b)
        assert op(op(a)) == op(a)


@given(case=cases(), data=st.data())
def test_t2_additivity(case, data):
    """T2: X(A ∪ B) = X(A) ∪ X(B) for X in D, U, D1 and both."""
    g = case.graph
    a = data.draw(seeds(g.rows), label="A")
    b = data.draw(seeds(g.rows), label="B")
    for op in (g.D, g.U, g.D1, g.both):
        assert op(a | b) == op(a) | op(b)


@given(case=cases())
def test_t3_duality(case):
    """T3: y ∈ D({x}) if and only if x ∈ U({y})."""
    g = case.graph
    up = {y: g.U({y}) for y in g.rows}
    for x in g.rows:
        down = g.D({x})
        for y in g.rows:
            assert (y in down) == (x in up[y]), (x, y)


# =============================================================================
# T4-T5: delete safety and the read boundary
# =============================================================================


@given(case=cases(), data=st.data())
def test_t4_delete_safety(case, data):
    """T4: removing D(S) leaves the database consistent and takes every master-part instance whole."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    d = g.D(s)
    after = apply_delete(case.db, d)
    assert ri_ok(case.schema, after)
    assert group_whole(case.schema, after)
    assert g.group_closed(d)


@given(case=tiny_cases, data=st.data())
def test_t4_minimality(case, data):
    """T4: D(S) is the least superset of S whose removal is safe."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    safe = [x for x in supersets(g.rows, s) if ri_ok(case.schema, apply_delete(case.db, x)) and g.group_closed(x)]
    d = g.D(s)
    assert d in safe
    assert all(d <= x for x in safe)


@given(case=cases(), data=st.data())
def test_t5_read_boundary(case, data):
    """T5: U(S) is consistent on its own and holds every master-part instance it touches whole."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    u = g.U(s)
    export = subdatabase(case.db, u)
    assert ri_ok(case.schema, export)
    assert group_whole(case.schema, export)
    assert g.group_closed(u)


def _self_contained(case, rows) -> bool:
    return ri_ok(case.schema, subdatabase(case.db, rows)) and case.graph.group_closed(rows)


@given(case=tiny_cases, data=st.data())
def test_t5_minimality(case, data):
    """T5: U(S) is the least superset of S that is consistent on its own."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    consistent = [x for x in supersets(g.rows, s) if _self_contained(case, x)]
    u = g.U(s)
    assert u in consistent
    assert all(u <= x for x in consistent)


# =============================================================================
# T6-T7: both and enforce
# =============================================================================


@given(case=cases(), data=st.data())
def test_t6_both(case, data):
    """T6: U(D(S)) contains D(S), is up-closed and group-closed; both is extensive and monotone."""
    g = case.graph
    big = data.draw(seeds(g.rows), label="B")
    s = data.draw(seeds(big), label="S ⊆ B")
    b = g.both(s)
    assert g.D(s) <= b
    assert g.U(b) == b
    assert g.group_closed(b)
    assert b <= g.both(b)  # extensive, though not idempotent (test_t6_both_is_not_idempotent)
    assert b <= g.both(big)


@given(case=tiny_cases, data=st.data())
def test_t6_minimality(case, data):
    """T6: U(D(S)) is the least superset of D(S) that is consistent on its own."""
    g = case.graph
    d = g.D(data.draw(seeds(g.rows), label="S"))
    consistent = [x for x in supersets(g.rows, d) if _self_contained(case, x)]
    assert g.U(d) in consistent
    assert all(g.U(d) <= x for x in consistent)


def test_t6_both_is_not_idempotent():
    """T6: A -> B and A -> C with S = {b} give both(S) = {a, b}, then both(both(S)) = {a, b, c}."""
    schema = Schema(
        [TableSpec("a", ("a",)), TableSpec("b", ("b",), ("a",)), TableSpec("c", ("c",), ("a",))],
        [FK("b", "a", {"a": "a"}), FK("c", "a", {"a": "a"})],
    )
    db = make_db(schema, {"a": [(1,)], "b": [(1, 1)], "c": [(1, 1)]})
    g = RowGraph(schema, db)
    a, b, c = ("a", (1,)), ("b", (1,)), ("c", (1,))
    assert g.both({b}) == {a, b}
    assert g.both(g.both({b})) == {a, b, c}


@given(case=cases(), data=st.data())
def test_t7_enforce(case, data):
    """T7: D1(S) is lift-closed if and only if D1(S) = D(S); enforce deletes D1(S) or raises."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    d1, d = g.D1(s), g.D(s)
    assert d1 <= d
    assert g.lift_closed(d1) == (d1 == d)
    if d1 == d:
        assert g.delete(s, "enforce") == d
    else:
        with pytest.raises(PartIntegrityError):
            g.delete(s, "enforce")


# =============================================================================
# T8: topo_sort and the group quotient
# =============================================================================


def assert_valid_order(schema: Schema, order: list[str], quote: str = "`") -> None:
    """Every parent precedes its children, and each master is followed at once by its owned parts."""
    name = {t.name: full_table_name(t.name, quote) for t in schema.tables}
    assert sorted(order) == sorted(name.values())
    pos = {n: i for i, n in enumerate(order)}
    for fk in schema.fks:
        assert pos[name[fk.parent]] < pos[name[fk.child]], fk
    for master, parts in schema.owned_parts.items():
        block = order[pos[name[master]] : pos[name[master]] + 1 + len(parts)]
        assert set(block) == {name[master]} | {name[p] for p in parts}, (master, order)


@pytest.mark.parametrize("quote", ["`", '"'])
@given(schema=schemas(owned_only=True))
def test_t8_topo_sort(quote, schema):
    """
    T8: topo_sort succeeds if and only if Q is acyclic, and its order is valid.

    The graph is a ``Dependencies`` object filled the way ``Dependencies.load``
    fills it, and sorted by ``datajoint.dependencies.topo_sort``. Both MySQL and
    PostgreSQL quoting are used, since ``extract_master`` recognizes parts by name.
    Every part is owned here: ``topo_sort`` still groups a part with its master
    by name, owned or not (see the two examples below).
    """
    graph = dependency_graph(schema, quote)
    if q_acyclic(schema):
        assert_valid_order(schema, graph.topo_sort(), quote)
    else:
        # The declaration check (#1619) refuses these schemas; topo_sort raises on them.
        with pytest.raises((nx.NetworkXUnfeasible, DataJointError)):
            graph.topo_sort()


@given(schema=schemas())
def test_model_part_names_follow_datajoint(schema):
    """``extract_master`` finds the master of every part in the model, and of no other table."""
    for quote in ("`", '"'):
        for t in schema.tables:
            expected = full_table_name(t.master, quote) if t.master else None
            assert extract_master(full_table_name(t.name, quote)) == expected


TOPO_SORT_GROUPS_BY_NAME = (
    "engine vs docs: datajoint.dependencies.topo_sort groups every part with its master by name; "
    "diagram.md (A1, T8) makes a part its master does not own a plain table in topo_sort (#1619)"
)


@pytest.mark.xfail(strict=True, reason=TOPO_SORT_GROUPS_BY_NAME)
def test_t8_part_not_owned_q_acyclic():
    """
    Q is acyclic, so topo_sort must succeed.

    M; X references M; part P of M does not reference M, and references X. P is
    not owned, so it is a plain table and Q is ``M -> X -> P``. Today
    ``topo_sort`` treats P as a part and adds the edge ``X -> M``, a cycle.
    """
    schema = Schema(
        [TableSpec("m", ("m",)), TableSpec("x", ("x",), ("m",)), TableSpec("m__p", ("p",), ("x",), master="m")],
        [FK("x", "m", {"m": "m"}), FK("m__p", "x", {"x": "x"})],
    )
    assert schema.owned_parts == {"m": ()}
    assert q_acyclic(schema)
    assert_valid_order(schema, dependency_graph(schema).topo_sort())


@pytest.mark.xfail(strict=True, reason=TOPO_SORT_GROUPS_BY_NAME)
def test_t8_part_not_owned_q_cyclic():
    """
    Q has a cycle, so topo_sort must raise.

    M; part P1 references M only through a nullable FK, so it is not owned; part
    P2 is owned and references P1. Q is ``{M, P2} -> P1 -> {M, P2}``. Today
    ``topo_sort`` sees P1 as a part of M by name, adds no edge, and succeeds.
    The declaration check refuses P2's reference, since P1 depends on M.
    """
    schema = Schema(
        [
            TableSpec("m", ("m",)),
            TableSpec("m__p1", ("p1",), ("m",), master="m"),
            TableSpec("m__p2", ("m", "p2"), ("p1",), master="m"),
        ],
        [FK("m__p1", "m", {"m": "m"}, nullable=True), FK("m__p2", "m", {"m": "m"}), FK("m__p2", "m__p1", {"p1": "p1"})],
    )
    assert schema.owned_parts == {"m": ("m__p2",)}
    assert not q_acyclic(schema)
    with pytest.raises((nx.NetworkXUnfeasible, DataJointError)):
        dependency_graph(schema).topo_sort()


# =============================================================================
# The one pass over Q, and the groups that iterate (diagram.md, Computation)
# =============================================================================
# ``one_pass`` evaluates the engine's computation on rows: one visit per node of
# Q, the lift through the owning attributes, and the group step repeated for a
# group that iterates. It must give D(S) and U(S).


@pytest.mark.parametrize("direction", ["down", "up"])
@given(case=cases(), data=st.data())
def test_one_pass_equals_the_closure(direction, case, data):
    """One pass over Q, iterating the groups that iterate, gives D(S) and U(S)."""
    s = data.draw(seeds(case.graph.rows), label="S")
    found, _ = one_pass(case.schema, case.db, s, direction)
    assert found == (case.graph.D(s) if direction == "down" else case.graph.U(s))


@pytest.mark.parametrize("direction", ["down", "up"])
@given(case=cases(), data=st.data())
def test_one_step_suffices_when_no_group_iterates(direction, case, data):
    """When every FK inside every group maps owning attributes onto owning attributes, one step per group suffices."""
    assume(not any(iterates(case.schema, m) for m in case.schema.owned_parts))
    s = data.draw(seeds(case.graph.rows), label="S")
    found, _ = one_pass(case.schema, case.db, s, direction, iterate=False)
    assert found == (case.graph.D(s) if direction == "down" else case.graph.U(s))


@given(schema=schemas())
def test_every_fk_inside_a_group_carries_a_master_key(schema):
    """An FK references the whole primary key, which holds the referenced table's owning attributes."""
    for master in schema.owned_parts:
        for fk in schema.intra_group_fks(master):
            assert set(schema.owning[fk.parent].values()) <= set(fk.attr_map.values())


# =============================================================================
# R1: copying a condition (diagram.md, R1, the edge rule)
# =============================================================================


def _edge_and_names(case: Case, data, copyable: bool):
    fks = [fk for fk in case.schema.fks if fk.child != fk.parent]
    if copyable:
        fks = [fk for fk in fks if not fk.renamed and not set(fk.attr_map) & case.schema.nullable[fk.child]]
    assume(fks)
    fk = data.draw(st.sampled_from(fks), label="fk")
    parent, child = case.schema.table(fk.parent), case.schema.table(fk.child)
    # The FK's own attributes, three times as likely as the others.
    names = list(fk.attr_map) * 3 + sorted(set(parent.heading) | set(child.heading)) + ["unrelated"]
    return fk, names


@given(case=cases(), data=st.data())
def test_r1_copy_rule(case, data):
    """
    A condition copied under R1's rule selects the same child rows as the restriction.

    The rule: the attributes the condition names (every key of a dict, the whole
    heading of a query expression) are among the FK's attributes, the FK is not
    renamed, and none of its columns is nullable.
    """
    fk, names = _edge_and_names(case, data, copyable=True)
    cond = data.draw(conditions(names), label="condition")
    assume(copy_allowed(case.schema, fk, cond))
    parents = restrict(case.schema, case.db, fk.parent, cond)
    assert restrict(case.schema, case.db, fk.child, cond) == r1_down(case.schema, case.db, fk, parents)


# =============================================================================
# Relabeling materialized keys (diagram.md, Seeds)
# =============================================================================


@given(case=cases(), data=st.data())
def test_relabel_down_rule(case, data):
    """
    Parent keys relabeled under the Seeds rule select the same child rows as R1.

    The rule: every key names, among the parent's attributes, only attributes
    the FK carries; and if the FK has a nullable column, every key names all of
    the FK's attributes, none of them NULL.
    """
    fk, names = _edge_and_names(case, data, copyable=False)
    keys = data.draw(st.lists(key_dicts(names), max_size=3), label="keys")
    assume(relabel_down_allowed(case.schema, fk, keys))
    parents = restrict(case.schema, case.db, fk.parent, keys)
    relabeled = relabel_down(fk, keys)
    assert restrict(case.schema, case.db, fk.child, relabeled) == r1_down(case.schema, case.db, fk, parents)


@given(case=cases(), data=st.data())
def test_relabel_up_for_keys_read_from_the_child(case, data):
    """Keys read from child rows that contain all the FK's columns, relabeled, select the referenced parents."""
    fk, _ = _edge_and_names(case, data, copyable=False)
    child = case.schema.table(fk.child)
    picked = data.draw(st.frozensets(st.sampled_from(sorted(case.db[fk.child])) if case.db[fk.child] else st.nothing()))
    extra = data.draw(st.sets(st.sampled_from(child.heading)), label="other attributes")
    attrs = set(fk.attr_map) | extra
    keys = [{a: case.db[fk.child][k][a] for a in attrs} for k in picked]
    relabeled = relabel_up(fk, keys)
    referenced = r1_up(case.schema, case.db, fk, {(fk.child, k) for k in picked})
    assert restrict(case.schema, case.db, fk.parent, relabeled) == referenced


def test_relabel_rule_refuses_a_key_on_a_secondary_attribute():
    """diagram.md, Seeds: ``Session & {"quality": 0}`` relabels to ``{}`` across Trial's FK and would select every trial."""
    schema = Schema(
        [
            TableSpec("subject", ("subject_id",), ("species",)),
            TableSpec("session", ("subject_id", "session_id"), ("quality",)),
            TableSpec("trial", ("subject_id", "session_id", "trial_id")),
        ],
        [
            FK("session", "subject", {"subject_id": "subject_id"}),
            FK("trial", "session", {"subject_id": "subject_id", "session_id": "session_id"}),
        ],
    )
    db = make_db(
        schema,
        {
            "subject": [(1, "mouse"), (2, "rat")],
            "session": [(1, 1, 0), (1, 2, 1), (2, 2, 0)],
            "trial": [(1, 1, 1), (1, 2, 1), (2, 2, 1)],
        },
    )
    fk, keys = schema.fks_of("trial")[0], [{"quality": 0}]
    assert not relabel_down_allowed(schema, fk, keys)
    assert relabel_down(fk, keys) == [{}]
    assert restrict(schema, db, "trial", [{}]) == {("trial", k) for k in db["trial"]}  # every trial
    assert r1_down(schema, db, fk, restrict(schema, db, "session", keys)) == {("trial", (1, 1, 1)), ("trial", (2, 2, 1))}


def test_relabel_rule_refuses_a_null_key_value():
    """
    diagram.md, Seeds, on S3's schema: ``Surgery & {"subject_id": 1, "surgery_id": None}``
    selects no surgery but, relabeled, would select session (1, 2; NULL).
    """
    schema = Schema(
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
    )
    db = make_db(schema, {"subject": [(1,)], "surgery": [(1, 1)], "session": [(1, 1, 1), (1, 2, None)]})
    fk, keys = schema.fks_of("session")[1], [{"subject_id": 1, "surgery_id": None}]
    assert not relabel_down_allowed(schema, fk, keys)
    assert restrict(schema, db, "surgery", keys) == set()
    assert restrict(schema, db, "session", relabel_down(fk, keys)) == {("session", (1, 2))}


# =============================================================================
# self.upstream for a row not inserted yet
# =============================================================================


def _rows_outside_parts(case: Case):
    # make() fills a plain table or a master (with its parts), never a part on its own.
    return sorted(r for r in case.graph.rows if case.schema.table(r[0]).master is None)


@given(case=cases(), data=st.data())
def test_virtual_upstream_does_not_need_the_row(case, data):
    """
    ``self.upstream`` is the same before and after the row is inserted.

    Before the insert, the row, its parts and its descendants are absent:
    ``db - D({r})``. Its upstream is built from its primary FKs alone.
    """
    rows = _rows_outside_parts(case)
    assume(rows)
    r = data.draw(st.sampled_from(rows), label="r")
    table = case.schema.table(r[0])
    key = dict(zip(table.pk, r[1]))
    before = apply_delete(case.db, case.graph.D({r}))
    upstream = virtual_upstream(case.schema, before, table.name, key)
    assert upstream == virtual_upstream(case.schema, case.db, table.name, key)
    assert r not in upstream


@given(case=cases(), data=st.data())
def test_virtual_upstream_matches_trace_of_the_inserted_row(case, data):
    """
    Once the row exists, ``U({r})`` contains ``self.upstream``. They are equal,
    apart from r itself, when r is in no group and all its FKs are primary.
    """
    rows = _rows_outside_parts(case)
    assume(rows)
    r = data.draw(st.sampled_from(rows), label="r")
    schema, g = case.schema, case.graph
    table = schema.table(r[0])
    upstream = virtual_upstream(schema, case.db, table.name, dict(zip(table.pk, r[1])))
    traced = g.U({r})
    assert upstream <= traced
    if table.name not in schema.group and all(schema.is_primary(fk) for fk in schema.fks_of(table.name)):
        assert upstream == traced - {r}


def test_virtual_upstream_of_a_row_not_inserted():
    """U(∅) = ∅, so the upstream of a row not inserted yet comes from its primary FKs."""
    schema = Schema(
        [
            TableSpec("subject", ("subject_id",)),
            TableSpec("rig", ("rig_id",)),
            TableSpec("session", ("subject_id", "session_id"), ("rig_id",)),
            TableSpec("analysis", ("subject_id", "session_id")),
        ],
        [
            FK("session", "subject", {"subject_id": "subject_id"}),
            FK("session", "rig", {"rig_id": "rig_id"}),
            FK("analysis", "session", {"subject_id": "subject_id", "session_id": "session_id"}),
        ],
    )
    db = make_db(schema, {"subject": [(1,), (2,)], "rig": [(7,)], "session": [(1, 1, 7), (2, 1, 7)]})
    assert RowGraph(schema, db).U(set()) == set()
    assert virtual_upstream(schema, db, "analysis", {"subject_id": 1, "session_id": 1}) == {
        ("session", (1, 1)),
        ("subject", (1,)),
        ("rig", (7,)),  # reached through session's secondary FK, which is known
    }
    # A key with no matching parent row has an empty upstream.
    assert virtual_upstream(schema, db, "analysis", {"subject_id": 1, "session_id": 9}) == set()


def test_fk_value_follows_match_simple():
    """A2: an FK value with any NULL names nothing, even when its other attributes match a row."""
    fk = FK("child", "parent", {"a": "a", "b": "b"}, nullable=True)
    schema = Schema([TableSpec("parent", ("a", "b")), TableSpec("child", ("a", "c"), ("b",))], [fk])
    assert fk_value(schema, fk, {"a": 1, "b": None, "c": 1}) is None
    assert fk_value(schema, fk, {"a": 1, "b": 2, "c": 1}) == (1, 2)
