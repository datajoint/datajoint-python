"""
Tests of the traversal planner (``datajoint.expand``) against the reference model.

The planner builds a plan, an intermediate form, from a schema description and
a seed (the "Computation" section of the traversal specification, ``diagram.md``
in datajoint-docs). This module checks it without a database:

- **The schema description** agrees with the reference model: owned parts,
  owning attributes, groups that iterate, R1's copy rule, the relabeling rules,
  the table-level reach and the group quotient.
- **The plan, evaluated on rows held in memory**, selects exactly the model's
  ``D(S)``, ``U(S)`` and ``U(D(S))``, for random schemas, databases and seeds of
  every kind (a condition, literal key dicts, keys read from the seed table).
  Copies are evaluated as copies, by applying the copied condition to the
  child's rows, so the copy rule is tested and not assumed.
- **A delete simulation** runs the plan in reverse topological order, evaluating
  each table's restriction on the database as it is at that moment, with the
  materialized nodes replaced by the rows they selected before the first
  DELETE, and refusing a DELETE that would leave a reference to a deleted row
  (``ON DELETE RESTRICT``). It must leave ``DB - D(S)``.
- **The plan's structure** for the shapes of the catalog (``traversal-shapes.md``):
  where a condition is copied and where it is restricted, the lifts, and which
  groups iterate.

``HYPOTHESIS_PROFILE`` selects the number of examples (``tests/conftest.py``).
"""

from __future__ import annotations

import copy

import pytest
from hypothesis import assume, event, example, find, given, settings
from hypothesis import strategies as st

from datajoint import dependencies
from datajoint.errors import DataJointError
from datajoint.expand import (
    ForeignKey,
    Lift,
    Literal,
    Or,
    Plan,
    Reexpand,
    Restrict,
    RowEvaluator,
    Seed,
    TableInfo,
    TraversalSchema,
    evaluate_rows,
    plan_expand,
)

from .expand_model import (
    FK,
    RowGraph,
    Schema,
    TableSpec,
    apply_delete,
    condition_attributes,
    copy_allowed,
    dependency_graph,
    fk_value,
    full_table_name,
    iterates,
    make_db,
    q_acyclic,
    relabel_down_allowed,
    restrict,
    satisfies,
    table_reach,
)
from .expand_strategies import Case, cases, conditions, key_dicts, schemas

DIRECTIONS = ["down", "up", "both"]


# =============================================================================
# Helpers
# =============================================================================


def describe(schema: Schema) -> TraversalSchema:
    """The planner's schema description of a model schema."""
    tables = [TableInfo(t.name, t.pk, t.heading, schema.nullable[t.name], t.master) for t in schema.tables]
    return TraversalSchema(tables, [ForeignKey(fk.child, fk.parent, fk.attr_map) for fk in schema.fks])


def model_match(cond, row, table: TableInfo) -> bool:
    """A seed condition in the model's format (dict, list, Relation, Not), as a restriction."""
    return satisfies(row, table.attributes, cond)


def by_table(rows) -> dict:
    """``{table: {pk, ...}}`` from model row identifiers."""
    result = {}
    for t, k in rows:
        result.setdefault(t, set()).add(k)
    return result


def expected(case, seed_rows, direction):
    op = {"down": case.graph.D, "up": case.graph.U, "both": case.graph.both}[direction]
    return by_table(op(seed_rows))


def evaluated(plan: Plan, db) -> dict:
    """The plan's rows in memory, leaving out tables with none."""
    return {t: set(rows) for t, rows in evaluate_rows(plan, db, model_match).items() if rows}


@st.composite
def seed_nodes(draw, case, parts_first: bool = False):
    """
    Draw a seed table and a seed node of one of the kinds the planner handles.

    - a condition (a dict, a list of dicts, a query expression, a Not), whose
      attributes are those the model's ``condition_attributes`` reads;
    - literal key dicts (a materialized dict or list of dicts);
    - keys read from rows of the seed table, over any of its attributes;
    - no restriction.

    With ``parts_first``, the seed table is an owned part when the schema has one,
    so that the seed lifts to its master. Returns the seed node and the rows it
    selects.
    """
    schema = case.schema
    parts = [t.name for t in schema.tables if t.master and t.name in schema.group]
    table = draw(st.sampled_from(parts if parts_first and parts else [t.name for t in schema.tables]), label="seed table")
    heading = schema.table(table).heading
    names = list(heading) * 2 + sorted({a for t in schema.tables for a in t.heading}) + ["unrelated"]
    kind = draw(st.sampled_from(["condition", "literal", "read", "all"]), label="seed kind")
    if kind == "condition":
        cond = draw(conditions(names), label="condition")
        seed = Seed(table, cond, frozenset(condition_attributes(cond)))
    elif kind == "literal":
        cond = draw(st.lists(key_dicts(names), max_size=3), label="keys")
        seed = Literal(table, tuple(cond))
    elif kind == "read":
        rows = sorted(case.db[table])
        picked = draw(st.lists(st.sampled_from(rows), unique=True, max_size=3) if rows else st.just([]))
        attrs = draw(st.sets(st.sampled_from(heading)), label="attributes read")
        cond = [{a: case.db[table][k][a] for a in sorted(attrs)} for k in picked]
        seed = Literal(table, tuple(cond), read=True)
    else:
        cond = {}
        seed = Seed(table, cond, frozenset())
    return seed, restrict(schema, case.db, table, cond)


@st.composite
def seeded_cases(draw, parts_first: bool = False):
    """A drawn case with a seed node: ``(case, seed, seed_rows)``."""
    case = draw(cases())
    return (case, *draw(seed_nodes(case, parts_first)))


def lifts_evaluated_at_delete(plan: Plan) -> set:
    """
    The Lift nodes that a delete evaluates when it reaches a table.

    A group that iterates has its master keys found before the first DELETE, so
    the lifts behind them are not evaluated again.
    """
    found, seen = set(), set()
    stack = list(plan.restriction.values())
    while stack:
        h = stack.pop()
        if h in seen:
            continue
        seen.add(h)
        node = plan.nodes[h]
        if isinstance(node, Lift):
            found.add(h)
        if isinstance(node, (Restrict, Lift, Reexpand)):
            stack.append(node.src)
        elif isinstance(node, Or):
            stack.extend(node.terms)
    return found


class RestrictViolation(Exception):
    """A DELETE would leave a row that references a deleted row (``ON DELETE RESTRICT``)."""


def simulate_delete(plan: Plan, model: Schema, db, match_for=lambda db: model_match, materialize=()):
    """
    Run a ``down`` plan as ``Table.delete`` runs a cascade, on a copy of ``db``.

    Before the first DELETE, the nodes in ``materialize`` are replaced by the rows
    they select, and the master keys of the groups that iterate are found (as
    compiling the plan does). Then each table, leaves first, deletes the rows its
    restriction selects on the database as it is at that moment. A DELETE that
    would leave a row referencing a deleted row raises ``RestrictViolation``.

    ``match_for(db)`` returns the seed matcher for a database state, so that a
    seed whose condition reads other tables reads them as they are.
    """
    first = RowEvaluator(plan, db, match_for(db))
    keys = first.group_key_sets()
    fixed = {h: first.value(h) for h in materialize}
    current = copy.deepcopy(db)
    for table in reversed(plan.order):
        doomed = RowEvaluator(plan, current, match_for(current), fixed=fixed, keys=keys).table(table)
        for fk in model.fks:
            if fk.parent != table:
                continue
            for k, row in current[fk.child].items():
                if fk.child == table and k in doomed:
                    continue
                if fk_value(model, fk, row) in doomed:
                    raise RestrictViolation(f"deleting {table} {fk_value(model, fk, row)} leaves {fk.child} {k}")
        current[table] = {k: r for k, r in current[table].items() if k not in doomed}
    return current


def spec_materialization(plan: Plan) -> set:
    """
    What diagram.md ("Materialization happens only at delete time") materializes:
    every lift, and the seed unless its restriction is literal keys.
    """
    flagged = set(lifts_evaluated_at_delete(plan))
    if isinstance(plan.nodes[plan.seed], Seed):
        flagged.add(plan.seed)
    return flagged


# =============================================================================
# The schema description agrees with the model
# =============================================================================


@given(schema=schemas())
def test_description_agrees_with_the_model(schema):
    """Owned parts, owning attributes, the groups that iterate, and the group quotient."""
    d = describe(schema)
    model_groups = {m: parts for m, parts in schema.owned_parts.items() if parts}
    assert {m: set(p) for m, p in d.groups.items()} == {m: set(p) for m, p in model_groups.items()}
    for table in d.group_of:
        assert d.owning_attributes(table) == schema.owning_attributes(table), table
    for master in d.groups:
        assert bool(d.links(master)) == iterates(schema, master)
    if q_acyclic(schema):
        order = d.order(d.tables)
        position = {t: i for i, t in enumerate(order)}
        assert all(position[fk.parent] < position[fk.child] for fk in schema.fks)
        for master, parts in d.groups.items():
            assert order[position[master] : position[master] + 1 + len(parts)] == [master, *parts]
    else:
        with pytest.raises(DataJointError, match="cycle"):
            d.quotient_order(d.tables)


@given(schema=schemas())
def test_reach_is_the_table_level_reach(schema):
    """The node set of an expanded diagram, in each direction."""
    d = describe(schema)
    for t in schema.tables:
        for direction in DIRECTIONS:
            assert d.reach({t.name}, direction) == table_reach(schema, {t.name}, direction)


@given(case=cases(), data=st.data())
def test_copy_and_relabel_rules_agree_with_the_model(case, data):
    """R1's copy rule and the Seeds rule for relabeling keys, as the model states them."""
    d = describe(case.schema)
    fks = [fk for fk in case.schema.fks if fk.child != fk.parent]
    assume(fks)
    fk = data.draw(st.sampled_from(fks))
    names = list(fk.attr_map) * 3 + sorted({a for t in case.schema.tables for a in t.heading}) + ["unrelated"]
    cond = data.draw(conditions(names))
    keys = data.draw(st.lists(key_dicts(names), max_size=3))
    ours = ForeignKey(fk.child, fk.parent, fk.attr_map)
    assert d.copy_allowed(ours, frozenset(condition_attributes(cond))) == copy_allowed(case.schema, fk, cond)
    assert d.relabel_down_allowed(ours, keys) == relabel_down_allowed(case.schema, fk, keys)


class _Heading:
    """The parts of a ``Heading`` that ``TraversalSchema.from_dependencies`` reads."""

    class _Attribute:
        def __init__(self, nullable):
            self.nullable = nullable

    def __init__(self, spec: TableSpec, nullable):
        self.names = list(spec.heading)
        self.primary_key = list(spec.pk)
        self.attributes = {a: self._Attribute(a in nullable) for a in spec.heading}


@pytest.mark.parametrize("quote", ["`", '"'])
@given(schema=schemas())
def test_description_from_the_dependency_graph(quote, schema):
    """
    Built from ``connection.dependencies``, the description has the same groups
    and owning attributes; the order of the foreign keys comes from the headings.
    """
    graph = dependency_graph(schema, quote)
    name = {t.name: full_table_name(t.name, quote) for t in schema.tables}
    headings = {name[t.name]: _Heading(t, schema.nullable[t.name]) for t in schema.tables}
    d = TraversalSchema.from_dependencies(graph, headings, headings.__getitem__)
    expected = describe(schema)
    assert {m: set(p) for m, p in d.groups.items()} == {name[m]: {name[p] for p in ps} for m, ps in expected.groups.items()}
    for t in expected.group_of:
        assert d.owning_attributes(name[t]) == expected.owning_attributes(t)
    assert {n: d.tables[n].nullable for n in d.tables} == {name[t]: v.nullable for t, v in expected.tables.items()}


@given(schema=schemas())
def test_owned_parts_agree_with_dependencies(schema):
    """
    ``datajoint.dependencies.owned_parts`` (#1620) finds the same owned parts in
    the dependency graph. Skipped until that function exists on this branch.
    """
    owned_parts = getattr(dependencies, "owned_parts", None)
    if owned_parts is None:
        pytest.skip("datajoint.dependencies.owned_parts is not on this branch (#1620)")
    graph = dependency_graph(schema)
    d = describe(schema)
    assert owned_parts(graph) == {full_table_name(p) for parts in d.groups.values() for p in parts}


# =============================================================================
# The plan evaluated in memory equals the definitions
# =============================================================================


@pytest.mark.parametrize("direction", DIRECTIONS)
@given(case=cases(), data=st.data())
def test_plan_equals_the_model(direction, case, data):
    """
    The plan, evaluated on rows, selects the model's D(S), U(S) or U(D(S)), and
    its tables are the table-level reach of the seed table.
    """
    seed, seed_rows = data.draw(seed_nodes(case), label="seed")
    plan = plan_expand(describe(case.schema), seed, direction)
    assert set(plan.order) == table_reach(case.schema, {seed.table}, direction)
    assert evaluated(plan, case.db) == expected(case, seed_rows, direction)


@given(case=cases(), data=st.data())
def test_seed_kinds_give_the_same_rows(case, data):
    """``plan(T & r)`` and ``plan(T & (T & r).keys())`` select the same rows (K3)."""
    seed, seed_rows = data.draw(seed_nodes(case), label="seed")
    pk = case.schema.table(seed.table).pk
    keys = Literal(seed.table, tuple(dict(zip(pk, k)) for _, k in sorted(seed_rows)), read=True)
    d = describe(case.schema)
    for direction in DIRECTIONS:
        assert evaluated(plan_expand(d, seed, direction), case.db) == evaluated(plan_expand(d, keys, direction), case.db)


@pytest.mark.parametrize("direction", ["down", "up"])
@given(case=cases(), data=st.data())
def test_groups_iterate_by_the_model_rule(direction, case, data):
    """A plan marks a group to iterate exactly when the model's rule does."""
    seed, _ = data.draw(seed_nodes(case), label="seed")
    plan = plan_expand(describe(case.schema), seed, direction)
    for group in plan.groups:
        assert group.iterate == iterates(case.schema, group.master)
        assert (group.keys is None) == (not group.iterate)
        assert plan.restriction[group.master] == (group.keys if group.iterate else group.entry)


def _plans(seeded, direction):
    case, seed, _ = seeded
    return plan_expand(describe(case.schema), seed, direction)


def _has(kind, direction="down", **fields):
    def predicate(seeded):
        plan = _plans(seeded, direction)
        return any(
            type(n) is kind and all(getattr(n, k) == v for k, v in fields.items()) and h != plan.seed
            for h, n in enumerate(plan.nodes)
        )

    return predicate


PLAN_FEATURES = {
    "a copy": _has(Restrict, copy=True),
    "a restriction across a renamed foreign key": lambda x: _has(Restrict, copy=False)(x)
    and any(not all(a == b for a, b in n.attr_map) for n in _plans(x, "down").nodes if type(n) is Restrict),
    "keys relabeled downward": _has(Literal, read=False),
    "keys relabeled upward": _has(Literal, direction="up", read=False),
    "a lift": _has(Lift),
    "a group that iterates downward": lambda x: bool(_plans(x, "down").iterating_groups()),
    "a group that iterates upward": lambda x: bool(_plans(x, "up").iterating_groups()),
}


@pytest.mark.parametrize("feature", sorted(PLAN_FEATURES))
def test_drawn_plans_have_every_node_kind(feature):
    """The drawn schemas and seeds reach every kind of plan node the properties rely on."""
    find(seeded_cases(), PLAN_FEATURES[feature], settings=settings(max_examples=5000, database=None, derandomize=True))


# =============================================================================
# The catalog shapes: plan structure and rows
# =============================================================================


class Shape:
    """A catalog shape: its model schema and rows, and its description."""

    def __init__(self, tables, fks, data):
        self.model = Schema(tables, fks)
        self.db = make_db(self.model, data)
        self.schema = describe(self.model)

    def plan(self, table, cond, direction, attributes=None):
        attrs = frozenset(condition_attributes(cond)) if attributes is None else attributes
        return plan_expand(self.schema, Seed(table, cond, attrs), direction)

    def check(self, plan, expected_rows):
        """The plan's rows equal the catalog's (and so the model's)."""
        rows = {t: {k if isinstance(k, tuple) else (k,) for k in ks} for t, ks in expected_rows.items() if ks}
        assert evaluated(plan, self.db) == rows
        assert set(plan.order) == set(expected_rows)


def node(plan: Plan, table: str):
    """The node that holds the rows of ``table``."""
    return plan.nodes[plan.restriction[table]]


def terms(plan: Plan, table: str) -> list:
    """The terms of the union that holds the rows of ``table``."""
    n = node(plan, table)
    return [plan.nodes[t] for t in n.terms] if isinstance(n, Or) else [n]


def s1():
    return Shape(
        [TableSpec("subject", ("subject_id",)), TableSpec("session", ("subject_id", "session_id"))],
        [FK("session", "subject", {"subject_id": "subject_id"})],
        {"subject": [(1,), (2,)], "session": [(1, 1), (1, 2), (2, 1)]},
    )


def test_S1_plan():
    """S1: downward the condition is copied; upward the parent is restricted."""
    s = s1()
    down = s.plan("subject", {"subject_id": 1}, "down")
    assert node(down, "session") == Restrict("session", down.seed, (("subject_id", "subject_id"),), copy=True)
    s.check(down, {"subject": [1], "session": [(1, 1), (1, 2)]})
    up = s.plan("session", {"session_id": 1}, "up")
    assert node(up, "subject") == Restrict("subject", up.seed, (("subject_id", "subject_id"),))
    s.check(up, {"session": [(1, 1), (2, 1)], "subject": [1, 2]})


def test_S2_plan():
    """S2: a secondary FK copies downward and restricts upward."""
    s = Shape(
        [TableSpec("#rig", ("rig_id",)), TableSpec("session", ("session_id",), ("rig_id",))],
        [FK("session", "#rig", {"rig_id": "rig_id"})],
        {"#rig": [(1,), (2,), (3,)], "session": [(1, 1), (2, 1), (3, 2)]},
    )
    down = s.plan("#rig", {"rig_id": 1}, "down")
    assert node(down, "session").copy
    s.check(down, {"#rig": [1], "session": [1, 2]})
    up = s.plan("session", {"session_id": 3}, "up")
    assert node(up, "#rig") == Restrict("#rig", up.seed, (("rig_id", "rig_id"),))
    s.check(up, {"session": [3], "#rig": [2]})


def s3():
    return Shape(
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


def test_S3_plan():
    """S3: a foreign key with a nullable column is crossed by restriction, never copied or relabeled."""
    s = s3()
    down = s.plan("surgery", {"subject_id": 1}, "down")
    pairs = (("subject_id", "subject_id"), ("surgery_id", "surgery_id"))
    assert node(down, "session") == Restrict("session", down.seed, pairs)
    s.check(down, {"surgery": [(1, 1), (1, 2)], "session": [(1, 1), (1, 3)]})
    up = s.plan("session", {"subject_id": 1, "session_id": 2}, "up")
    s.check(up, {"session": [(1, 2)], "subject": [1], "surgery": []})
    # Literal keys that name the whole foreign key, none NULL, are relabeled; a NULL is not.
    fk = s.schema.fks_of("session")[1]
    assert s.schema.relabel_down_allowed(fk, [{"subject_id": 1, "surgery_id": 2}])
    assert not s.schema.relabel_down_allowed(fk, [{"subject_id": 1, "surgery_id": None}])
    assert not s.schema.relabel_down_allowed(fk, [{"subject_id": 1}])
    keys = plan_expand(s.schema, Literal("surgery", ({"subject_id": 1, "surgery_id": 2},)), "down")
    assert node(keys, "session") == Literal("session", ({"subject_id": 1, "surgery_id": 2},))


def test_S4_plan():
    """S4: a renamed foreign key is crossed through its attr_map."""
    s = Shape(
        [TableSpec("neuron", ("neuron_id",)), TableSpec("input", ("source_id", "input_id"), ("neuron_id",))],
        [FK("input", "neuron", {"source_id": "neuron_id"})],
        {"neuron": [(1,), (2,), (3,)], "input": [(1, 1, 2), (1, 2, 3), (2, 1, 1), (3, 1, 1)]},
    )
    down = s.plan("neuron", {"neuron_id": 1}, "down")
    assert node(down, "input") == Restrict("input", down.seed, (("source_id", "neuron_id"),))
    s.check(down, {"neuron": [1], "input": [(1, 1), (1, 2)]})
    up = s.plan("input", {"source_id": 2, "input_id": 1}, "up")
    assert node(up, "neuron") == Restrict("neuron", up.seed, (("neuron_id", "source_id"),))
    s.check(up, {"input": [(2, 1)], "neuron": [2]})


def test_S5_plan():
    """S5: parallel foreign keys are two terms of a union; the unrenamed one is a copy."""
    s = Shape(
        [TableSpec("neuron", ("neuron_id",)), TableSpec("synapse", ("neuron_id", "target_id"))],
        [FK("synapse", "neuron", {"neuron_id": "neuron_id"}), FK("synapse", "neuron", {"target_id": "neuron_id"})],
        {"neuron": [(1,), (2,), (3,)], "synapse": [(1, 2), (2, 3), (3, 1)]},
    )
    down = s.plan("neuron", {"neuron_id": 1}, "down")
    assert terms(down, "synapse") == [
        Restrict("synapse", down.seed, (("neuron_id", "neuron_id"),), copy=True),
        Restrict("synapse", down.seed, (("target_id", "neuron_id"),)),
    ]
    s.check(down, {"neuron": [1], "synapse": [(1, 2), (3, 1)]})
    s.check(s.plan("neuron", {"neuron_id": 1}, "both"), {"neuron": [1, 2, 3], "synapse": [(1, 2), (3, 1)]})
    up = s.plan("synapse", {"neuron_id": 1, "target_id": 2}, "up")
    assert len(terms(up, "neuron")) == 2
    s.check(up, {"synapse": [(1, 2)], "neuron": [1, 2]})


def s7():
    return Shape(
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


def test_S7_plan():
    """S7: Rig's copied condition names lab_id, which Session's FK to Rig does not carry, so Rig restricts Session."""
    s = s7()
    down = s.plan("lab", {"lab_id": 1}, "down")
    rig = down.restriction["rig"]
    assert node(down, "rig") == Restrict("rig", down.seed, (("lab_id", "lab_id"),), copy=True)
    assert terms(down, "session") == [
        Restrict("session", down.seed, (("lab_id", "lab_id"),), copy=True),
        Restrict("session", rig, (("rig_id", "rig_id"),)),
    ]
    s.check(down, {"lab": [1], "rig": [10], "session": [(1, 1), (2, 1)]})
    s.check(s.plan("session", {"lab_id": 2, "session_id": 1}, "up"), {"session": [(2, 1)], "rig": [10], "lab": [1, 2]})
    s.check(s.plan("rig", {"rig_id": 10}, "both"), {"rig": [10], "session": [(2, 1)], "lab": [1, 2]})


def g1():
    return Shape(
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


def test_G1_plan():
    """G1: the part lifts to its master, and the master re-expands to all its part rows."""
    s = g1()
    down = s.plan("session__trial", {"trial_id": 2}, "down")
    master = down.restriction["session"]
    assert node(down, "session") == Lift("session", down.seed, (("session_id", "session_id"),))
    assert node(down, "session__trial") == Reexpand("session__trial", master, (("session_id", "session_id"),))
    assert node(down, "review") == Restrict("review", master, (("session_id", "session_id"),), copy=True)
    assert [g.iterate for g in down.groups] == [False]
    s.check(down, {"session__trial": [(1, 1), (1, 2)], "session": [1], "review": [(1, 1)]})
    up = s.plan("session__trial", {"trial_id": 2}, "up")
    assert node(up, "session") == Lift("session", up.seed, (("session_id", "session_id"),))
    s.check(up, {"session__trial": [(1, 1), (1, 2)], "session": [1]})


def test_G2_plan():
    """G2: the lift is one step on the master key, whichever chain owns the part; no iteration."""
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
    down = s.plan("ext", {"ext_id": 1}, "down")
    assert [type(t) for t in terms(down, "master")] == [Lift, Lift]
    assert not down.iterating_groups()
    s.check(down, {"ext": [1], "master__q": [(1, 10), (2, 20)], "master__p": [(1, 100), (2, 200)], "master": [1, 2]})
    up = s.plan("master__p", {"master_id": 1, "p_id": 100}, "up")
    s.check(up, {"master__p": [(1, 100)], "master": [1], "master__q": [(1, 10)], "ext": [1, 2]})


def test_G3_plan():
    """G3: only the owning reference lifts; the second, renamed FK to the master links instances, so the group iterates."""
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
    down = s.plan("session__link", {"session_id": 1}, "down")
    (group,) = down.groups
    assert group.iterate and [fk.mapping for fk in group.links] == [{"other_session": "session_id"}]
    assert down.nodes[group.entry] == Lift("session", down.seed, (("session_id", "session_id"),))
    s.check(down, {"session__link": [(1, 1), (3, 1)], "session": [1, 3]})
    s.check(s.plan("session__link", {"session_id": 1}, "up"), {"session__link": [(1, 1)], "session": [1, 2]})


def g4():
    return Shape(
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


def test_G4_plan():
    """G4: the renamed sibling FK links instances; the master keys grow {1}, {1, 2}, {1, 2, 3}."""
    s = g4()
    down = s.plan("run", {"run_id": 1}, "down")
    (group,) = down.iterating_groups()
    assert [(fk.child, fk.parent) for fk in group.links] == [("run__carry", "run__step")]
    assert down.restriction["run"] == group.keys
    assert down.nodes[group.keys] == Literal("run", None, read=True)
    # One group step: carry rows that reference a step row of the current runs, lifted to their run.
    step = down.nodes[group.step]
    assert isinstance(step, Lift) and down.nodes[step.src].attr_map == (("prev_run", "run_id"), ("prev_step", "step_id"))
    evaluator = RowEvaluator(down, s.db, model_match)
    assert {t: set(r) for t, r in evaluator.result().items()} == {
        "run": {(1,), (2,), (3,)},
        "run__step": {(1, 1), (2, 1), (3, 1)},
        "run__carry": {(2, 1), (3, 1)},
    }
    assert evaluator.rounds[("down", "run")] == [{(1,)}, {(1,), (2,)}, {(1,), (2,), (3,)}]
    up = s.plan("run", {"run_id": 2}, "up")
    evaluator = RowEvaluator(up, s.db, model_match)
    evaluator.result()
    assert evaluator.rounds[("up", "run")] == [{(2,)}, {(1,), (2,)}]
    s.check(up, {"run": [1, 2], "run__step": [(1, 1), (2, 1)], "run__carry": [(2, 1)]})
    s.check(
        s.plan("run", {"run_id": 2}, "both"),
        {"run": [1, 2, 3], "run__step": [(1, 1), (2, 1), (3, 1)], "run__carry": [(2, 1), (3, 1)]},
    )
    # Both passes iterate, each with its own keys.
    both = s.plan("run", {"run_id": 2}, "both")
    assert [(g.direction, g.iterate) for g in both.groups] == [("down", True), ("up", True)]


def test_G5_plan():
    """G5: a trace reaches the parts of an ancestor master and their parents; a node may hold zero rows."""
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
    up = s.plan("analysis", {"recording_id": 1}, "up")
    assert node(up, "ephys_source") == Restrict(
        "ephys_source", up.restriction["recording__ephys"], (("ephys_id", "ephys_id"),)
    )
    expected_up = {
        "analysis": [1],
        "recording": [1],
        "recording__ephys": [1],
        "recording__imaging": [],
        "ephys_source": [1],
        "imaging_source": [],
    }
    s.check(up, expected_up)
    down = s.plan("ephys_source", {"ephys_id": 2}, "down")
    assert isinstance(node(down, "recording"), Lift)
    s.check(
        down,
        {"ephys_source": [2], "recording__ephys": [2], "recording": [2], "recording__imaging": [], "analysis": [2]},
    )


def test_G6_plan():
    """G6: a part its master does not own is a plain table: no group, no lift."""
    s = Shape(
        [TableSpec("probe", ("probe_id",)), TableSpec("probe__channel", ("channel_id",), ("probe_id",), master="probe")],
        [FK("probe__channel", "probe", {"probe_id": "probe_id"})],
        {"probe": [(1,), (2,)], "probe__channel": [(1, 1), (2, 1), (3, 2)]},
    )
    assert s.schema.groups == {}
    down = s.plan("probe__channel", {"channel_id": 1}, "down")
    assert not down.groups
    s.check(down, {"probe__channel": [1]})
    s.check(s.plan("probe__channel", {"channel_id": 1}, "up"), {"probe__channel": [1], "probe": [1]})
    s.check(s.plan("probe", {"probe_id": 1}, "down"), {"probe": [1], "probe__channel": [1, 2]})


def g7():
    return Shape(
        [
            TableSpec("master", ("master_id",)),
            TableSpec("master__part_a", ("master_id", "part_a_id"), master="master"),
            TableSpec("master__part_b", ("src_master", "src_part", "part_b_id"), master="master"),
            TableSpec("report", ("master_id", "report_id")),
        ],
        [
            FK("master__part_a", "master", {"master_id": "master_id"}),
            FK("master__part_b", "master__part_a", {"src_master": "master_id", "src_part": "part_a_id"}),
            FK("report", "master", {"master_id": "master_id"}),
        ],
        {
            "master": [(1,), (2,), (3,)],
            "master__part_a": [(1, 10), (1, 11), (2, 20)],
            "master__part_b": [(1, 10, 100), (1, 11, 110), (2, 20, 200)],
            "report": [(1, 1), (2, 1)],
        },
    )


def test_G7_plan():
    """G7: the lift renames the owning attribute onto the master's key; no iteration."""
    s = g7()
    down = s.plan("master__part_b", {"part_b_id": 100}, "down")
    assert node(down, "master") == Lift("master", down.seed, (("master_id", "src_master"),))
    assert node(down, "master__part_b") == Reexpand(
        "master__part_b", down.restriction["master"], (("src_master", "master_id"),)
    )
    assert not down.iterating_groups()
    s.check(
        down,
        {
            "master__part_b": [(1, 10, 100), (1, 11, 110)],
            "master": [1],
            "master__part_a": [(1, 10), (1, 11)],
            "report": [(1, 1)],
        },
    )


def k_shape():
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
            "flag": [(1, 1), (2, 3)],
        },
    )


class SessionsOfQuality:
    """``Subject & (Session & "quality = <q>")`` as a live condition: it reads Session as it is."""

    def __init__(self, quality):
        self.quality = quality


def k1_match_for(db):
    def match(cond, row, table):
        if isinstance(cond, SessionsOfQuality):
            return any(s["subject_id"] == row["subject_id"] and s["quality"] == cond.quality for s in db["session"].values())
        return model_match(cond, row, table)

    return match


K1_DOWN = {
    "subject": [1, 2],
    "session": [(1, 1), (1, 2), (2, 1), (2, 2)],
    "trial": [(1, 1, 1), (1, 2, 1), (2, 1, 1), (2, 2, 1)],
    "flag": [1],
}


def test_K1_plan():
    """
    K1: a seed restricted by a descendant. Its condition, a query expression,
    names the whole heading of Session, so it is not copied; nor is the seed
    copied across Flag's nullable foreign key.
    """
    s = k_shape()
    seed = Seed("subject", SessionsOfQuality(0), frozenset({"subject_id", "session_id", "quality"}))
    down = plan_expand(s.schema, seed, "down")
    assert node(down, "session") == Restrict("session", down.seed, (("subject_id", "subject_id"),))
    assert node(down, "flag") == Restrict("flag", down.seed, (("subject_id", "subject_id"),))
    rows = {t: {k if isinstance(k, tuple) else (k,) for k in ks} for t, ks in K1_DOWN.items()}
    assert {t: set(r) for t, r in RowEvaluator(down, s.db, k1_match_for(s.db)).result().items() if r} == rows
    # Deleting with the seed materialized first removes exactly these rows.
    after = simulate_delete(down, s.model, s.db, k1_match_for, materialize={down.seed})
    expected_rows = {(t, k) for t, ks in rows.items() for k in ks}
    assert after == apply_delete(s.db, expected_rows)


def test_K4_plan():
    """K4: a dj.Top seed is replaced by its keys, which cross downward by relabeling."""
    s = k_shape()
    keys = ({"subject_id": 1, "session_id": 2}, {"subject_id": 2, "session_id": 1})
    down = plan_expand(s.schema, Literal("session", keys, read=True), "down")
    assert node(down, "trial") == Literal("trial", keys)
    s.check(down, {"session": [(1, 2), (2, 1)], "trial": [(1, 2, 1), (2, 1, 1)]})
    # Upward, keys read from the table itself that hold the whole foreign key are relabeled too.
    up = plan_expand(s.schema, Literal("session", keys, read=True), "up")
    assert node(up, "subject") == Literal("subject", ({"subject_id": 1}, {"subject_id": 2}))
    s.check(up, {"session": [(1, 2), (2, 1)], "subject": [1, 2]})


def test_K5_empty_seeds():
    """K5: a seed that selects no rows expands to nodes with no rows."""
    s = k_shape()
    for seed in (Literal("session", ()), Seed("session", [], frozenset()), Seed("session", {"subject_id": 4}, None)):
        s.check(plan_expand(s.schema, seed, "down"), {"session": [], "trial": []})
        s.check(plan_expand(s.schema, seed, "up"), {"session": [], "subject": []})


def test_user_keys_cross_upward_by_restriction():
    """K5: a user-supplied key dict may name rows that do not exist, so it is not relabeled upward."""
    s = k_shape()
    up = plan_expand(s.schema, Literal("session", ({"subject_id": 4},)), "up")
    assert isinstance(node(up, "subject"), Restrict)
    s.check(up, {"session": [], "subject": []})


# =============================================================================
# The delete simulation
# =============================================================================


@given(seeded=seeded_cases())
def test_delete_simulation_without_live_lifts(seeded):
    """
    Without a lift evaluated at delete time, deleting table by table, leaves
    first, with live evaluation, removes exactly D(S) and never breaks a
    foreign key.

    The model's seed conditions read no table, and a parent is deleted after its
    children, so every other restriction still sees the rows it reads.
    """
    case, seed, seed_rows = seeded
    plan = plan_expand(describe(case.schema), seed, "down")
    assume(not lifts_evaluated_at_delete(plan))
    after = simulate_delete(plan, case.schema, case.db)
    assert after == apply_delete(case.db, case.graph.D(seed_rows))


@pytest.mark.parametrize("parts_first", [False, True], ids=["any seed", "part seed"])
@given(data=st.data())
def test_delete_simulation_with_the_spec_materialization(parts_first, data):
    """
    Materializing what diagram.md says to materialize before the first DELETE
    (every lift, and the seed unless it is literal keys) makes the delete remove
    exactly D(S), and it never breaks a foreign key.
    """
    case, seed, seed_rows = data.draw(seeded_cases(parts_first), label="case and seed")
    plan = plan_expand(describe(case.schema), seed, "down")
    event("a lift is evaluated at delete time" if lifts_evaluated_at_delete(plan) else "no lift at delete time")
    after = simulate_delete(plan, case.schema, case.db, materialize=spec_materialization(plan))
    assert after == apply_delete(case.db, case.graph.D(seed_rows))


NEEDS_E3 = (
    "E3: Table.delete materializes the lifts and live seeds of the plan before the first DELETE "
    "(diagram.md, 'Materialization happens only at delete time'); the E2a plan carries no materialization flags"
)


def _plan_materialization(plan: Plan) -> set:
    # The flags E3 adds to the plan; none yet.
    return set(getattr(plan, "materialize", ()))


def _g1_seeded():
    """G1's schema with the seed Session.Trial & {"trial_id": 2}, as ``seeded_cases`` draws them."""
    s = g1()
    cond = {"trial_id": 2}
    return (
        Case(s.model, s.db, RowGraph(s.model, s.db)),
        Seed("session__trial", cond, frozenset(cond)),
        restrict(s.model, s.db, "session__trial", cond),
    )


@pytest.mark.xfail(strict=True, reason=NEEDS_E3)
@given(seeded=seeded_cases(parts_first=True))
@example(seeded=_g1_seeded())
def test_delete_simulation(seeded):
    """The L0′ delete property: the plan's own materialization flags make the delete remove exactly D(S)."""
    case, seed, seed_rows = seeded
    plan = plan_expand(describe(case.schema), seed, "down")
    after = simulate_delete(plan, case.schema, case.db, materialize=_plan_materialization(plan))
    assert after == apply_delete(case.db, case.graph.D(seed_rows))


# =============================================================================
# The delete simulation on catalog shapes
# =============================================================================


@pytest.mark.parametrize(
    "shape, table, cond",
    [
        pytest.param(g1, "session__trial", {"trial_id": 2}, id="G1"),
        pytest.param(g7, "master__part_b", {"part_b_id": 100}, id="G7"),
    ],
)
def test_delete_needs_the_lift_materialized(shape, table, cond):
    """
    The part rows are deleted before their master, so a lift evaluated at the
    master's DELETE finds none and the master stays; materialized first, the
    delete removes D(S).
    """
    s = shape()
    plan = s.plan(table, cond, "down")
    seed_rows = restrict(s.model, s.db, table, cond)
    expected_db = apply_delete(s.db, RowGraph(s.model, s.db).D(seed_rows))
    assert simulate_delete(plan, s.model, s.db) != expected_db
    assert simulate_delete(plan, s.model, s.db, materialize=spec_materialization(plan)) == expected_db


@pytest.mark.xfail(strict=True, reason=NEEDS_E3)
@pytest.mark.parametrize(
    "shape, table, cond",
    [
        pytest.param(g1, "session__trial", {"trial_id": 2}, id="G1"),
        pytest.param(g7, "master__part_b", {"part_b_id": 100}, id="G7"),
        pytest.param(k_shape, "subject", SessionsOfQuality(0), id="K1"),
    ],
)
def test_delete_with_the_plan_flags(shape, table, cond):
    """With the materialization flags of the plan itself, the delete removes D(S) (#1496)."""
    s = shape()
    attrs = frozenset({"subject_id", "session_id", "quality"}) if isinstance(cond, SessionsOfQuality) else None
    plan = s.plan(table, cond, "down", attributes=attrs)
    match_for = k1_match_for if isinstance(cond, SessionsOfQuality) else (lambda db: model_match)
    seed_rows = {(table, k) for k, row in s.db[table].items() if match_for(s.db)(cond, row, s.schema.tables[table])}
    expected_db = apply_delete(s.db, RowGraph(s.model, s.db).D(seed_rows))
    assert simulate_delete(plan, s.model, s.db, match_for, materialize=_plan_materialization(plan)) == expected_db


# =============================================================================
# Plans and errors
# =============================================================================


def test_unknown_direction_and_seed_table():
    s = s1()
    with pytest.raises(ValueError, match="direction"):
        s.plan("subject", {}, "sideways")
    with pytest.raises(DataJointError, match="not in the schema"):
        plan_expand(s.schema, Seed("nowhere", {}, frozenset()), "down")


def test_group_cycle_is_refused():
    """X2: a group whose part references a table that depends on its master cannot be ordered."""
    model = Schema(
        [
            TableSpec("session", ("session_id",)),
            TableSpec("review", ("session_id",)),
            TableSpec("session__note", ("session_id",), master="session"),
        ],
        [
            FK("review", "session", {"session_id": "session_id"}),
            FK("session__note", "session", {"session_id": "session_id"}),
            FK("session__note", "review", {"session_id": "session_id"}),
        ],
    )
    with pytest.raises(DataJointError, match="cycle: session -> review -> session"):
        plan_expand(describe(model), Seed("session", {}, frozenset()), "down")


def test_describe_lists_nodes_and_tables():
    s = g4()
    text = s.plan("run", {"run_id": 1}, "down").describe()
    assert "Seed(run; names {run_id})" in text
    assert "down: run iterates over run__carry -> run__step" in text
    assert "run__carry = #" in text
