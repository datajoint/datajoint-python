"""
Property tests of the ``Diagram.expand`` reference model against the specification.

The specification states eight results, T1-T8, about the operations ``D``,
``D1``, ``U`` and ``both`` (see ``expand_model.py``). Each one is tested here on
random schemas and databases drawn by Hypothesis, with no database server:

- T1 closure: D and U are extensive, monotone and idempotent.
- T2 additivity: ``X(A ∪ B) = X(A) ∪ X(B)`` for D, U and both.
- T3 duality: ``y ∈ D({x})`` if and only if ``x ∈ U({y})``.
- T4 delete safety: removing D(S) leaves a consistent database consistent, and
  D(S) is the least such superset of S.
- T5 read boundary: U(S) is consistent on its own and is the least such superset.
- T6 both: U(D(S)) is up-closed, group-closed and contains D(S); ``both`` is
  not idempotent.
- T7 enforce: D1(S) is lift-closed if and only if D1(S) = D(S).
- T8 ordering: under A1, ``datajoint.dependencies.topo_sort`` succeeds if and
  only if the group quotient Q is acyclic, and its order is valid.

The tests of ``self.upstream`` for a row not inserted yet come last.

Drawn schemas may contain group cycles (Q cyclic), which E1b refuses at
declaration. T1-T7 and the ``self.upstream`` properties are stated for acyclic Q
and discard the other schemas with ``assume``; T8 tests both cases. Select the
number of examples with ``HYPOTHESIS_PROFILE`` (see ``tests/conftest.py``).
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

import networkx as nx
import pytest
from hypothesis import assume, find, given, settings
from hypothesis import strategies as st

from datajoint.dependencies import extract_master
from datajoint.errors import DataJointError

from .expand_model import (
    DB,
    FK,
    RowGraph,
    Schema,
    TableSpec,
    all_rows,
    apply_delete,
    dependency_graph,
    fk_value,
    full_table_name,
    group_whole,
    make_db,
    q_acyclic,
    ri_ok,
    subdatabase,
    virtual_upstream,
)

#: Attribute values are drawn from this many small integers, so that keys collide.
DOMAIN = 3
#: Tier prefixes of DataJoint table names: manual, lookup, imported, computed.
TIER_PREFIXES = ("", "#", "_", "__")


# =============================================================================
# Generators
# =============================================================================


class _TableBuilder:
    """Collects the attributes and FKs of one table while it is drawn."""

    def __init__(self, name: str, master: str | None = None) -> None:
        self.name = name
        self.master = master
        self.pk: list[str] = []
        self.secondary: list[str] = []
        self.fks: list[FK] = []

    def has(self, attr: str) -> bool:
        return attr in self.pk or attr in self.secondary

    def add_fk(self, parent: TableSpec, primary: bool, renames: dict, nullable: bool = False) -> None:
        # An FK adds only the attributes not declared yet, as in declare.py, so
        # unrenamed attributes shared with an earlier FK are reused.
        attr_map = {}
        for attr in parent.pk:
            name = renames.get(attr, attr)
            attr_map[name] = attr
            if not self.has(name):
                (self.pk if primary else self.secondary).append(name)
        if any(f.parent == parent.name and dict(f.attr_map) == attr_map for f in self.fks):
            return  # an identical FK adds nothing
        self.fks.append(FK(self.name, parent.name, attr_map, nullable=nullable and not primary))

    def spec(self) -> TableSpec:
        return TableSpec(self.name, tuple(self.pk), tuple(self.secondary), self.master)


def _draw_renames(draw, parent: TableSpec, fresh: itertools.count, keep: tuple = ()) -> dict:
    """Rename none or some of the parent's key attributes, never those in ``keep``."""
    candidates = [a for a in parent.pk if a not in keep]
    if not candidates or not draw(st.booleans()):
        return {}
    renamed = draw(st.sets(st.sampled_from(candidates), min_size=1))
    return {a: f"{a}_r{next(fresh)}" for a in sorted(renamed)}


def _descendants(fks: list[FK], names: set[str]) -> set[str]:
    """Tables that reference any of ``names``, directly or through other tables."""
    found, frontier = set(), set(names)
    while frontier:
        frontier = {fk.child for fk in fks if fk.parent in frontier} - found - names
        found |= frontier
    return found


def _draw_fks(draw, tables, fks, fresh, n, solid, group=None) -> list:
    """
    Draw ``n`` FKs from the table being built to tables declared earlier.

    A uniform choice of parent rarely makes the shapes that matter, so the
    parent is drawn from one of several pools: every table; tables whose key
    shares some but not all of its attributes with this table's non-null
    attributes ``solid`` (diamonds and, for a nullable FK, partial NULLs); and,
    for a part, its own group (renamed references to the master or a sibling),
    the parts of other groups, and the tables downstream of its group (group
    cycles).
    """
    drawn = []
    solid, known = set(solid), set(solid)
    for _ in range(n):
        kind = draw(st.sampled_from(["primary", "secondary", "nullable"]))
        sharing = [t for t in tables if set(t.pk) & solid and not set(t.pk) <= known]
        pools = [tables, sharing]
        if kind == "nullable":
            pools.append(sharing)  # twice as likely, to make partial NULLs
        if group is not None:
            master = group[0]
            pools.append([t for t in tables if t.name in group])
            pools.append([t for t in tables if t.master and t.master != master])
            downstream = _descendants(fks, set(group))
            pools.append([t for t in tables if t.name in downstream])
        parent = draw(st.sampled_from(draw(st.sampled_from([p for p in pools if p]))))
        renames = _draw_renames(draw, parent, fresh)
        attrs = {renames.get(a, a) for a in parent.pk}
        if kind != "nullable":
            solid |= attrs - known
        known |= attrs
        drawn.append((parent, kind == "primary", renames, kind == "nullable"))
    return drawn


def _finish(draw, builder: _TableBuilder, tables: list[TableSpec], fks: list, own_pk: str, force_own: bool) -> TableSpec:
    # Primary FKs first, then the table's own key attribute, then secondary FKs,
    # as a definition lists them above and below the --- line.
    for parent, primary, renames, nullable in fks:
        if primary:
            builder.add_fk(parent, True, renames)
    # Without its own attribute, a table's key is its parents' (one-to-one, as in S1).
    if force_own or not builder.pk or draw(st.sampled_from([True, True, False])):
        builder.pk.append(own_pk)
    for parent, primary, renames, nullable in fks:
        if not primary:
            builder.add_fk(parent, False, renames, nullable)
    # Sometimes add a namesake: an attribute that shares a name with another
    # table's key attribute but belongs to no FK.
    namesakes = sorted({a for t in tables for a in t.pk if not builder.has(a)})
    if namesakes and draw(st.booleans()):
        builder.secondary.append(draw(st.sampled_from(namesakes)))
    return builder.spec()


@st.composite
def schemas(draw, max_tables: int = 7, a1_only: bool = False) -> Schema:
    """
    Draw a schema: tables in declaration order, so the FK graph is a DAG.

    - Each step declares a plain table, a master, or a part of a master declared
      earlier, so parts can be added late, after other tables.
    - A master has 0-3 parts. A part's owning reference is direct (``-> master``
      in the primary key), through a sibling part, secondary, or (unless
      ``a1_only``) it violates A1: it is missing, renamed or nullable.
    - Other FKs are primary or secondary, nullable, renamed and parallel, to any
      table declared earlier. Diamonds that share attributes, part -> part across
      groups, renamed references within a group and group cycles all occur.
    - Some tables get a namesake attribute that belongs to no FK.
    """
    fresh = itertools.count()
    tables: list[TableSpec] = []
    fks: list[FK] = []
    parts_of: dict[str, list[str]] = {}
    a1_parts: dict[str, list[str]] = {}  # parts whose owning reference satisfies A1 by construction
    for i in range(draw(st.integers(min(2, max_tables), max_tables))):
        by_name = {t.name: t for t in tables}
        open_masters = [m for m, parts in parts_of.items() if len(parts) < 3]
        kind = draw(st.sampled_from(["plain", "master"] + (["part", "part"] if open_masters else [])))
        if kind != "part":
            builder = _TableBuilder(f"{draw(st.sampled_from(TIER_PREFIXES))}t{i}")
            extra = _draw_fks(draw, tables, fks, fresh, draw(st.integers(0, 3)) if tables else 0, set())
            spec = _finish(draw, builder, tables, extra, own_pk=f"k{i}", force_own=False)
            if kind == "master":
                parts_of[spec.name], a1_parts[spec.name] = [], []
        else:
            master = draw(st.sampled_from(open_masters))
            mspec = by_name[master]
            builder = _TableBuilder(f"{master}__p{len(parts_of[master])}", master=master)
            owners = [s for s in a1_parts[master] if set(mspec.pk) <= set(by_name[s].pk)]
            modes = ["direct", "secondary"] + (["sibling"] if owners else []) + ([] if a1_only else ["violate"])
            mode = draw(st.sampled_from(modes))
            how = None
            owning = []  # the owning reference, when it is a secondary FK
            if mode == "direct":
                builder.add_fk(mspec, True, {})
            elif mode == "sibling":
                sibling = by_name[draw(st.sampled_from(owners))]
                builder.add_fk(sibling, True, _draw_renames(draw, sibling, fresh, keep=mspec.pk))
            elif mode == "secondary":
                owning = [(mspec, False, {}, False)]
            else:
                how = draw(st.sampled_from(["missing", "renamed", "nullable"]))
                if how == "renamed":
                    builder.add_fk(mspec, True, {a: f"{a}_r{next(fresh)}" for a in mspec.pk})
                elif how == "nullable":
                    owning = [(mspec, False, {}, True)]
            solid = set(builder.pk) | {a for o in owning if not o[3] for a in o[0].pk}
            group = [master] + parts_of[master]
            extra = _draw_fks(draw, tables, fks, fresh, draw(st.integers(0, 3)), solid, group)
            # With a secondary owning reference the master key is not in the
            # primary key, so the part needs a key attribute of its own.
            spec = _finish(draw, builder, tables, owning + extra, own_pk=f"k{i}", force_own=bool(owning))
            parts_of[master].append(spec.name)
            if mode != "violate":
                a1_parts[master].append(spec.name)
        tables.append(spec)
        fks.extend(builder.fks)
    schema = Schema(tables, fks)
    if a1_only:
        assert all(t.name in schema.group for t in schema.tables if t.master), schema
    return schema


def _draw_row(draw, schema: Schema, db: DB, table: TableSpec) -> dict | None:
    """Draw one row whose FK values name existing parent rows or are NULL; None if no row fits."""
    row = {}
    nullable = schema.nullable[table.name]
    for fk in schema.fks_of(table.name):
        pairs = list(fk.attr_map.items())
        if any(row.get(c, 0) is None for c, _ in pairs):
            continue  # an earlier FK set a shared attribute to NULL, so this FK references nothing
        matches = [p for p in db[fk.parent].values() if all(c not in row or row[c] == p[a] for c, a in pairs)]
        can_be_null = any(c in nullable and c not in row for c, _ in pairs)
        options = len(matches) + can_be_null
        if not options:
            return None
        choice = draw(st.integers(0, options - 1))
        if choice < len(matches):
            row.update({c: matches[choice][a] for c, a in pairs})
        else:
            # NULL in the attributes this FK introduced; shared key attributes keep values (partial NULL).
            row.update({c: None for c, _ in pairs if c in nullable and c not in row})
    for attr in table.heading:
        if attr not in row:
            row[attr] = draw(st.integers(0, DOMAIN - 1))
    return row


@st.composite
def databases(draw, schema: Schema, max_rows: int = 4) -> DB:
    """Draw a consistent database: tables filled in declaration order over small integer domains."""
    db = {}
    for table in schema.tables:
        rows = {}
        for _ in range(draw(st.integers(1, max_rows))):
            row = _draw_row(draw, schema, db, table)
            if row is not None:
                rows.setdefault(tuple(row[a] for a in table.pk), row)
        db[table.name] = rows
    return db


@dataclass
class Case:
    """A drawn schema and database, with the row graph built once."""

    schema: Schema
    db: DB
    graph: RowGraph = field(repr=False)

    def __repr__(self) -> str:
        rows = {t: list(r.values()) for t, r in self.db.items() if r}
        return f"Case(\nschema={self.schema!r},\ndb={rows!r})"


@st.composite
def cases(draw, max_tables: int = 7, max_rows: int = 4, acyclic: bool = True, max_total: int | None = None) -> Case:
    """Draw a schema and a database. With ``acyclic``, schemas whose Q has a cycle are discarded."""
    schema = draw(schemas(max_tables))
    if acyclic:
        assume(q_acyclic(schema))
    db = draw(databases(schema, max_rows))
    if max_total is not None:
        assume(len(all_rows(db)) <= max_total)
    return Case(schema, db, RowGraph(schema, db))


def seeds(rows) -> st.SearchStrategy:
    """Draw a set of seed rows from ``rows``."""
    rows = sorted(rows)
    return st.frozensets(st.sampled_from(rows)) if rows else st.just(frozenset())


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
    # A part row references a row of its own group in another master instance,
    # through an FK that renames the master key.
    g = case.graph
    for r in g.rows:
        for p in g.parent(r):
            mr, mp = g.master(r), g.master(p)
            if mr and mp and mr[0] == mp[0] and mr != mp:
                return True
    return False


def _group_parts(s: Schema):
    return [t for t in s.tables if t.master and t.name in s.group]


def _is_owning_direct(s: Schema, fk: FK) -> bool:
    return fk.parent == s.table(fk.child).master and all(fk.attr_map.get(a) == a for a in s.table(fk.parent).pk)


def _renames_master_key(s: Schema, fk: FK) -> bool:
    master = s.group.get(fk.child)
    return (
        master is not None
        and fk.child != master
        and s.group.get(fk.parent) == master
        and any(c != p for c, p in fk.attr_map.items() if p in s.table(master).pk)
    )


SCHEMA_FEATURES = {
    "group cycle": lambda s: not q_acyclic(s),
    "part violating A1": lambda s: any(t.master and t.name not in s.group for t in s.tables),
    "owning reference through a sibling": lambda s: any(
        not any(_is_owning_direct(s, fk) for fk in s.fks_of(t.name)) for t in _group_parts(s)
    ),
    "secondary owning reference": lambda s: any(not set(s.table(t.master).pk) <= set(t.pk) for t in _group_parts(s)),
    "renamed FK to the own master": lambda s: any(
        _renames_master_key(s, fk) and fk.parent == s.group[fk.child] for fk in s.fks
    ),
    "sibling FK renaming the master key": lambda s: any(
        _renames_master_key(s, fk) and fk.parent != s.group[fk.child] for fk in s.fks
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
    """T1: D and U are extensive, monotone and idempotent."""
    g = case.graph
    b = data.draw(seeds(g.rows), label="B")
    a = data.draw(seeds(b), label="A ⊆ B")
    for op in (g.D, g.U):
        assert a <= op(a)
        assert op(a) <= op(b)
        assert op(op(a)) == op(a)


@given(case=cases(), data=st.data())
def test_t2_additivity(case, data):
    """T2: X(A ∪ B) = X(A) ∪ X(B) for X in D, U and both."""
    g = case.graph
    a = data.draw(seeds(g.rows), label="A")
    b = data.draw(seeds(g.rows), label="B")
    for op in (g.D, g.U, g.both):
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


@given(case=tiny_cases, data=st.data())
def test_t5_minimality(case, data):
    """T5: U(S) is the least superset of S that is consistent on its own."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    consistent = [x for x in supersets(g.rows, s) if ri_ok(case.schema, subdatabase(case.db, x)) and g.group_closed(x)]
    u = g.U(s)
    assert u in consistent
    assert all(u <= x for x in consistent)


# =============================================================================
# T6-T7: both and enforce
# =============================================================================


@given(case=cases(), data=st.data())
def test_t6_both(case, data):
    """T6: U(D(S)) is up-closed, group-closed and contains D(S)."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    b = g.both(s)
    assert g.D(s) <= b
    assert g.U(b) == b
    assert g.group_closed(b)
    assert b <= g.both(b)  # extensive, though not idempotent (next test)


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
    """T7: D1(S) is lift-closed if and only if D1(S) = D(S)."""
    g = case.graph
    s = data.draw(seeds(g.rows), label="S")
    d1, d = g.D1(s), g.D(s)
    assert d1 <= d
    assert g.lift_closed(d1) == (d1 == d)


# =============================================================================
# T8: topo_sort and the group quotient
# =============================================================================


def assert_valid_order(schema: Schema, order: list[str], quote: str) -> None:
    """Every parent precedes its children, and each master is followed at once by its parts."""
    name = {t.name: full_table_name(t.name, quote) for t in schema.tables}
    assert sorted(order) == sorted(name.values())
    pos = {n: i for i, n in enumerate(order)}
    for fk in schema.fks:
        assert pos[name[fk.parent]] < pos[name[fk.child]], fk
    for master, parts in schema.a1_parts.items():
        block = order[pos[name[master]] : pos[name[master]] + 1 + len(parts)]
        assert set(block) == {name[master]} | {name[p] for p in parts}, (master, order)


@pytest.mark.parametrize("quote", ["`", '"'])
@given(schema=schemas(a1_only=True))
def test_t8_topo_sort(quote, schema):
    """
    T8: under A1, topo_sort succeeds if and only if Q is acyclic, and its order is valid.

    The graph is a ``Dependencies`` object filled the way ``Dependencies.load``
    fills it, and sorted by ``datajoint.dependencies.topo_sort``. Both MySQL and
    PostgreSQL quoting are used, since ``extract_master`` recognizes parts by name.
    """
    graph = dependency_graph(schema, quote)
    if q_acyclic(schema):
        assert_valid_order(schema, graph.topo_sort(), quote)
    else:
        # E1b will replace NetworkXUnfeasible with a DataJointError that names the cycle.
        with pytest.raises((nx.NetworkXUnfeasible, DataJointError)):
            graph.topo_sort()


@given(schema=schemas())
def test_model_part_names_follow_datajoint(schema):
    """``extract_master`` finds the master of every part in the model, and of no other table."""
    for quote in ("`", '"'):
        for t in schema.tables:
            expected = full_table_name(t.master, quote) if t.master else None
            assert extract_master(full_table_name(t.name, quote)) == expected


def _violating_part_schema(*, nullable_owner: bool) -> Schema:
    if not nullable_owner:
        # M; X references M; part P of M has no reference to M and references X.
        return Schema(
            [TableSpec("m", ("m",)), TableSpec("x", ("x",), ("m",)), TableSpec("m__p", ("p",), ("x",), master="m")],
            [FK("x", "m", {"m": "m"}), FK("m__p", "x", {"x": "x"})],
        )
    # M; part P1 references M through a nullable FK (violates A1); part P2 references M and P1.
    return Schema(
        [
            TableSpec("m", ("m",)),
            TableSpec("m__p1", ("p1",), ("m",), master="m"),
            TableSpec("m__p2", ("m", "p2"), ("p1",), master="m"),
        ],
        [FK("m__p1", "m", {"m": "m"}, nullable=True), FK("m__p2", "m", {"m": "m"}), FK("m__p2", "m__p1", {"p1": "p1"})],
    )


def test_t8_needs_a1_q_acyclic_but_topo_sort_fails():
    """
    Without A1, T8 fails in one direction: Q is acyclic but topo_sort raises.

    P violates A1, so the traversal treats it as a plain table and Q is
    ``M -> X -> P``. ``topo_sort`` still recognizes P as a part by its name and
    adds the edge ``X -> M``, which closes the cycle ``M -> X -> M``.
    """
    schema = _violating_part_schema(nullable_owner=False)
    assert schema.a1_parts == {"m": ()}
    assert q_acyclic(schema)
    with pytest.raises(nx.NetworkXUnfeasible):
        dependency_graph(schema).topo_sort()


def test_t8_needs_a1_q_cyclic_but_topo_sort_succeeds():
    """
    Without A1, T8 fails in the other direction: Q has a cycle but topo_sort succeeds.

    P1 references M through a nullable FK, so it violates A1 and is its own node
    in Q. Q is then ``{M, P2} -> P1 -> {M, P2}``, a cycle, so Q has no
    topological order for the one-pass plan to follow: a row of P1 belongs to
    one master instance by its FK and is referenced by a part row of another.
    ``topo_sort`` sees both P1 and P2 as parts of M by name, adds no edge, and
    succeeds. The E1b rule as planned allows this schema, since P1 references
    its master and P2 references its master and a sibling part.
    """
    schema = _violating_part_schema(nullable_owner=True)
    assert schema.a1_parts == {"m": ("m__p2",)}
    assert not q_acyclic(schema)
    # topo_sort keeps the parts by name after their master, so P1 sits inside the block of M.
    assert dependency_graph(schema).topo_sort() == [full_table_name(n) for n in ("m", "m__p1", "m__p2")]
    # The rows show the cycle: P1 row 5 belongs to master 1 by its FK, and P2 row (2, 1) of master 2 references it.
    db = make_db(schema, {"m": [(1,), (2,)], "m__p1": [(5, 1)], "m__p2": [(2, 1, 5)]})
    g = RowGraph(schema, db)
    assert g.D({("m", (1,))}) == {("m", (1,)), ("m__p1", (5,)), ("m__p2", (2, 1)), ("m", (2,))}


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
