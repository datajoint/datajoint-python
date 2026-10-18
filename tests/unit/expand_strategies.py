"""
Hypothesis strategies for the ``Diagram.expand`` tests.

They draw schemas in the reference model's format (``expand_model.py``),
consistent databases over small integer domains, seed rows, and restriction
conditions. ``test_expand_spec.py`` checks the model against the
specification with them, and ``test_expand_plan.py`` checks the planner against
the model.
"""

from __future__ import annotations

import itertools
from dataclasses import dataclass, field

from hypothesis import assume
from hypothesis import strategies as st

from .expand_model import DB, FK, Not, Relation, RowGraph, Schema, TableSpec, all_rows, q_acyclic

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
def schemas(draw, max_tables: int = 7, owned_only: bool = False) -> Schema:
    """
    Draw a schema: tables in declaration order, so the FK graph is a DAG.

    - Each step declares a plain table, a master, or a part of a master declared
      earlier, so parts can be added late, after other tables.
    - A master has 0-3 parts. A part is owned through ``-> master`` in its
      primary key, a renamed one, or a primary-key FK to an owned sibling with
      or without renames (#1429). Unless ``owned_only``, a part may also be not
      owned: it references the master only below the line, or through a
      nullable FK, or through a part that is not owned, or not at all.
    - Other FKs are primary or secondary, nullable, renamed and parallel, to any
      table declared earlier. Diamonds that share attributes, part -> part across
      groups, references inside a group that link instances, and group cycles
      all occur.
    - Some tables get a namesake attribute that belongs to no FK.
    """
    fresh = itertools.count()
    tables: list[TableSpec] = []
    fks: list[FK] = []
    parts_of: dict[str, list[str]] = {}
    for i in range(draw(st.integers(min(2, max_tables), max_tables))):
        by_name = {t.name: t for t in tables}
        open_masters = [m for m, parts in parts_of.items() if len(parts) < 3]
        kind = draw(st.sampled_from(["plain", "master"] + (["part", "part"] if open_masters else [])))
        if kind != "part":
            builder = _TableBuilder(f"{draw(st.sampled_from(TIER_PREFIXES))}t{i}")
            extra = _draw_fks(draw, tables, fks, fresh, draw(st.integers(0, 3)) if tables else 0, set())
            spec = _finish(draw, builder, tables, extra, own_pk=f"k{i}", force_own=False)
            if kind == "master":
                parts_of[spec.name] = []
        else:
            master = draw(st.sampled_from(open_masters))
            mspec = by_name[master]
            builder = _TableBuilder(f"{master}__p{len(parts_of[master])}", master=master)
            owned = Schema(tables, fks).owned_parts.get(master, ())
            not_owned = [p for p in parts_of[master] if p not in owned]
            owner_modes = ["direct", "renamed"] + (["sibling"] if owned else [])
            ways_not_owned = ["secondary", "nullable", "missing"] + (["through a part not owned"] if not_owned else [])
            mode = draw(st.sampled_from(owner_modes + ([] if owned_only else ["not owned"])))
            if mode == "not owned":
                mode = draw(st.sampled_from(ways_not_owned))
            below_line = []  # a reference to the master below the line
            if mode == "direct":
                builder.add_fk(mspec, True, {})
            elif mode == "renamed":
                renamed = draw(st.sets(st.sampled_from(mspec.pk), min_size=1))
                builder.add_fk(mspec, True, {a: f"{a}_r{next(fresh)}" for a in sorted(renamed)})
            elif mode == "sibling":
                sibling = by_name[draw(st.sampled_from(owned))]
                builder.add_fk(sibling, True, _draw_renames(draw, sibling, fresh))
            elif mode == "through a part not owned":
                other = by_name[draw(st.sampled_from(not_owned))]
                builder.add_fk(other, True, _draw_renames(draw, other, fresh))
            elif mode in ("secondary", "nullable"):
                below_line = [(mspec, False, {}, mode == "nullable")]
            solid = set(builder.pk) | {a for o in below_line if not o[3] for a in o[0].pk}
            group = [master] + parts_of[master]
            extra = _draw_fks(draw, tables, fks, fresh, draw(st.integers(0, 3)), solid, group)
            # With the master referenced below the line, the part needs a key attribute of its own.
            spec = _finish(draw, builder, tables, below_line + extra, own_pk=f"k{i}", force_own=bool(below_line))
            parts_of[master].append(spec.name)
        tables.append(spec)
        fks.extend(builder.fks)
    schema = Schema(tables, fks)
    if owned_only:
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


# =============================================================================
# Conditions
# =============================================================================


VALUES = st.sampled_from([0, 1, 2, None])


def key_dicts(names: list[str]) -> st.SearchStrategy:
    """A dict over some of ``names``, with values that may be NULL."""
    return st.lists(st.sampled_from(names), unique=True, max_size=3).flatmap(
        lambda keys: st.fixed_dictionaries({k: VALUES for k in keys})
    )


def relations(names: list[str]) -> st.SearchStrategy:
    """A query expression used as a condition: a few rows over some of ``names``."""

    def over(heading):
        rows = st.frozensets(st.tuples(*[VALUES] * len(heading)), max_size=4) if heading else st.just(frozenset({()}))
        return rows.map(lambda r: Relation(tuple(heading), r))

    return st.lists(st.sampled_from(names), unique=True, max_size=3).flatmap(over)


def conditions(names: list[str]) -> st.SearchStrategy:
    """A dict, a list of dicts, a query expression (Relation) or a Not, over ``names``."""
    return st.one_of(
        key_dicts(names),
        st.lists(key_dicts(names), max_size=2),
        relations(names),
        relations(names).map(Not),
        key_dicts(names).map(Not),
    )
