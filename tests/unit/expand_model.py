"""
Executable reference model of ``Diagram.expand``.

This module is an executable copy of the traversal specification (the
"Traversal specification" section of ``diagram.md``). It holds rows in plain
Python dictionaries, needs no database, and defines the row sets that the
engine must produce:

- ``D(S)``: the least superset of the seed rows ``S`` closed under ``down``.
  This is ``expand(seed, "down")`` and ``cascade(seed, part_integrity="cascade")``.
- ``D1(S)``: the closure under ``child`` alone, which ``part_integrity="ignore"``
  deletes and ``"enforce"`` deletes when it is lift-closed.
- ``U(S)``: the least superset closed under ``up``. This is ``expand(seed, "up")``
  and ``trace(seed)``.
- ``both(S) = U(D(S))``, which is ``expand(seed, "both")``.

Each operation is a plain breadth-first search over row identifiers
``(table, pk_tuple)``. ``test_expand_spec.py`` checks the model against the
theorems of the specification, and later test layers check the engine against
the model.

The module also models parts of the specification's "Computation" section, so
that its claims can be checked against the definitions: the table-level reach
(the node set of an expanded diagram), the one pass over the group quotient with
its iterating groups, and the rules under which R1 may copy a condition or
relabel materialized keys.

Model
-----
- A schema is a list of tables in declaration order and a list of foreign keys.
  An FK maps child attributes onto the parent's primary key (``attr_map``).
  Parallel FKs between the same two tables are separate edges.
- A2: an FK with any NULL column references nothing, as in SQL's MATCH SIMPLE.
  FKs can share columns, so partial NULLs occur.
- A1, owned parts: a part is owned by its master if an FK in its primary key
  references the master or an owned part of the same master, renames allowed.
  The chain maps the part's owning attributes onto the master's key. A master
  and its owned parts form its group; a part that is not owned is a plain table.
- A database is ``{table: {pk_tuple: row_dict}}``, where ``pk_tuple`` lists the
  primary-key values in the order of ``TableSpec.pk``.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping

import networkx as nx

from datajoint.dependencies import Dependencies

#: A row identifier: ``(table name, primary-key tuple)``.
Row = tuple
#: A database: ``{table: {pk_tuple: row_dict}}``.
DB = dict


# =============================================================================
# Schema
# =============================================================================


@dataclass(frozen=True)
class TableSpec:
    """
    One table of a model schema.

    Parameters
    ----------
    name : str
        Table name. A part's name is ``<master>__<part>``, as in DataJoint, so
        that ``datajoint.dependencies.extract_master`` recognizes it.
    pk : tuple[str, ...]
        Primary-key attributes, in order.
    secondary : tuple[str, ...], optional
        Secondary attributes.
    master : str, optional
        Name of the master table if this table is a part.
    """

    name: str
    pk: tuple[str, ...]
    secondary: tuple[str, ...] = ()
    master: str | None = None

    @property
    def heading(self) -> tuple[str, ...]:
        """All attributes: the primary key, then the secondary attributes."""
        return self.pk + self.secondary


@dataclass(frozen=True)
class FK:
    """
    One foreign key, an edge ``parent -> child`` of the dependency graph.

    Parameters
    ----------
    child : str
        The referencing table.
    parent : str
        The referenced table.
    attr_map : dict[str, str]
        Maps each child attribute of the FK to the parent primary-key attribute
        it references. An unrenamed FK maps every attribute to itself.
    nullable : bool, optional
        Whether the FK was declared ``-> [nullable]``. The attributes it
        introduces may then hold NULL.
    """

    child: str
    parent: str
    attr_map: Mapping[str, str] = field(hash=False)
    nullable: bool = False

    @property
    def renamed(self) -> bool:
        """True if any child attribute has a different name in the parent."""
        return any(c != p for c, p in self.attr_map.items())


class Schema:
    """
    Tables in declaration order and the foreign keys between them.

    Parameters
    ----------
    tables : iterable of TableSpec
        Tables in declaration order.
    fks : iterable of FK
        Foreign keys. Each child's FKs are listed in declaration order.

    Notes
    -----
    Three facts are derived from the declarations:

    - **Nullable attributes.** A secondary attribute is nullable when the first
      FK that includes it is nullable, as in ``declare.py``, where an FK adds
      only the attributes not declared yet. Primary-key attributes and
      attributes outside every FK are never NULL.
    - **Owned parts and groups** (A1). A part is owned if an FK in its primary
      key (every attribute of the FK in the primary key, as ``Dependencies``
      marks it ``primary``) references the master or an owned part of the same
      master, renamed or not. Each master that has parts forms a group with its
      owned parts; ``group[t]`` names the master of table ``t`` for the master
      and for its owned parts. A part that is not owned is in no group.
    - **Owning attributes.** ``owning[t]`` maps each attribute of the master's
      key to the attribute of ``t`` that the owning chain maps onto it: the
      identity for the master. If a part has several owning references, an
      unrenamed ``-> master`` wins, and otherwise the first in its definition.
    """

    def __init__(self, tables: Iterable[TableSpec], fks: Iterable[FK]) -> None:
        self.tables = tuple(tables)
        self.fks = tuple(fks)
        self._by_name = {t.name: t for t in self.tables}
        if len(self._by_name) != len(self.tables):
            raise ValueError("table names must be unique")
        self._validate()
        self.nullable = {t.name: self._nullable_attributes(t) for t in self.tables}
        self.owned_parts = {}
        self.group = {}
        self.owning = {}
        for t in self.tables:
            parts = [p for p in self.tables if p.master == t.name]
            if parts:
                owning = self._find_owned(t, parts)
                self.owned_parts[t.name] = tuple(p.name for p in parts if p.name in owning)
                self.owning.update(owning)
                self.group.update({name: t.name for name in owning})

    def __repr__(self) -> str:
        lines = ["Schema("]
        for t in self.tables:
            master = f", master={t.master!r}" if t.master else ""
            lines.append(f"  TableSpec({t.name!r}, pk={t.pk!r}, secondary={t.secondary!r}{master}),")
        for fk in self.fks:
            null = ", nullable=True" if fk.nullable else ""
            lines.append(f"  FK({fk.child!r}, {fk.parent!r}, {dict(fk.attr_map)!r}{null}),")
        return "\n".join(lines) + "\n)"

    def table(self, name: str) -> TableSpec:
        """Return the table named ``name``."""
        return self._by_name[name]

    def fks_of(self, child: str) -> list[FK]:
        """Return the FKs declared in ``child``, in declaration order."""
        return [fk for fk in self.fks if fk.child == child]

    def is_primary(self, fk: FK) -> bool:
        """True if every attribute of the FK is in the child's primary key (as in ``Dependencies``)."""
        return set(fk.attr_map) <= set(self.table(fk.child).pk)

    def owning_attributes(self, table: str) -> tuple[str, ...] | None:
        """Attributes of ``table`` that hold its master's key, in the master's key order; None if in no group."""
        master = self.group.get(table)
        if master is None:
            return None
        return tuple(self.owning[table][a] for a in self.table(master).pk)

    def intra_group_fks(self, master: str) -> list[FK]:
        """FKs between two different tables of the group of ``master``."""
        return [
            fk
            for fk in self.fks
            if fk.child != fk.parent and self.group.get(fk.child) == master and self.group.get(fk.parent) == master
        ]

    def _validate(self) -> None:
        declared = set()
        for t in self.tables:
            if not t.pk:
                raise ValueError(f"{t.name} has no primary key")
            if len(set(t.heading)) != len(t.heading):
                raise ValueError(f"{t.name} repeats an attribute")
            if t.master is not None:
                if t.master not in declared or self.table(t.master).master is not None:
                    raise ValueError(f"{t.name}: master {t.master} must be a table declared earlier, not a part")
                if not t.name.startswith(t.master + "__"):
                    raise ValueError(f"part {t.name} must be named {t.master}__<part>")
            declared.add(t.name)
        for fk in self.fks:
            child, parent = self.table(fk.child), self.table(fk.parent)
            if not set(fk.attr_map) <= set(child.heading):
                raise ValueError(f"{fk}: attributes missing from {child.name}")
            if sorted(fk.attr_map.values()) != sorted(parent.pk):
                raise ValueError(f"{fk}: must map onto the whole primary key of {parent.name}")

    def _nullable_attributes(self, table: TableSpec) -> frozenset[str]:
        seen, nullable = set(), set()
        for fk in self.fks_of(table.name):
            for attr in fk.attr_map:
                if attr not in seen:
                    seen.add(attr)
                    if fk.nullable and attr not in table.pk:
                        nullable.add(attr)
        return frozenset(nullable)

    def _find_owned(self, master: TableSpec, parts: list[TableSpec]) -> dict[str, dict[str, str]]:
        # Parts are taken in declaration order, so an owning reference to another
        # part is decided after that part (an FK references a table declared earlier).
        owning = {master.name: {a: a for a in master.pk}}
        for part in parts:
            chains = [fk for fk in self.fks_of(part.name) if fk.parent in owning and self.is_primary(fk)]
            if not chains:
                continue
            unrenamed = [fk for fk in chains if fk.parent == master.name and not fk.renamed]
            fk = (unrenamed or chains)[0]
            by_parent_attr = {p: c for c, p in fk.attr_map.items()}
            owning[part.name] = {a: by_parent_attr[owning[fk.parent][a]] for a in master.pk}
        return owning


# =============================================================================
# Databases and references
# =============================================================================


def make_db(schema: Schema, data: Mapping[str, Iterable[tuple]]) -> DB:
    """
    Build a database from rows written as tuples in heading order.

    Parameters
    ----------
    schema : Schema
        The schema.
    data : dict[str, iterable of tuple]
        For each table, its rows as tuples of the primary-key values followed by
        the secondary values. Tables left out are empty.

    Returns
    -------
    DB
        ``{table: {pk_tuple: row_dict}}``.
    """
    db = {t.name: {} for t in schema.tables}
    for name, rows in data.items():
        table = schema.table(name)
        for values in rows:
            if len(values) != len(table.heading):
                raise ValueError(f"{name}: expected {table.heading}, got {values}")
            row = dict(zip(table.heading, values))
            key = tuple(row[a] for a in table.pk)
            if key in db[name]:
                raise ValueError(f"{name}: duplicate primary key {key}")
            db[name][key] = row
    return db


def all_rows(db: DB) -> frozenset[Row]:
    """Return the identifiers of every row in ``db``."""
    return frozenset((t, k) for t, rows in db.items() for k in rows)


def fk_value(schema: Schema, fk: FK, row: Mapping) -> tuple | None:
    """
    Return the parent primary key that ``row`` names through ``fk``.

    Returns None if any FK attribute is NULL (A2, MATCH SIMPLE).
    """
    by_parent_attr = {p: c for c, p in fk.attr_map.items()}
    value = tuple(row[by_parent_attr[a]] for a in schema.table(fk.parent).pk)
    return None if any(v is None for v in value) else value


def ref_e(schema: Schema, db: DB, fk: FK, row: Mapping) -> Row | None:
    """
    Return the parent row that ``row`` references through ``fk``.

    Returns None if the FK value has a NULL (A2) or names no existing row.
    """
    value = fk_value(schema, fk, row)
    if value is None or value not in db.get(fk.parent, {}):
        return None
    return (fk.parent, value)


def master_of(schema: Schema, table: str, row: Mapping) -> Row | None:
    """
    ``master(r)``: identifier of the master row of a row in a group.

    The master row is named by the row's owning attributes, which lie in the
    primary key and so are never NULL. Returns None for a row in no group. The
    identifier is returned whether or not the master row exists.
    """
    attrs = schema.owning_attributes(table)
    if attrs is None:
        return None
    return (schema.group[table], tuple(row[a] for a in attrs))


def apply_delete(db: DB, rows: Iterable[Row]) -> DB:
    """Return a copy of ``db`` without ``rows``."""
    gone = set(rows)
    return {t: {k: r for k, r in table.items() if (t, k) not in gone} for t, table in db.items()}


def subdatabase(db: DB, rows: Iterable[Row]) -> DB:
    """Return the database that holds only ``rows`` (an export of them)."""
    keep = set(rows)
    return {t: {k: r for k, r in table.items() if (t, k) in keep} for t, table in db.items()}


# =============================================================================
# Integrity checkers
# =============================================================================


def ri_ok(schema: Schema, db: DB) -> bool:
    """
    P1, referential integrity: every non-NULL FK value names an existing parent row.

    An FK value with any NULL attribute names nothing (A2) and is not checked.
    """
    for fk in schema.fks:
        parent_rows = db.get(fk.parent, {})
        for row in db.get(fk.child, {}).values():
            value = fk_value(schema, fk, row)
            if value is not None and value not in parent_rows:
                return False
    return True


def group_whole(schema: Schema, db: DB) -> bool:
    """
    The static half of P2: no part row exists without its master row.

    This is a property of one database state. P2 for a delete is stronger: the
    delete must remove each master-part instance whole or not at all, which
    ``RowGraph.group_closed`` checks on the deleted rows. Removing one part row
    of an instance leaves a database that still passes this check, yet breaks
    P2. For owned parts the check also follows from ``ri_ok``, since each part
    row references its master along its owning chain.
    """
    for table, master in schema.group.items():
        if table == master:
            continue
        for row in db.get(table, {}).values():
            m = master_of(schema, table, row)
            if m[1] not in db.get(master, {}):
                return False
    return True


def quotient(schema: Schema) -> nx.DiGraph:
    """
    The group quotient Q: the FK graph with each master-part group contracted to one node.

    FKs between two tables of the same group are left out. A part that is not
    owned is in no group and stays its own node. A self-referencing FK is a
    self-loop, which counts as a cycle.
    """
    node = {t.name: schema.group.get(t.name, t.name) for t in schema.tables}
    q = nx.DiGraph()
    q.add_nodes_from(set(node.values()))
    for fk in schema.fks:
        u, v = node[fk.parent], node[fk.child]
        if u != v or fk.parent == fk.child:
            q.add_edge(u, v)
    return q


def q_acyclic(schema: Schema) -> bool:
    """True if the group quotient Q is acyclic."""
    return nx.is_directed_acyclic_graph(quotient(schema))


def table_reach(schema: Schema, tables: Iterable[str], direction: str = "down", group_step: bool = True) -> frozenset:
    """
    The node set of an expanded diagram: the table-level reach of ``tables``.

    The tables reached by following FKs in the direction of travel, together
    with the master and all owned parts of every group that has a reached
    member. ``group_step=False`` leaves the group step out (``D1``). For
    ``"both"``, it is the upward reach of the downward reach.
    """
    if direction == "both":
        return table_reach(schema, table_reach(schema, tables, "down", group_step), "up", group_step)
    if direction not in ("down", "up"):
        raise ValueError(f"direction must be 'down', 'up' or 'both', not {direction!r}")
    seen = set(tables)
    stack = list(seen)
    while stack:
        t = stack.pop()
        if direction == "down":
            nxt = {fk.child for fk in schema.fks if fk.parent == t}
        else:
            nxt = {fk.parent for fk in schema.fks if fk.child == t}
        if group_step and t in schema.group:
            master = schema.group[t]
            nxt |= {master, *schema.owned_parts[master]}
        for n in nxt - seen:
            seen.add(n)
            stack.append(n)
    return frozenset(seen)


# =============================================================================
# Row relations and the operations
# =============================================================================


class PartIntegrityError(Exception):
    """``part_integrity="enforce"`` refuses a delete that would split an instance."""


class RowGraph:
    """
    The row relations of one database and the closures over them.

    Parameters
    ----------
    schema : Schema
        The schema.
    db : DB
        The database.

    Notes
    -----
    - ``child(r) = {c : ref_e(c) = r}`` and ``parent(r) = {ref_e(r)}``, over
      every FK edge e.
    - ``inst(r)`` is the master-part instance of a row in a group: the master
      row and every owned part row whose owning attributes hold the same master
      key. For a row in no group, ``inst(r) = {r}``.
    - ``down = child ∪ inst`` and ``up = parent ∪ inst``, so ``up`` is the
      converse of ``down``.
    """

    def __init__(self, schema: Schema, db: DB) -> None:
        self.schema = schema
        self.db = db
        self.rows = all_rows(db)
        self._parents = {r: set() for r in self.rows}
        self._children = {r: set() for r in self.rows}
        for fk in schema.fks:
            for key, row in db.get(fk.child, {}).items():
                ref = ref_e(schema, db, fk, row)
                if ref is not None:
                    self._parents[(fk.child, key)].add(ref)
                    self._children[ref].add((fk.child, key))
        # Instances are keyed by the master row's identifier, which exists in a
        # consistent database; the key works even when it does not.
        instances = defaultdict(set)
        self._master = {}
        for table, rows in db.items():
            for key, row in rows.items():
                m = master_of(schema, table, row)
                if m is not None:
                    self._master[(table, key)] = m
                    instances[m].add((table, key))
        self._inst = {r: frozenset(instances[m]) for r, m in self._master.items()}

    def _check(self, rows: Iterable[Row]) -> set[Row]:
        rows = set(rows)
        unknown = rows - self.rows
        if unknown:
            raise ValueError(f"rows not in the database: {sorted(unknown)}")
        return rows

    # --- relations -----------------------------------------------------------

    def child(self, r: Row) -> frozenset[Row]:
        """Rows that reference ``r`` through some FK."""
        return frozenset(self._children[r])

    def parent(self, r: Row) -> frozenset[Row]:
        """Rows that ``r`` references through some FK."""
        return frozenset(self._parents[r])

    def inst(self, r: Row) -> frozenset[Row]:
        """The master-part instance of ``r``; ``{r}`` for a row in no group."""
        return self._inst.get(r, frozenset({r}))

    def master(self, r: Row) -> Row | None:
        """``master(r)`` for a row in a group, else None."""
        return self._master.get(r)

    def down(self, r: Row) -> frozenset[Row]:
        """``down = child ∪ inst``."""
        return self.child(r) | self.inst(r)

    def up(self, r: Row) -> frozenset[Row]:
        """``up = parent ∪ inst``."""
        return self.parent(r) | self.inst(r)

    # --- operations ----------------------------------------------------------

    def closure(self, seed: Iterable[Row], step: Callable[[Row], Iterable[Row]]) -> frozenset[Row]:
        """The least superset of ``seed`` closed under ``step``, by breadth-first search."""
        seen = self._check(seed)
        queue = deque(seen)
        while queue:
            for s in step(queue.popleft()):
                if s not in seen:
                    seen.add(s)
                    queue.append(s)
        return frozenset(seen)

    def D(self, seed: Iterable[Row]) -> frozenset[Row]:
        """``expand(seed, "down")``: the closure under ``down``."""
        return self.closure(seed, self.down)

    def D1(self, seed: Iterable[Row]) -> frozenset[Row]:
        """The closure under ``child`` alone, with no group rule."""
        return self.closure(seed, self.child)

    def U(self, seed: Iterable[Row]) -> frozenset[Row]:
        """``expand(seed, "up")``: the closure under ``up``."""
        return self.closure(seed, self.up)

    def both(self, seed: Iterable[Row]) -> frozenset[Row]:
        """``expand(seed, "both") = U(D(seed))``."""
        return self.U(self.D(seed))

    def delete(self, seed: Iterable[Row], part_integrity: str = "enforce") -> frozenset[Row]:
        """
        The rows ``delete(part_integrity=...)`` removes.

        ``"cascade"`` deletes D(S) and ``"ignore"`` deletes D1(S). ``"enforce"``
        deletes D1(S) if it is lift-closed, and otherwise raises
        ``PartIntegrityError`` before deleting anything, naming a part row and
        its master.
        """
        if part_integrity == "cascade":
            return self.D(seed)
        if part_integrity not in ("ignore", "enforce"):
            raise ValueError(f"part_integrity must be 'enforce', 'ignore' or 'cascade', not {part_integrity!r}")
        rows = self.D1(seed)
        if part_integrity == "enforce":
            for r in sorted(rows):
                m = self._master.get(r)
                if m is not None and m != r and m not in rows:
                    raise PartIntegrityError(f"{r} would be deleted without its master {m}")
        return rows

    # --- group predicates ----------------------------------------------------

    def lift_closed(self, rows: Iterable[Row]) -> bool:
        """True if every part row in ``rows`` that belongs to a group has its master row in ``rows``."""
        rows = set(rows)
        return all(self._master[r] in rows for r in rows if r in self._master and self._master[r] != r)

    def group_closed(self, rows: Iterable[Row]) -> bool:
        """
        True if ``rows`` takes every master-part instance it touches whole.

        This is P2 for an operation: a delete or an export of ``rows`` keeps
        each instance whole if and only if this holds.
        """
        rows = set(rows)
        return all(self.inst(r) <= rows for r in rows)


# =============================================================================
# The one pass over the group quotient
# =============================================================================


def preserves_instance(schema: Schema, fk: FK) -> bool:
    """
    True if an FK inside a group maps the referencing part's owning attributes
    onto the referenced table's owning attributes, so it stays in the instance.

    A group iterates if and only if some FK inside it does not.
    """
    master = schema.group[fk.child]
    child_owning, parent_owning = schema.owning[fk.child], schema.owning[fk.parent]
    return all(fk.attr_map.get(child_owning[a]) == parent_owning[a] for a in schema.table(master).pk)


def iterates(schema: Schema, master: str) -> bool:
    """Whether the group of ``master`` iterates: some FK inside it does not preserve the instance."""
    return not all(preserves_instance(schema, fk) for fk in schema.intra_group_fks(master))


def one_pass(
    schema: Schema, db: DB, seed: Iterable[Row], direction: str = "down", iterate: bool = True
) -> tuple[frozenset[Row], dict]:
    """
    The engine's computation, evaluated on rows: one visit per node of Q.

    The nodes of Q are visited in topological order for ``"down"`` and in
    reverse order for ``"up"``. A plain table takes its seed rows and the rows
    that R1 reaches from its neighbors on the incoming side. A group takes the
    master keys of its seed rows and of the rows R1 reaches in any of its tables
    (the lift, through the owning attributes), then all rows of those instances
    (the re-expansion). A group that iterates repeats the group step, following
    the FKs inside the group, until no new master key appears.

    Parameters
    ----------
    schema, db : Schema, DB
        The schema, whose Q must be acyclic, and the database.
    seed : iterable of Row
        The seed rows.
    direction : {"down", "up"}
        The direction of travel.
    iterate : bool
        Whether the groups that ``iterates`` selects repeat the group step. With
        False, every group takes one step.

    Returns
    -------
    rows : frozenset
        The rows reached. They equal D(seed) or U(seed) when the computation is exact.
    rounds : dict
        For each group, the master keys after each group step.
    """
    q = quotient(schema)
    if not nx.is_directed_acyclic_graph(q):
        raise ValueError("the group quotient has a cycle")
    graph = RowGraph(schema, db)
    seed = graph._check(seed)
    order = list(nx.topological_sort(q))
    if direction == "up":
        order.reverse()
    node_of = {t.name: schema.group.get(t.name, t.name) for t in schema.tables}
    members = defaultdict(list)
    for t in schema.tables:
        members[node_of[t.name]].append(t.name)

    def rows_of(table):
        return {(table, k) for k in db[table]}

    def ref(fk, r):
        return ref_e(schema, db, fk, db[r[0]][r[1]])

    def crossing(fk, reached):
        # R1 across one FK: children of reached parents (down) or parents of reached children (up).
        if direction == "down":
            return {c for c in rows_of(fk.child) if ref(fk, c) in reached}
        return {p for c in reached if c[0] == fk.child for p in [ref(fk, c)] if p is not None}

    reached, rounds = set(), {}
    for node in order:
        mem = members[node]
        incoming = {r for r in seed if r[0] in mem}
        for fk in schema.fks:
            inside = fk.child if direction == "down" else fk.parent
            outside = fk.parent if direction == "down" else fk.child
            if inside in mem and node_of[outside] != node:
                incoming |= crossing(fk, reached)
        if node not in schema.owned_parts:
            reached |= incoming
            continue
        inner = schema.intra_group_fks(node)
        repeat = iterate and iterates(schema, node)
        keys = {graph.master(r) for r in incoming}
        history = [set(keys)]
        while True:
            instance = {r for t in mem for r in rows_of(t) if graph.master(r) in keys}
            if not repeat:
                break
            step = set().union(*(crossing(fk, instance) for fk in inner))
            grown = keys | {graph.master(r) for r in step}
            if grown == keys:
                break
            keys = grown
            history.append(set(keys))
        rounds[node] = history
        reached |= instance
    return frozenset(reached), rounds


# =============================================================================
# Conditions: R1 copies and relabeled keys
# =============================================================================


@dataclass(frozen=True)
class Relation:
    """
    A query expression used as a condition: rows over ``heading``.

    Restricting a table by it keeps the rows whose values on the shared
    attributes appear in it; with no shared attribute, it keeps every row if it
    has any rows.
    """

    heading: tuple[str, ...]
    rows: frozenset = frozenset()


@dataclass(frozen=True)
class Not:
    """
    The complement of a condition: ``table - cond``, or ``table & dj.Not(cond)``.

    The complement of a relation follows SQL's ``NOT IN``: a NULL on either side
    makes it unknown, so the row is not kept. With several shared attributes,
    any NULL counts as unknown, which is exact for one shared attribute.
    """

    cond: object


def condition_attributes(cond) -> frozenset[str]:
    """The attributes a condition names: a dict's keys, a relation's heading."""
    if isinstance(cond, dict):
        return frozenset(cond)
    if isinstance(cond, (list, tuple)):
        return frozenset().union(*(condition_attributes(c) for c in cond)) if cond else frozenset()
    if isinstance(cond, Relation):
        return frozenset(cond.heading)
    if isinstance(cond, Not):
        return condition_attributes(cond.cond)
    raise TypeError(f"unsupported condition {cond!r}")


def satisfies(row: Mapping, heading: tuple[str, ...], cond) -> bool:
    """
    Whether ``row`` of a table with ``heading`` passes the restriction by ``cond``.

    As in DataJoint: a dict compares the keys that are attributes of the table
    and ignores the others, and None matches only NULL; a list is the OR of its
    elements, so the empty list matches nothing.
    """
    if isinstance(cond, dict):
        return all(row[k] == v for k, v in cond.items() if k in heading)
    if isinstance(cond, (list, tuple)):
        return any(satisfies(row, heading, c) for c in cond)
    if isinstance(cond, Relation):
        shared = [i for i, a in enumerate(cond.heading) if a in heading]
        if not shared:
            return bool(cond.rows)
        value = tuple(row[cond.heading[i]] for i in shared)
        return None not in value and value in {tuple(r[i] for i in shared) for r in cond.rows}
    if isinstance(cond, Not):
        inner = cond.cond
        if isinstance(inner, Relation):
            shared = [i for i, a in enumerate(inner.heading) if a in heading]
            if shared and inner.rows:
                value = [row[inner.heading[i]] for i in shared]
                if None in value or any(r[i] is None for r in inner.rows for i in shared):
                    return False  # x NOT IN (..., NULL) is never true
        return not satisfies(row, heading, inner)
    raise TypeError(f"unsupported condition {cond!r}")


def restrict(schema: Schema, db: DB, table: str, cond) -> frozenset[Row]:
    """The rows of ``table`` that pass the restriction by ``cond``."""
    heading = schema.table(table).heading
    return frozenset((table, k) for k, row in db[table].items() if satisfies(row, heading, cond))


def r1_down(schema: Schema, db: DB, fk: FK, parent_rows: Iterable[Row]) -> frozenset[Row]:
    """R1 downward across ``fk``: the child rows that reference one of ``parent_rows``."""
    parents = set(parent_rows)
    return frozenset((fk.child, k) for k, row in db[fk.child].items() if ref_e(schema, db, fk, row) in parents)


def r1_up(schema: Schema, db: DB, fk: FK, child_rows: Iterable[Row]) -> frozenset[Row]:
    """R1 upward across ``fk``: the parent rows that one of ``child_rows`` references."""
    refs = {ref_e(schema, db, fk, db[fk.child][k]) for t, k in child_rows if t == fk.child}
    return frozenset(refs - {None})


def copy_allowed(schema: Schema, fk: FK, cond) -> bool:
    """
    R1's rule for copying a parent's condition to the child unchanged.

    The attributes the condition names are among the FK's attributes, the FK is
    not renamed, and none of its columns is nullable.
    """
    return (
        condition_attributes(cond) <= set(fk.attr_map) and not fk.renamed and not set(fk.attr_map) & schema.nullable[fk.child]
    )


def relabel_down(fk: FK, keys: list[dict]) -> list[dict]:
    """Relabel parent keys for the child: keep the keys the FK carries, renamed through ``attr_map``."""
    by_parent_attr = {p: c for c, p in fk.attr_map.items()}
    return [{by_parent_attr[a]: v for a, v in key.items() if a in by_parent_attr} for key in keys]


def relabel_up(fk: FK, keys: list[dict]) -> list[dict]:
    """Relabel child keys for the parent: keep the keys the FK carries, renamed through ``attr_map``."""
    return [{fk.attr_map[c]: v for c, v in key.items() if c in fk.attr_map} for key in keys]


def relabel_down_allowed(schema: Schema, fk: FK, keys: list[dict]) -> bool:
    """
    The Seeds rule for relabeling parent keys downward.

    Every key names, among the parent's attributes, only attributes the FK
    carries; and if the FK has a nullable column, every key names all of the
    FK's attributes, none of them NULL.
    """
    parent = schema.table(fk.parent)
    carried = set(fk.attr_map.values())
    if any(set(key) & set(parent.heading) - carried for key in keys):
        return False
    if not set(fk.attr_map) & schema.nullable[fk.child]:
        return True
    return all(carried <= set(key) and all(key[a] is not None for a in carried) for key in keys)


# =============================================================================
# self.upstream for a row not inserted yet
# =============================================================================


def relabel(schema: Schema, fk: FK, key: Mapping) -> tuple | None:
    """
    ``relabel_e(key)``: the parent primary key named by a child's primary-key values.

    ``fk`` must be a primary FK of the child, so ``key`` holds every one of its
    attributes. Returns None if a value is NULL.
    """
    return fk_value(schema, fk, key)


def virtual_upstream(schema: Schema, db: DB, table: str, key: Mapping) -> frozenset[Row]:
    """
    ``self.upstream`` for a row of ``table`` with primary key ``key``, inserted or not.

    ``make()`` runs before its row is inserted, and ``U(∅) = ∅``, so the upstream
    of the row is built from its primary FKs instead: the union, over each FK e
    whose attributes all lie in the primary key, of
    ``U(rows of P_e matching relabel_e(key))``. Secondary FKs are unknown before
    the insert and are left out.

    Parameters
    ----------
    schema : Schema
        The schema.
    db : DB
        The database.
    table : str
        The table that ``make()`` fills.
    key : dict
        The primary-key values of the row.

    Returns
    -------
    frozenset
        The upstream rows. The row itself is not included.
    """
    graph = RowGraph(schema, db)
    upstream = frozenset()
    for fk in schema.fks_of(table):
        if schema.is_primary(fk):
            value = relabel(schema, fk, key)
            matching = {(fk.parent, value)} if value is not None and value in db.get(fk.parent, {}) else set()
            upstream |= graph.U(matching)
    return upstream


# =============================================================================
# The dependency graph, built the way Dependencies.load builds it
# =============================================================================


def full_table_name(name: str, quote: str = "`", database: str = "model") -> str:
    """Full table name as ``Dependencies`` stores it, e.g. ```model`.`t0__p1```."""
    return f"{quote}{database}{quote}.{quote}{name}{quote}"


def dependency_graph(schema: Schema, quote: str = "`", database: str = "model") -> Dependencies:
    """
    Build the ``Dependencies`` graph that ``Dependencies.load`` would build for ``schema``.

    Nodes are full table names with a ``primary_key`` attribute. Each FK is one
    edge ``parent -> child`` keyed by the tuple of its child attributes, with the
    edge attributes ``primary``, ``attr_map``, ``aliased`` and ``multi``.
    ``quote`` is the backtick for MySQL or the double quote for PostgreSQL.
    """
    graph = Dependencies()  # no connection: nodes and edges are added by hand
    for t in schema.tables:
        graph.add_node(full_table_name(t.name, quote, database), primary_key=set(t.pk))
    for fk in schema.fks:
        child_pk = set(schema.table(fk.child).pk)
        graph.add_edge(
            full_table_name(fk.parent, quote, database),
            full_table_name(fk.child, quote, database),
            key=tuple(fk.attr_map),
            primary=set(fk.attr_map) <= child_pk,
            attr_map=dict(fk.attr_map),
            aliased=fk.renamed,
            multi=set(fk.attr_map) != child_pk,
        )
    return graph
