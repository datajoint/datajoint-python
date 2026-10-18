"""
The traversal planner behind ``Diagram.expand``.

``Diagram.expand(seed, direction)`` selects, in each table it reaches, the rows
that the traversal specification defines: ``D(S)``, the seed rows and every row
that depends on them (``"down"``); ``U(S)``, the seed rows and every row they
depend on (``"up"``); and ``U(D(S))`` (``"both"``). The specification is the
"Traversal specification" section of the Diagram specification
(``reference/specs/diagram.md`` in datajoint-docs); its shape catalog,
``reference/specs/traversal-shapes.md``, works through one example per schema
shape.

The rows are computed in three steps, as the specification's "Computation"
section describes them:

1. A **schema description** (:class:`TraversalSchema`) holds the tables, their
   primary keys and nullable attributes, the foreign keys with their attribute
   maps, and each master's owned parts with their owning attributes (A1). It is
   plain data: :meth:`TraversalSchema.from_dependencies` builds it from
   ``connection.dependencies`` and the table headings, and tests build it by hand.
2. The **planner** (:func:`plan_expand`) builds a :class:`Plan`: an intermediate
   form, not SQL, whose nodes are :class:`Seed`, :class:`Literal`,
   :class:`Restrict`, :class:`Lift`, :class:`Reexpand` and :class:`Or`. A node
   refers to the restrictions it uses by a handle, the index of another node. The
   planner makes one pass over the group quotient (R1, the edge rule, and R2, the
   group rule) and marks the groups that iterate. It reads no rows.
3. An **evaluator** gives the plan its rows. :func:`compile_plan` turns it into
   query expressions; :class:`RowEvaluator` evaluates it on rows held in memory,
   which is how the test suite compares it with the definitions.

Compilation reads rows only for the groups that iterate. A ``dj.Top`` seed is
replaced by the keys it selects before planning (:func:`seed_from_expression`).
Materialization before a delete is not done here; ``Table.delete`` does not use
this module yet.
"""

from __future__ import annotations

import copy
import inspect
import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Mapping

import networkx as nx

from .condition import AndList, make_condition
from .dependencies import extract_master
from .errors import DataJointError

#: The directions of ``Diagram.expand``.
DIRECTIONS = ("down", "up", "both")


# =============================================================================
# Schema description
# =============================================================================


@dataclass(frozen=True)
class TableInfo:
    """
    One table of a schema description.

    Parameters
    ----------
    name : str
        Table name: the full table name when built from a connection.
    primary_key : tuple[str, ...]
        Primary-key attributes, in order.
    attributes : tuple[str, ...], optional
        All attributes in heading order, the primary key included. Defaults to the
        primary key.
    nullable : frozenset[str], optional
        Attributes that can hold NULL.
    master : str, optional
        The master's name if the table is a part table (by name, as
        ``extract_master`` reads it). Whether the master owns it is decided by
        :class:`TraversalSchema`.
    """

    name: str
    primary_key: tuple[str, ...]
    attributes: tuple[str, ...] = ()
    nullable: frozenset = frozenset()
    master: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "primary_key", tuple(self.primary_key))
        object.__setattr__(self, "attributes", tuple(self.attributes) or self.primary_key)
        object.__setattr__(self, "nullable", frozenset(self.nullable))


@dataclass(frozen=True)
class ForeignKey:
    """
    One foreign key: the child references the parent's primary key.

    Parameters
    ----------
    child : str
        The referencing table.
    parent : str
        The referenced table.
    attr_map : mapping or tuple of pairs
        Each child attribute of the foreign key and the parent primary-key
        attribute it references. Stored as a tuple of ``(child, parent)`` pairs.
    """

    child: str
    parent: str
    attr_map: tuple = ()

    def __post_init__(self) -> None:
        pairs = self.attr_map.items() if isinstance(self.attr_map, Mapping) else self.attr_map
        object.__setattr__(self, "attr_map", tuple((c, p) for c, p in pairs))

    @property
    def mapping(self) -> dict[str, str]:
        """The attribute map as a dict, child attribute to parent attribute."""
        return dict(self.attr_map)

    @property
    def attributes(self) -> tuple[str, ...]:
        """The child's attributes in this foreign key."""
        return tuple(c for c, _ in self.attr_map)

    @property
    def renamed(self) -> bool:
        """True if any attribute has a different name in the child than in the parent."""
        return any(c != p for c, p in self.attr_map)


class TraversalSchema:
    """
    The tables and foreign keys a traversal needs, with the owned parts (A1).

    Parameters
    ----------
    tables : iterable of TableInfo
        The tables. Their order breaks ties wherever the topological order leaves
        a choice, so that plans are reproducible.
    foreign_keys : iterable of ForeignKey
        The foreign keys between the tables. Each child's foreign keys are listed
        in the order of its definition (see Notes).

    Attributes
    ----------
    groups : dict[str, tuple[str, ...]]
        For each master that owns at least one part, its owned parts, parents
        before children.
    group_of : dict[str, str]
        The master of every table in a group, the master itself included.
    owning : dict[str, dict[str, str]]
        For each table in a group, the attribute of that table that holds each
        attribute of the master's primary key: its owning attributes. The master
        maps its key onto itself.

    Notes
    -----
    A part is owned by its master if a foreign key in the part's primary key (all
    its attributes are primary-key attributes of the part) references the master,
    or an owned part of the same master, renamed or not. Its owning attributes
    are the attributes that chain maps onto the master's key. If a part has
    several owning references, an unrenamed reference to the master wins, and
    otherwise the first one in the part's definition (diagram.md, A1).

    The same rule decides ownership in ``datajoint.dependencies.owned_parts``
    (#1620), which finds the owned parts in the dependency graph. This class
    also needs the owning attributes and that tie-break, so it computes both from
    the foreign keys directly.
    """

    def __init__(self, tables: Iterable[TableInfo], foreign_keys: Iterable[ForeignKey]) -> None:
        self.tables: dict[str, TableInfo] = {}
        for table in tables:
            if table.name in self.tables:
                raise DataJointError(f"Table {table.name} is described twice.")
            self.tables[table.name] = table
        self.foreign_keys = tuple(foreign_keys)
        self._position = {name: i for i, name in enumerate(self.tables)}
        self._fks_of = defaultdict(list)  # child -> its foreign keys, in definition order
        self._fks_to = defaultdict(list)  # parent -> the foreign keys that reference it
        for fk in self.foreign_keys:
            if fk.child not in self.tables or fk.parent not in self.tables:
                raise DataJointError(f"Foreign key {fk.child} -> {fk.parent} names a table that is not described.")
            self._fks_of[fk.child].append(fk)
            self._fks_to[fk.parent].append(fk)
        self.groups: dict[str, tuple[str, ...]] = {}
        self.group_of: dict[str, str] = {}
        self.owning: dict[str, dict[str, str]] = {}
        self._find_owned_parts()

    def __repr__(self) -> str:
        lines = ["TraversalSchema("]
        for t in self.tables.values():
            extra = f", nullable={sorted(t.nullable)}" if t.nullable else ""
            extra += f", master={t.master!r}" if t.master else ""
            lines.append(f"  TableInfo({t.name!r}, {t.primary_key!r}, {t.attributes!r}{extra}),")
        for fk in self.foreign_keys:
            lines.append(f"  ForeignKey({fk.child!r}, {fk.parent!r}, {fk.mapping!r}),")
        return "\n".join(lines) + "\n)"

    # --- construction ---------------------------------------------------------

    @classmethod
    def from_dependencies(cls, dependencies: nx.MultiDiGraph, names: Iterable[str], heading: Callable) -> TraversalSchema:
        """
        Describe the tables ``names`` of a dependency graph.

        Parameters
        ----------
        dependencies : networkx.MultiDiGraph
            ``connection.dependencies``: one edge per foreign key, parent to
            child, with its ``attr_map``.
        names : iterable of str
            Full names of the tables to describe. Foreign keys between them are
            included; foreign keys to other tables are left out.
        heading : callable
            Returns the ``Heading`` of a table, given its full name. Attribute
            order and nullability come from it.

        Returns
        -------
        TraversalSchema

        Notes
        -----
        The database does not record the order in which a definition lists its
        foreign keys. Each foreign key adds the attributes the table does not
        have yet, in definition order, so the foreign keys are ordered here by the
        heading positions of their attributes. This order is used only to break
        the tie between two renamed owning references of one part.
        """
        names = sorted(set(names))
        tables = []
        position = {}
        for name in names:
            h = heading(name)
            attributes = tuple(h.names)
            position[name] = {a: i for i, a in enumerate(attributes)}
            master = extract_master(name)
            tables.append(
                TableInfo(
                    name,
                    tuple(h.primary_key),
                    attributes,
                    frozenset(a for a in attributes if h.attributes[a].nullable),
                    master if master in names else None,
                )
            )
        selected = set(names)
        fks = [
            ForeignKey(child, parent, props["attr_map"])
            for parent, child, props in dependencies.edges(data=True)
            if parent in selected and child in selected
        ]
        fks.sort(key=lambda fk: (fk.child, sorted(position[fk.child].get(a, len(position[fk.child])) for a in fk.attributes)))
        return cls(tables, fks)

    def _find_owned_parts(self) -> None:
        # Parts are taken parents first, so that a part's references to other
        # parts are decided before the part itself.
        parts_of = defaultdict(list)
        for t in self.tables.values():
            if t.master is not None and t.master in self.tables:
                parts_of[t.master].append(t.name)
        for master, parts in parts_of.items():
            key = self.tables[master].primary_key
            owning = {master: {a: a for a in key}}
            graph = nx.DiGraph()
            graph.add_nodes_from(parts)
            graph.add_edges_from((fk.parent, fk.child) for p in parts for fk in self._fks_of[p] if fk.parent in parts)
            for part in nx.lexicographical_topological_sort(graph, key=self._position.get):
                chains = [fk for fk in self._fks_of[part] if fk.parent in owning and self.is_primary(fk)]
                if not chains:
                    continue
                unrenamed = [fk for fk in chains if fk.parent == master and not fk.renamed]
                fk = (unrenamed or chains)[0]
                by_parent = {p: c for c, p in fk.attr_map}
                owning[part] = {a: by_parent[owning[fk.parent][a]] for a in key}
            owned = tuple(p for p in nx.lexicographical_topological_sort(graph, key=self._position.get) if p in owning)
            if owned:
                self.groups[master] = owned
                self.owning.update(owning)
                self.group_of.update({name: master for name in owning})

    # --- facts about tables and foreign keys -----------------------------------

    def fks_of(self, child: str) -> list[ForeignKey]:
        """The foreign keys of ``child``, in definition order."""
        return list(self._fks_of[child])

    def fks_to(self, parent: str) -> list[ForeignKey]:
        """The foreign keys that reference ``parent``."""
        return list(self._fks_to[parent])

    def is_primary(self, fk: ForeignKey) -> bool:
        """True if all the foreign key's attributes are in the child's primary key."""
        return set(fk.attributes) <= set(self.tables[fk.child].primary_key)

    def owning_attributes(self, table: str) -> tuple[str, ...] | None:
        """The table's owning attributes, in the order of the master's key; None outside a group."""
        master = self.group_of.get(table)
        if master is None:
            return None
        return tuple(self.owning[table][a] for a in self.tables[master].primary_key)

    def intra_group_fks(self, master: str) -> list[ForeignKey]:
        """The foreign keys between two different tables of the group of ``master``."""
        return [
            fk
            for fk in self.foreign_keys
            if fk.child != fk.parent and self.group_of.get(fk.child) == master and self.group_of.get(fk.parent) == master
        ]

    def preserves_instance(self, fk: ForeignKey) -> bool:
        """
        True if a foreign key inside a group maps the referencing table's owning
        attributes onto the referenced table's owning attributes.

        Such a foreign key stays inside one master-part instance. One that does
        not links different instances, and its group iterates (diagram.md,
        "Groups that iterate").
        """
        master = self.group_of[fk.child]
        child, parent, mapping = self.owning[fk.child], self.owning[fk.parent], fk.mapping
        return all(mapping.get(child[a]) == parent[a] for a in self.tables[master].primary_key)

    def links(self, master: str) -> tuple[ForeignKey, ...]:
        """The foreign keys inside the group of ``master`` that link different instances."""
        return tuple(fk for fk in self.intra_group_fks(master) if not self.preserves_instance(fk))

    def copy_allowed(self, fk: ForeignKey, attributes: frozenset | None) -> bool:
        """
        R1's rule for copying a parent's condition to the child unchanged.

        The attributes the condition names are known and are among the foreign
        key's attributes, the foreign key is not renamed, and none of its columns
        is nullable.
        """
        return (
            attributes is not None
            and attributes <= set(fk.attributes)
            and not fk.renamed
            and not set(fk.attributes) & self.tables[fk.child].nullable
        )

    def relabel_down_allowed(self, fk: ForeignKey, keys: Iterable[Mapping]) -> bool:
        """
        The Seeds rule for relabeling a parent's literal keys for the child.

        Every key names, among the parent's attributes, only attributes the
        foreign key carries; and if the foreign key has a nullable column, every
        key names all of the foreign key's attributes, none of them NULL.
        """
        keys = list(keys)
        parent = set(self.tables[fk.parent].attributes)
        carried = {p for _, p in fk.attr_map}
        if any((set(key) & parent) - carried for key in keys):
            return False
        if not set(fk.attributes) & self.tables[fk.child].nullable:
            return True
        return all(carried <= set(key) and all(key[a] is not None for a in carried) for key in keys)

    # --- reach and order --------------------------------------------------------

    def reach(self, tables: Iterable[str], direction: str) -> frozenset:
        """
        The table-level reach of ``tables``: the node set of an expanded diagram.

        The tables reached by following foreign keys in the direction of travel,
        together with the master and all owned parts of every group that has a
        reached member. For ``"both"``, the upward reach of the downward reach.
        """
        if direction == "both":
            return self.reach(self.reach(tables, "down"), "up")
        _check_direction(direction)
        seen = set(tables)
        stack = list(seen)
        while stack:
            t = stack.pop()
            nxt = {fk.child for fk in self._fks_to[t]} if direction == "down" else {fk.parent for fk in self._fks_of[t]}
            master = self.group_of.get(t)
            if master is not None:
                nxt |= {master, *self.groups[master]}
            for n in nxt - seen:
                seen.add(n)
                stack.append(n)
        return frozenset(seen)

    def quotient_order(self, names: Iterable[str]) -> list[str]:
        """
        The nodes of the group quotient Q among ``names``, in topological order.

        A group is represented by its master. Raises ``DataJointError`` if Q has a
        cycle, which the declaration check refuses (#1619).
        """
        names = sorted(set(names), key=lambda n: self._position[n])
        node = {t: self.group_of.get(t, t) for t in names}
        q = nx.DiGraph()
        q.add_nodes_from(node[t] for t in names)
        for fk in self.foreign_keys:
            if fk.parent in node and fk.child in node:
                u, v = node[fk.parent], node[fk.child]
                if u != v or fk.parent == fk.child:
                    q.add_edge(u, v)
        try:
            return list(nx.lexicographical_topological_sort(q, key=self._position.get))
        except nx.NetworkXUnfeasible:
            cycle = [u for u, _ in nx.find_cycle(q)]
            first = cycle.index(min(cycle, key=lambda n: self._position[n]))
            cycle = cycle[first:] + cycle[:first]
            raise DataJointError(
                "Cannot traverse tables whose master-part groups form a cycle: "
                f"{' -> '.join(cycle + cycle[:1])}. Each group is named by its master."
            ) from None

    def order(self, names: Iterable[str]) -> list[str]:
        """``names`` in topological order, with each master's owned parts right after it."""
        names = set(names)
        result = []
        for q in self.quotient_order(names):
            result.append(q)
            result.extend(p for p in self.groups.get(q, ()) if p in names)
        return result


def _check_direction(direction: str) -> None:
    if direction not in ("down", "up"):
        raise ValueError(f"direction must be 'down', 'up' or 'both', not {direction!r}")


# =============================================================================
# The plan
# =============================================================================
# Every node selects rows of one table, ``node.table``, by a condition. A node
# that uses another node's rows names it by its handle, its index in
# ``Plan.nodes``. The attribute maps of Restrict, Lift and Reexpand pair each
# attribute of the node's table with the attribute of the source's table whose
# values it must match.


@dataclass(frozen=True)
class Seed:
    """
    The seed: one table restricted by a condition.

    Parameters
    ----------
    table : str
        The seed table.
    condition : object
        The restriction, opaque to the planner. ``seed_from_expression`` stores
        a query expression's compiled conditions; tests store model conditions.
    attributes : frozenset[str] or None
        The attributes the condition names, which decide whether R1 may copy it.
        None if they cannot be read from the condition, which is then never copied.
    """

    table: str
    condition: Any = field(compare=False)
    attributes: frozenset | None = None


@dataclass(frozen=True)
class Literal:
    """
    Literal keys of a table.

    Parameters
    ----------
    table : str
        The table.
    keys : tuple[dict, ...] or None
        Key dicts, combined with OR; an empty tuple selects no rows. None marks
        the master keys of a group that iterates, which the evaluator finds by
        repeating the group step (see ``GroupStep``).
    read : bool
        True if the keys were read from the table's own rows, as ``dj.Top`` keys
        are. Only such keys cross a foreign key upward by relabeling.
    """

    table: str
    keys: tuple | None
    read: bool = False


@dataclass(frozen=True)
class Restrict:
    """
    R1 across one foreign key: the rows of ``table`` that match rows of ``src``.

    Downward, ``table`` is the child and ``attr_map`` is the foreign key's map;
    upward, ``table`` is the parent and the map is reversed. Compiled, it is
    ``table & src_rows.proj(<attributes renamed to their names in table>)``.

    With ``copy=True``, the child is restricted by ``src``'s own condition,
    unchanged, as R1 allows when the condition names only attributes of an
    unrenamed foreign key with no nullable column.
    """

    table: str
    src: int
    attr_map: tuple
    copy: bool = False


@dataclass(frozen=True)
class Lift:
    """
    R2's lift: the master rows of the part rows that ``src`` selects.

    ``table`` is the master and ``attr_map`` pairs each attribute of the master's
    key with the part's owning attribute that holds it. Compiled, it is
    ``master & part_rows.proj(m1="o1", ...)``.
    """

    table: str
    src: int
    attr_map: tuple


@dataclass(frozen=True)
class Reexpand:
    """
    R2's re-expansion: every row of the part ``table`` whose master ``src`` selects.

    ``attr_map`` pairs each owning attribute of the part with the attribute of
    the master's key it holds. Compiled, it is ``part & master_rows.proj(o1="m1", ...)``.
    """

    table: str
    src: int
    attr_map: tuple


@dataclass(frozen=True)
class Or:
    """The union of the rows of several nodes of one table."""

    table: str
    terms: tuple


#: The node types of a plan.
NODE_TYPES = (Seed, Literal, Restrict, Lift, Reexpand, Or)
_MATCHING = (Restrict, Lift, Reexpand)


@dataclass(frozen=True)
class GroupStep:
    """
    The visit of one group in one pass of a plan.

    Parameters
    ----------
    master : str
        The group's master.
    parts : tuple[str, ...]
        Its owned parts.
    direction : str
        The pass: ``"down"`` or ``"up"``.
    entry : int
        The master's rows after one step: the seed on the master, R1 into the
        master, and the lift of R1 into each part and of the seed on each part.
    links : tuple[ForeignKey, ...]
        The foreign keys inside the group that link different instances. The
        group iterates if there is one.
    keys : int, optional
        For a group that iterates: the ``Literal`` with ``keys=None`` that stands
        for its master keys. The master's restriction is this node.
    step : int, optional
        For a group that iterates: the master rows reached in one more group step
        from the instances of ``keys``, across ``links``. The evaluator binds
        ``keys`` to the keys of ``entry`` and adds the keys of ``step`` until no
        new key appears.
    """

    master: str
    parts: tuple
    direction: str
    entry: int
    links: tuple = ()
    keys: int | None = None
    step: int | None = None

    @property
    def iterate(self) -> bool:
        """Whether the group needs iterative evaluation."""
        return bool(self.links)


class Plan:
    """
    The plan of one expansion.

    Attributes
    ----------
    schema : TraversalSchema
        The schema the plan was built for.
    direction : str
        ``"down"``, ``"up"`` or ``"both"``.
    seed : int
        Handle of the seed node.
    nodes : list
        The nodes; a handle is an index into this list.
    restriction : dict[str, int]
        For each table of the expanded diagram, the handle of its rows.
    order : tuple[str, ...]
        The tables of the expanded diagram in topological order, owned parts
        right after their master.
    groups : list[GroupStep]
        The groups visited, in the order of the passes.
    """

    def __init__(self, schema: TraversalSchema, direction: str) -> None:
        self.schema = schema
        self.direction = direction
        self.seed: int = -1  # set by plan_expand
        self.nodes: list = []
        self.restriction: dict[str, int] = {}
        self.order: tuple = ()
        self.groups: list[GroupStep] = []
        self._index: dict = {}

    def add(self, node) -> int:
        """Add a node and return its handle; an identical node already present is reused."""
        key = _node_key(node)
        if key is not None and key in self._index:
            return self._index[key]
        self.nodes.append(node)
        handle = len(self.nodes) - 1
        if key is not None:
            self._index[key] = handle
        return handle

    def union(self, table: str, terms: Iterable[int]) -> int:
        """The handle of the union of ``terms``: the term itself if there is one, no rows if none."""
        flat = []
        for t in terms:
            node = self.nodes[t]
            for term in node.terms if isinstance(node, Or) else (t,):
                if term not in flat:
                    flat.append(term)
        if not flat:
            return self.add(Literal(table, ()))
        if len(flat) == 1:
            return flat[0]
        return self.add(Or(table, tuple(flat)))

    def attributes(self, handle: int) -> frozenset | None:
        """
        The attributes the condition of a node names, for R1's copy rule.

        A Seed's are given with it, and a Literal's are the names in its keys. A
        Restrict, Lift or Reexpand names the attributes it matches; a copy names
        those of the condition it copies; an Or names those of all its terms.
        None means unknown.
        """
        node = self.nodes[handle]
        if isinstance(node, Seed):
            return node.attributes
        if isinstance(node, Literal):
            if node.keys is None:
                return frozenset(self.schema.tables[node.table].primary_key)
            return frozenset(a for key in node.keys for a in key)
        if isinstance(node, Restrict) and node.copy:
            return self.attributes(node.src)
        if isinstance(node, _MATCHING):
            return frozenset(a for a, _ in node.attr_map)
        names = [self.attributes(t) for t in node.terms]
        known = [n for n in names if n is not None]
        return frozenset().union(*known) if len(known) == len(names) else None

    def iterating_groups(self) -> list[GroupStep]:
        """The groups that need iterative evaluation."""
        return [g for g in self.groups if g.iterate]

    def describe(self) -> str:
        """The plan, one node per line, then each table's restriction."""
        lines = []
        for h, node in enumerate(self.nodes):
            if isinstance(node, Seed):
                text = f"Seed({node.table}; names {_names(node.attributes)})"
            elif isinstance(node, Literal):
                keys = "the keys of an iterating group" if node.keys is None else f"{len(node.keys)} keys"
                text = f"Literal({node.table}; {keys}{', read' if node.read else ''})"
            elif isinstance(node, Or):
                text = f"Or({node.table}; {', '.join(f'#{t}' for t in node.terms)})"
            else:
                kind = "copy of" if getattr(node, "copy", False) else "by"
                pairs = ", ".join(a if a == b else f"{a}={b}" for a, b in node.attr_map)
                text = f"{type(node).__name__}({node.table} {kind} #{node.src}; {pairs})"
            lines.append(f"#{h} {text}")
        for g in self.groups:
            if g.iterate:
                links = "; ".join(f"{fk.child} -> {fk.parent}" for fk in g.links)
                lines.append(f"{g.direction}: {g.master} iterates over {links}: #{g.keys} from #{g.entry}, step #{g.step}")
        lines.extend(f"{t} = #{self.restriction[t]}" for t in self.order)
        return "\n".join(lines)

    def __repr__(self) -> str:
        return f"Plan({self.direction})\n{self.describe()}"


def _names(attributes) -> str:
    return "unknown attributes" if attributes is None else "{" + ", ".join(sorted(attributes)) + "}"


def _node_key(node):
    """A hashable key for reusing identical nodes; None for nodes that are never shared."""
    if isinstance(node, Seed):
        return None
    if isinstance(node, Literal):
        if node.keys is None:
            return None  # each iterating group has its own keys
        return (Literal, node.table, tuple(tuple(sorted(k.items(), key=lambda kv: kv[0])) for k in node.keys), node.read)
    return node


# =============================================================================
# The planner
# =============================================================================


def plan_expand(schema: TraversalSchema, seed, direction: str = "down") -> Plan:
    """
    Plan ``expand(seed, direction)``.

    Parameters
    ----------
    schema : TraversalSchema
        The schema; its group quotient must be acyclic.
    seed : Seed or Literal
        The seed node.
    direction : {"down", "up", "both"}
        ``"down"`` plans ``D(S)``, ``"up"`` plans ``U(S)``, and ``"both"`` plans
        ``U(D(S))``: the downward pass, then an upward pass seeded with every
        table's rows from it.

    Returns
    -------
    Plan

    Notes
    -----
    diagram.md, "One pass over the group quotient": each node of Q is visited
    once, in topological order for ``down`` and in reverse order for ``up``. A
    plain table gets its seed and R1 from each restricted neighbor on the
    incoming side. A group's master gets the seed on the master, R1 into the
    master, and the lift of R1 into each part and of the seed on each part; each
    part then gets the re-expansion of the master. A group with a foreign key
    that links instances is marked to iterate.
    """
    if direction not in DIRECTIONS:
        raise ValueError(f"direction must be 'down', 'up' or 'both', not {direction!r}")
    if seed.table not in schema.tables:
        raise DataJointError(f"The seed table {seed.table} is not in the schema description.")
    plan = Plan(schema, direction)
    plan.seed = plan.add(seed)
    seeds = {seed.table: [plan.seed]}
    if direction == "both":
        down = _plan_pass(plan, seeds, "down")
        plan.restriction = _plan_pass(plan, {t: [h] for t, h in down.items()}, "up")
    else:
        plan.restriction = _plan_pass(plan, seeds, direction)
    plan.order = tuple(schema.order(plan.restriction))
    return plan


def _plan_pass(plan: Plan, seeds: dict, direction: str) -> dict:
    """One pass over Q; returns the handle of each reached table's rows."""
    schema = plan.schema
    order = schema.quotient_order(schema.reach(seeds, direction))
    if direction == "up":
        order.reverse()
    restriction: dict[str, int] = {}

    def incoming(table):
        # The seed on the table and R1 from each restricted neighbor outside its group.
        terms = list(seeds.get(table, ()))
        group = schema.group_of.get(table)
        for fk in schema.fks_of(table) if direction == "down" else schema.fks_to(table):
            other = fk.parent if direction == "down" else fk.child
            if other in restriction and (group is None or schema.group_of.get(other) != group):
                terms.append(_cross(plan, fk, restriction[other], direction))
        return terms

    for q in order:
        parts = schema.groups.get(q)
        if parts is None:
            restriction[q] = plan.union(q, incoming(q))
            continue
        # R2: the master's rows are its own incoming rows and the lift of each part's.
        terms = incoming(q)
        for part in parts:
            part_terms = incoming(part)
            if part_terms:
                terms.append(plan.add(Lift(q, plan.union(part, part_terms), _lift_map(schema, part))))
        entry = plan.union(q, terms)
        links = schema.links(q)
        keys = step = None
        if links:
            # A foreign key that links instances: one more group step from the
            # instances of the current master keys, across those foreign keys.
            keys = plan.add(Literal(q, None, read=True))
            instance = {q: keys}
            instance.update({p: plan.add(Reexpand(p, keys, _reexpand_map(schema, p))) for p in parts})
            reached = []
            for fk in links:
                source, target = (fk.parent, fk.child) if direction == "down" else (fk.child, fk.parent)
                term = _cross(plan, fk, instance[source], direction)
                reached.append(term if target == q else plan.add(Lift(q, term, _lift_map(schema, target))))
            step = plan.union(q, reached)
        restriction[q] = entry if keys is None else keys
        for part in parts:
            restriction[part] = plan.add(Reexpand(part, restriction[q], _reexpand_map(schema, part)))
        plan.groups.append(GroupStep(q, parts, direction, entry, links, keys, step))
    return restriction


def _cross(plan: Plan, fk: ForeignKey, src: int, direction: str) -> int:
    """R1 across ``fk`` from the restricted table ``src``, in the direction of travel."""
    schema = plan.schema
    node = plan.nodes[src]
    literal = isinstance(node, Literal) and node.keys is not None
    if direction == "down":
        if literal:
            if schema.relabel_down_allowed(fk, node.keys):
                return plan.add(Literal(fk.child, tuple(_relabel_down(fk, node.keys))))
        elif schema.copy_allowed(fk, plan.attributes(src)):
            return plan.add(Restrict(fk.child, src, tuple((c, c) for c in fk.attributes), copy=True))
        return plan.add(Restrict(fk.child, src, fk.attr_map))
    # Upward, R1 is always a restriction: a condition on the child describes child
    # rows. Only keys read from the child itself may be relabeled.
    if literal and node.read and all(set(fk.attributes) <= set(key) for key in node.keys):
        return plan.add(Literal(fk.parent, tuple(_relabel_up(fk, node.keys))))
    return plan.add(Restrict(fk.parent, src, tuple((p, c) for c, p in fk.attr_map)))


def _lift_map(schema: TraversalSchema, part: str) -> tuple:
    owning = schema.owning[part]
    return tuple((a, owning[a]) for a in schema.tables[schema.group_of[part]].primary_key)


def _reexpand_map(schema: TraversalSchema, part: str) -> tuple:
    return tuple((o, m) for m, o in _lift_map(schema, part))


def _relabel_down(fk: ForeignKey, keys) -> list[dict]:
    by_parent = {p: c for c, p in fk.attr_map}
    return [{by_parent[a]: v for a, v in key.items() if a in by_parent} for key in keys]


def _relabel_up(fk: ForeignKey, keys) -> list[dict]:
    mapping = fk.mapping
    return [{mapping[c]: v for c, v in key.items() if c in mapping} for key in keys]


# =============================================================================
# Evaluation
# =============================================================================


class _Evaluator:
    """
    Shared evaluation logic: memoized node values and the groups that iterate.

    A value that depends on the keys of a group that iterates is computed once
    those keys are known. The keys are found by repeating the group step: they
    start as the keys of the group's ``entry``, and the keys of its ``step``,
    evaluated from the current keys, are added until no new key appears.
    """

    def __init__(self, plan: Plan) -> None:
        self.plan = plan
        self.schema = plan.schema
        self._memo: dict[int, Any] = {}
        self._depends: dict[int, frozenset] = {}
        self._keys: dict[int, set] = {}  # handle of a group's Literal(keys=None) -> final set of key tuples
        self._trial: dict[int, set] = {}  # the same, while the group step repeats
        self._groups = {g.keys: g for g in plan.groups if g.keys is not None}
        #: For each group that iterates, ``(direction, master)``: the master keys after each round.
        self.rounds: dict[tuple, list] = {}

    def value(self, handle: int):
        """The value of a node: rows in memory, or a query expression."""
        if handle in self._memo:
            return self._memo[handle]
        depends = self._placeholders(handle)
        for p in depends:
            if p not in self._keys and p not in self._trial:
                self._iterate(self._groups[p])
        result = self._compute(handle)
        if all(p in self._keys for p in depends):
            self._memo[handle] = result
        return result

    def table(self, name: str):
        """The value of a table of the expanded diagram."""
        return self.value(self.plan.restriction[name])

    def result(self) -> dict:
        """The value of every table of the expanded diagram, in topological order."""
        return {name: self.table(name) for name in self.plan.order}

    def group_keys(self, handle: int) -> list[dict]:
        """The current master keys of a group that iterates, as key dicts."""
        keys = self._trial[handle] if handle in self._trial else self._keys[handle]
        pk = self.schema.tables[self.plan.nodes[handle].table].primary_key
        return [dict(zip(pk, k)) for k in sorted(keys, key=repr)]

    def _placeholders(self, handle: int) -> frozenset:
        """The keys of the groups that iterate that a node depends on."""
        if handle not in self._depends:
            node = self.plan.nodes[handle]
            if isinstance(node, Literal) and node.keys is None:
                found = frozenset({handle})
            elif isinstance(node, _MATCHING):
                found = self._placeholders(node.src)
            elif isinstance(node, Or):
                found = frozenset().union(*(self._placeholders(t) for t in node.terms))
            else:
                found = frozenset()
            self._depends[handle] = found
        return self._depends[handle]

    def _iterate(self, group: GroupStep) -> None:
        handle, step = group.keys, group.step
        assert handle is not None and step is not None, "only a group that iterates is iterated"
        keys = set(self._key_tuples(group.entry))
        history = [set(keys)]
        while True:
            self._trial[handle] = keys
            new = set(self._key_tuples(step)) - keys
            if not new:
                break
            keys = keys | new
            history.append(set(keys))
        del self._trial[handle]
        self._keys[handle] = keys
        self.rounds[(group.direction, group.master)] = history

    def _compute(self, handle: int):  # pragma: no cover - implemented by subclasses
        raise NotImplementedError

    def _key_tuples(self, handle: int):  # pragma: no cover - implemented by subclasses
        raise NotImplementedError


def _dict_matches(key: Mapping, row: Mapping, heading) -> bool:
    """A key dict as a restriction: keys that are not attributes are ignored, and None matches NULL."""
    return all(row[k] == v for k, v in key.items() if k in heading)


class RowEvaluator(_Evaluator):
    """
    Evaluate a plan on rows held in memory.

    Parameters
    ----------
    plan : Plan
        The plan.
    db : dict
        ``{table: {pk_tuple: row_dict}}``, where ``pk_tuple`` lists the
        primary-key values in key order; the format of the reference model in the
        test suite.
    seed_match : callable, optional
        ``seed_match(condition, row, table_info) -> bool`` decides whether a row
        passes the seed's condition. By default the condition is a key dict or a
        list of key dicts.
    fixed : dict[int, set], optional
        Nodes whose rows are given, as sets of primary-key tuples: the delete
        simulation passes the nodes it materialized.
    keys : dict[int, set], optional
        The master keys of groups that iterate, found already, as returned by
        ``group_key_sets``.

    Notes
    -----
    A value is a frozenset of primary-key tuples. A restriction by a projection
    matches the attributes it pairs, and a row with a NULL among them matches
    nothing (A2). A copy evaluates the copied condition on the child's own rows,
    so that the copy rule is tested rather than assumed.
    """

    def __init__(self, plan: Plan, db: Mapping, seed_match: Callable | None = None, fixed=None, keys=None) -> None:
        super().__init__(plan)
        self.db = db
        self.seed_match = seed_match or (lambda cond, row, table: _literal_matches(cond, row, table.attributes))
        self.fixed = dict(fixed or {})
        self._keys.update({h: set(k) for h, k in (keys or {}).items()})
        self._projected: dict[int, frozenset] = {}

    def group_key_sets(self) -> dict:
        """The master keys found for each group that iterates, to reuse on another database state."""
        for g in self.plan.iterating_groups():
            if g.keys not in self._keys:
                self._iterate(g)
        return {h: set(k) for h, k in self._keys.items()}

    def _key_tuples(self, handle: int):
        return self.value(handle)

    def _compute(self, handle: int) -> frozenset:
        if handle in self.fixed:
            return frozenset(self.fixed[handle])
        node = self.plan.nodes[handle]
        rows = self.db[node.table]
        if isinstance(node, Or):
            return frozenset().union(*(self.value(t) for t in node.terms))
        if isinstance(node, _MATCHING) and not getattr(node, "copy", False):
            projected = self._projection(handle)
            names = [a for a, _ in node.attr_map]
            return frozenset(k for k, row in rows.items() if _value_in(tuple(row[a] for a in names), projected))
        return frozenset(k for k, row in rows.items() if self.holds(handle, node.table, row))

    def _projection(self, handle: int) -> frozenset:
        """The source rows of a Restrict, Lift or Reexpand, projected onto the attributes it matches."""
        if handle in self._projected:
            return self._projected[handle]
        node = self.plan.nodes[handle]
        table = self.plan.nodes[node.src].table
        rows, pk = self.db[table], self.schema.tables[table].primary_key
        values = set()
        for k in self.value(node.src):
            # A materialized source can name rows deleted since; their keys still hold the key attributes.
            row = rows.get(k) or dict(zip(pk, k))
            if all(s in row for _, s in node.attr_map):
                values.add(tuple(row[s] for _, s in node.attr_map))
        projected = frozenset(values)
        if all(p in self._keys for p in self._placeholders(node.src)):
            self._projected[handle] = projected
        return projected

    def holds(self, handle: int, table: str, row: Mapping) -> bool:
        """Whether ``row`` of ``table`` passes the condition of node ``handle``."""
        info = self.schema.tables[table]
        node = self.plan.nodes[handle]
        if handle in self.fixed:
            # A materialized node: its keys, as a restriction of this table.
            pk = self.schema.tables[node.table].primary_key
            return any(_dict_matches(dict(zip(pk, k)), row, info.attributes) for k in self.fixed[handle])
        if isinstance(node, Seed):
            return self.seed_match(node.condition, row, info)
        if isinstance(node, Literal):
            keys = node.keys if node.keys is not None else self.group_keys(handle)
            return any(_dict_matches(key, row, info.attributes) for key in keys)
        if isinstance(node, Restrict) and node.copy:
            return self.holds(node.src, table, row)
        if isinstance(node, _MATCHING):
            # A restriction by a query expression matches on the shared attributes.
            shared = [i for i, (a, _) in enumerate(node.attr_map) if a in info.attributes]
            projected = self._projection(handle)
            if not shared:
                return bool(projected)
            value = tuple(row[node.attr_map[i][0]] for i in shared)
            return _value_in(value, frozenset(tuple(p[i] for i in shared) for p in projected))
        return any(self.holds(t, table, row) for t in node.terms)


def _value_in(value: tuple, values: frozenset) -> bool:
    """SQL's ``(a, b) IN (...)``: a NULL on either side matches nothing."""
    return None not in value and value in values


def _literal_matches(cond, row: Mapping, heading) -> bool:
    if isinstance(cond, Mapping):
        return _dict_matches(cond, row, heading)
    return any(_literal_matches(c, row, heading) for c in cond)


def evaluate_rows(plan: Plan, db: Mapping, seed_match: Callable | None = None) -> dict:
    """
    Evaluate a plan on rows held in memory.

    Returns
    -------
    dict[str, frozenset]
        For each table of the expanded diagram, the primary-key tuples of its rows.
    """
    return RowEvaluator(plan, db, seed_match).result()


class QueryCompiler(_Evaluator):
    """
    Compile a plan to query expressions.

    Parameters
    ----------
    plan : Plan
        A plan whose table names are full table names.
    connection : Connection
        The database connection.
    tables : dict, optional
        ``FreeTable`` objects to reuse, by full table name, so that headings are
        loaded once.

    Notes
    -----
    Each node is compiled to a ``FreeTable`` of its table with one condition
    appended. A Restrict, Lift or Reexpand compiles to
    ``(a1, a2) IN (SELECT s1, s2 FROM <src>)``, which is how
    ``table & src.proj(a1="s1", a2="s2")`` compiles; matching by position keeps
    other attributes of the source out of the match, also when a rename reuses a
    name the source holds (parallel foreign keys). A copy appends the copied
    condition, compiled for the child. A seed's conditions are already compiled
    and are appended as they are: compiling them again would escape ``%`` twice
    and reject the column names of a subquery (#1610). A Literal is a list of
    key dicts, and an Or joins its terms' conditions with OR.

    A group that iterates is the only place where compilation reads rows: the
    master keys are fetched after each group step.
    """

    def __init__(self, plan: Plan, connection, tables: dict | None = None) -> None:
        super().__init__(plan)
        self.connection = connection
        self._tables = dict(tables or {})

    def free_table(self, name: str):
        """The unrestricted table ``name``."""
        if name not in self._tables:
            from .table import FreeTable

            self._tables[name] = FreeTable(self.connection, name)
        return self._tables[name]

    def _key_tuples(self, handle: int):
        pk = self.schema.tables[self.plan.nodes[handle].table].primary_key
        return {tuple(row[a] for a in pk) for row in self.value(handle).keys()}

    def _compute(self, handle: int):
        node = self.plan.nodes[handle]
        table = self.free_table(node.table)
        return _append_condition(table, self.condition(handle, table), self.plan.attributes(handle))

    def condition(self, handle: int, target) -> str | bool:
        """The SQL condition that restricts ``target`` by the condition of node ``handle``."""
        node = self.plan.nodes[handle]
        if isinstance(node, Seed):
            items = list(node.condition)
            if not items:
                return True
            if any(item is False for item in items):
                return False
            return "(" + ")AND(".join(str(item) for item in items) + ")"
        if isinstance(node, Literal):
            keys = node.keys if node.keys is not None else self.group_keys(handle)
            return make_condition(target, [dict(k) for k in keys], set())
        if isinstance(node, Restrict) and node.copy:
            return self.condition(node.src, target)
        if isinstance(node, _MATCHING):
            quote = self.connection.adapter.quote_identifier
            source = self.value(node.src)
            fields = ", ".join(quote(a) for a, _ in node.attr_map)
            return f"({fields}) IN ({source.make_sql([s for _, s in node.attr_map])})"
        parts = [self.condition(t, target) for t in node.terms]
        if any(p is True for p in parts):
            return True
        parts = [p for p in parts if p is not False]
        if not parts:
            return False
        return "(" + " OR ".join(f"({p})" for p in parts) + ")"


def _append_condition(expr, condition, attributes):
    """
    Return ``expr`` restricted by an SQL condition that is already compiled.

    ``QueryExpression.restrict`` compiles its argument, so a compiled condition
    is appended here directly. ``expr`` is a base table, so no subquery is needed.
    """
    if condition is True:
        return expr
    result = copy.copy(expr)
    result._restriction = AndList(expr.restriction)
    result._restriction.append(condition)
    result._restriction_attributes = set(expr.restriction_attributes) | (set(attributes or ()) & set(expr.heading.names))
    return result


def compile_plan(plan: Plan, connection, tables: dict | None = None) -> dict:
    """
    Compile a plan to query expressions.

    Returns
    -------
    dict[str, QueryExpression]
        For each table of the expanded diagram, in topological order, the table
        restricted to its rows.
    """
    return QueryCompiler(plan, connection, tables).result()


# =============================================================================
# Seeds and the dependency graph
# =============================================================================

_SELECT = re.compile(r"\bselect\b", re.IGNORECASE)


def _has_subquery(condition) -> bool:
    """Whether a compiled condition contains a subquery (as in 2.3.5, #1610)."""
    if not isinstance(condition, str):
        return False
    text = re.sub(r"(\\\")|(\\\')", "", condition)  # escaped quotes
    text = re.sub(r"'[^']*'|\"[^\"]*\"", "", text)  # quoted text and identifiers
    return _SELECT.search(text) is not None


def seed_from_expression(expr) -> Seed | Literal:
    """
    The seed node for a query expression.

    A seed is one table with any restriction (diagram.md, "Seeds"). The
    restriction may use other queries, but a projection, a join, an aggregation
    or a union is refused as a seed. A ``dj.Top`` restriction is replaced by the
    primary keys it selects.

    Parameters
    ----------
    expr : QueryExpression or Table class
        The seed.

    Returns
    -------
    Seed or Literal
        A Literal for a ``dj.Top`` seed; otherwise a Seed holding the compiled
        conditions. Their attributes are ``restriction_attributes``, unless a
        condition contains a subquery: the attributes a subquery names cannot be
        told from those of the seed table in compiled text, so such a seed is
        never copied.

    Raises
    ------
    DataJointError
        If the expression is not one table with a restriction.
    """
    from .expression import Aggregation, QueryExpression, Union

    if inspect.isclass(expr) and issubclass(expr, QueryExpression):
        expr = expr()
    if not isinstance(expr, QueryExpression):
        raise DataJointError(f"The seed of Diagram.expand must be a table or a restricted table, not {type(expr).__name__}.")
    node, top = expr, False
    while True:
        if isinstance(node, Union):
            raise DataJointError("A union cannot seed Diagram.expand. Expand each operand and add the diagrams.")
        if isinstance(node, Aggregation):
            raise DataJointError("An aggregation cannot seed Diagram.expand. Restrict a table by it instead.")
        if len(node.support) != 1 or node._joins:
            raise DataJointError(
                "A join cannot seed Diagram.expand. Seed one table and restrict it by the other operand instead."
            )
        top = top or node._top is not None
        if isinstance(node.support[0], str):
            break
        node = node.support[0]  # a subquery made around a dj.Top
    name = node.support[0]
    from .table import FreeTable

    base = FreeTable(expr.connection, name).heading
    if expr.heading.new_attributes or expr.heading.names != base.names or expr.primary_key != base.primary_key:
        raise DataJointError(
            "A projection cannot seed Diagram.expand. Seed the table with the same restriction, without the projection."
        )
    if top:
        return Literal(name, tuple(expr.keys()), read=True)
    if node is not expr:  # pragma: no cover - only a dj.Top makes a subquery around a restriction
        raise DataJointError("The seed of Diagram.expand must be one table with a restriction.")
    items = tuple(expr.restriction)
    attributes = None if any(_has_subquery(c) for c in items) else frozenset(expr.restriction_attributes)
    return Seed(name, items, attributes)


def graph_reach(graph: nx.MultiDiGraph, table: str, direction: str) -> set[str]:
    """
    The tables a traversal of the dependency graph can reach, with every part.

    Like ``TraversalSchema.reach``, with each master grouped with all its parts
    by name, owned or not. The result contains the exact reach, and the tables a
    schema description needs: describe these, then plan on the description.
    """
    parts = defaultdict(set)
    for node in graph.nodes:
        master = extract_master(node)
        if master is not None and master in graph:
            parts[master].add(node)

    def closure(tables, step):
        seen = set(tables)
        stack = list(seen)
        while stack:
            t = stack.pop()
            owner = extract_master(t)
            master = owner if owner is not None and owner in parts else t
            for n in (set(step(t)) | {master} | parts.get(master, set())) - seen:
                seen.add(n)
                stack.append(n)
        return seen

    if direction == "both":
        return closure(closure({table}, graph.successors), graph.predecessors)
    _check_direction(direction)
    return closure({table}, graph.successors if direction == "down" else graph.predecessors)


def load_dependencies(connection, direction: str, schema_names: Iterable[str] = ()):
    """
    Load every schema a traversal in ``direction`` can reach (diagram.md, "Unloaded schemas").

    Starting from the activated schemas and ``schema_names``, schemas that
    reference them (``down``) or that they reference (``up``) are added until no
    new one appears. For ``both``, the downward closure comes first, then the
    upward closure of the result. The dependency graph is reloaded only if a
    schema is missing from it.

    Returns
    -------
    Dependencies
        ``connection.dependencies``.
    """
    adapter = connection.adapter
    known = set(connection.schemas) | set(schema_names)
    for step in ("down", "up") if direction == "both" else (direction,):
        find = adapter.find_downstream_schemas_sql if step == "down" else adapter.find_upstream_schemas_sql
        while True:
            listed = ", ".join(adapter.quote_string(s) for s in known)
            new = {row[0] for row in connection.query(find(listed))} - known
            if not new:
                break
            known |= new
    dependencies = connection.dependencies
    if not (dependencies._loaded and known <= dependencies._loaded_schemas):
        dependencies.load(force=True, schema_names=known)
    return dependencies
