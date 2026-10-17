"""
Foreign key dependency graph for DataJoint schemas.

This module provides the Dependencies class that tracks foreign key
relationships between tables and supports topological sorting for
proper ordering of operations like delete and drop.
"""

from __future__ import annotations

import re
from collections import defaultdict

import networkx as nx

from .errors import DataJointError


def extract_master(part_table: str) -> str | None:
    r"""
    Extract master table name from a part table name.

    Parameters
    ----------
    part_table : str
        Full table name (e.g., ```\`schema\`.\`master__part\```).

    Returns
    -------
    str or None
        Master table name if part_table is a part table, None otherwise.
    """
    # Match both MySQL backticks and PostgreSQL double quotes
    # MySQL: `schema`.`master__part`
    # PostgreSQL: "schema"."master__part"
    match = re.match(r'(?P<master>(?P<q>[`"])[\w]+(?P=q)\.(?P=q)#?[\w]+)__[\w]+(?P=q)', part_table)
    if match:
        q = match["q"]
        return match["master"] + q
    return None


#: How to read a chain of tables joined by `` -> `` in an error message.
CHAIN_LEGEND = "each table references the one before it or is the master of the part before it"


def owned_parts(graph: nx.MultiDiGraph) -> set[str]:
    """
    Return the part tables that are owned by their masters.

    A part is owned by its master when a foreign key in the part's primary key
    references the master, or references another owned part of the same master.
    The reference may rename the attributes. Only owned parts form a group with
    their master; every other part (one with no reference to its master, or only a
    secondary or nullable one, or only one through a part that is not owned) is
    treated as a separate table.

    Each foreign key is its own edge, renamed ones included, and carries a
    ``primary`` flag that is True when all its attributes are in the referencing
    table's primary key. An edge without the flag, as in a graph built by hand,
    counts as primary.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph.

    Returns
    -------
    set[str]
        Full names of the owned parts.
    """
    masters = {extract_master(node) for node in graph.nodes} & set(graph.nodes)
    owned = set()
    for master in masters:
        frontier = [master]
        while frontier:
            for _, child, props in graph.out_edges(frontier.pop(), data=True):
                if child not in owned and extract_master(child) == master and props.get("primary", True):
                    owned.add(child)
                    frontier.append(child)
    return owned


def _in_group(table: str, master: str, owned: set[str]) -> bool:
    """Whether ``table`` is ``master`` or one of its owned parts."""
    return table == master or (table in owned and extract_master(table) == master)


def is_owned(graph: nx.MultiDiGraph, part: str, references: list[tuple[str, bool]]) -> bool:
    """
    Tell whether a new part table will be owned by its master (see :func:`owned_parts`).

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph of the declared tables, which does not include ``part``.
    part : str
        Full name of the part table being declared.
    references : list[tuple[str, bool]]
        One ``(parent_table, primary)`` pair per foreign key of the part, where
        ``primary`` is True when all the foreign key's attributes are in the
        part's primary key.

    Returns
    -------
    bool
        True if a primary-key reference goes to the master or to an owned part of it.
    """
    master = extract_master(part)
    if master not in graph:
        return False
    owned = owned_parts(graph)
    return any(primary and _in_group(parent, master, owned) for parent, primary in references)


def group_graph(graph: nx.MultiDiGraph, owned: set[str] | None = None) -> nx.MultiDiGraph:
    """
    Copy a dependency graph and add an edge from each owned part to its master.

    A master's rows are inserted and deleted together with its owned parts' rows,
    so a master depends on everything its owned parts depend on. With the added
    edges, ``nx.ancestors`` gives a table's ancestors where reaching a master also
    reaches all of its owned parts, and ``nx.descendants`` gives the converse,
    where reaching an owned part also reaches its master. These are the
    dependencies that :func:`topo_sort` orders. Parts that are not owned are
    treated as separate tables.

    The added edges form a cycle with each owned part, so the result is for
    reachability only, not for sorting.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph.
    owned : set[str], optional
        The owned parts, if already computed with :func:`owned_parts`.

    Returns
    -------
    nx.MultiDiGraph
        A copy of ``graph`` with an edge from each owned part to its master.
    """
    owned = owned_parts(graph) if owned is None else owned
    graph = nx.MultiDiGraph(graph)
    for part in list(graph.nodes):
        if part in owned:
            graph.add_edge(part, extract_master(part), key="master")
    return graph


def group_ancestors(graph: nx.MultiDiGraph, table: str) -> set[str]:
    """
    Return a table and its ancestors, where reaching a master reaches all its owned parts.

    The closure starts from ``table`` and adds the parents of every table in it,
    and all the owned parts of every master in it, until nothing new is added. An
    owned part reaches its master through its own references.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph.
    table : str
        Full table name.

    Returns
    -------
    set[str]
        ``table`` and every table it depends on.
    """
    return {table} | nx.ancestors(group_graph(graph), table)


def group_cycle(graph: nx.MultiDiGraph, part: str, parents: list[str]) -> list[str] | None:
    """
    Find the cycle that the references of a new owned part would close.

    An owned part may reference its master, an owned part of the same master, or
    a table outside its master-part group, which includes the master's parts that
    are not owned. A reference to a table outside the group closes a cycle when
    that table already depends on the part's master (see :func:`group_ancestors`):
    the group would then depend on itself, and :func:`topo_sort` could no longer
    order its tables.

    A new table has no children yet, so only the references of a new owned part
    to tables outside its group can close such a cycle. A part that is not owned
    is a separate table and closes none (see :func:`is_owned`). Checking owned
    parts when they are declared therefore keeps every cycle out.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph of the declared tables, which does not include ``part``.
    part : str
        Full name of the owned part table being declared.
    parents : list[str]
        Full names of the tables the part references.

    Returns
    -------
    list[str] or None
        The cycle as a chain of tables joined by `` -> `` that starts and
        ends at the part's master, or None if the references close no cycle.
    """
    master = extract_master(part)
    if master not in graph:
        return None
    owned = owned_parts(graph)
    outside = [p for p in parents if p in graph and not _in_group(p, master, owned)]
    if not outside:
        return None
    extended = group_graph(graph, owned)
    for parent in outside:
        try:
            path = nx.shortest_path(extended, master, parent)
        except nx.NetworkXNoPath:
            continue
        return path + [part, master]
    return None


def _reachable(graph: nx.MultiDiGraph, sources: set[str], upstream: bool) -> set[str]:
    """Return the tables reachable from any of ``sources``, sources excluded unless reached."""
    step = graph.predecessors if upstream else graph.successors
    reached = set()
    frontier = list(sources)
    while frontier:
        for node in step(frontier.pop()):
            if node not in reached:
                reached.add(node)
                frontier.append(node)
    return reached


def _cycle_error(graph: nx.MultiDiGraph) -> DataJointError:
    """
    Describe a cycle in the graph that :func:`topo_sort` failed to sort.

    ``graph`` carries the edges ``topo_sort`` adds from a part's parents to its
    master, each marked with the part it stands for.
    """
    edges = nx.find_cycle(graph)
    # Start the chain at a master that the cycle reaches through one of its parts.
    added = [i for i, (u, v, k) in enumerate(edges) if graph.edges[u, v, k].get("part")]
    if added:
        edges = edges[added[0] + 1 :] + edges[: added[0] + 1]
    chain = [edges[0][0]]
    parts = []
    for u, v, k in edges:
        if part := graph.edges[u, v, k].get("part"):
            chain.append(part)
            parts.append(part)
        chain.append(v)
    message = f"Cannot order tables whose dependencies form a cycle: {' -> '.join(chain)} ({CHAIN_LEGEND})."
    if parts:
        names = " and ".join(parts)
        which = f"part table {names} references" if len(parts) == 1 else f"part tables {names} each reference"
        message += (
            f" The cycle runs through master-part groups: {which} a table that depends on its own master, which"
            " can no longer be declared. To restore delete, drop and trace on these tables, drop "
            f"{'that part' if len(parts) == 1 else 'one of these parts'} with `drop(part_integrity='ignore')` and"
            " declare it without that reference."
        )
    return DataJointError(message)


def topo_sort(graph: nx.MultiDiGraph, nodes=None) -> list[str]:
    """
    Topological sort keeping owned part tables with their masters.

    Each owned part (see :func:`owned_parts`) is placed right after its master, so
    the master is ordered after every table its owned parts reference. Parts that
    are not owned are sorted as separate tables.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph.
    nodes : iterable of str, optional
        Tables to sort. If given, only these tables and the tables on dependency
        paths between them are sorted, so a cycle elsewhere in the graph does not
        prevent ordering them, and only these tables are returned. Tables not in
        the graph are ignored. If None (default), all tables are sorted.

    Returns
    -------
    list[str]
        Table names in topological order with owned parts following their masters.

    Raises
    ------
    DataJointError
        If the dependencies of the tables to sort form a cycle, for example one
        that runs through a master and its owned parts.
    """
    owned = owned_parts(graph)
    if nodes is None:
        graph = nx.MultiDiGraph(graph)  # make a mutable copy
    else:
        # Sort the tables on paths between the requested ones too, so that their
        # order respects every dependency between them.
        nodes = set(nodes) & set(graph.nodes)
        keep = nodes | (_reachable(graph, nodes, upstream=False) & _reachable(graph, nodes, upstream=True))
        subgraph = nx.MultiDiGraph()
        subgraph.add_nodes_from(keep)
        subgraph.add_edges_from((u, v) for u, v in graph.edges(keep) if v in keep)
        graph = subgraph

    # Add owned parts' dependencies to their masters' dependencies
    # to ensure correct topological ordering of the masters.
    # Parts that are not owned are sorted as separate tables.
    for part in graph:
        if part in owned and (master := extract_master(part)) in graph:
            for parent, _ in graph.in_edges(part):
                if not _in_group(parent, master, owned):
                    # if parent is neither master nor owned part of master
                    graph.add_edge(parent, master, part=part)
    try:
        sorted_nodes = list(nx.topological_sort(graph))
    except nx.NetworkXUnfeasible:
        raise _cycle_error(graph) from None

    # bring parts up to their masters
    pos = len(sorted_nodes) - 1
    placed = set()
    while pos > 1:
        part = sorted_nodes[pos]
        if part not in owned or (master := extract_master(part)) not in graph or part in placed:
            pos -= 1
        else:
            placed.add(part)
            insert_pos = sorted_nodes.index(master) + 1
            if pos > insert_pos:
                # move the part to the position immediately after its master
                del sorted_nodes[pos]
                sorted_nodes.insert(insert_pos, part)

    if nodes is not None:
        return [node for node in sorted_nodes if node in nodes]
    return sorted_nodes


class Dependencies(nx.MultiDiGraph):
    """
    Graph of foreign key dependencies between loaded tables.

    Extends NetworkX MultiDiGraph to track foreign key relationships and
    support operations like cascade delete and topological ordering. A
    ``MultiDiGraph`` is used so that multiple foreign keys between the same
    pair of tables (including renamed/aliased ones) are represented as
    distinct parallel edges keyed by ``(parent, child, key)`` — no synthetic
    intermediate "alias" nodes are needed.

    Parameters
    ----------
    connection : Connection, optional
        Database connection. May be None to support NetworkX algorithms
        that create objects with empty constructors.

    Attributes
    ----------
    _conn : Connection or None
        Database connection.
    _loaded : bool
        Whether dependencies have been loaded from the database.

    Notes
    -----
    Empty constructor use is permitted to facilitate NetworkX algorithms.
    See: https://github.com/datajoint/datajoint-python/pull/443
    """

    def __init__(self, connection=None) -> None:
        self._conn = connection
        self._loaded = False
        self._loaded_schemas = set()  # schema names currently represented in the graph
        super().__init__(self)

    def clear(self) -> None:
        """Clear the graph and reset loaded state."""
        self._loaded = False
        self._loaded_schemas = set()
        super().clear()

    def load(self, force: bool = True, schema_names: set[str] | None = None) -> None:
        """
        Load dependencies for the given schemas.

        Called before operations requiring dependencies: delete, drop,
        populate, progress.

        Parameters
        ----------
        force : bool, optional
            If True (default), reload even if already loaded.
        schema_names : set[str], optional
            Schema names to load. If None, uses all activated schemas.
        """
        # reload from scratch to prevent duplication of renamed edges
        if self._loaded and not force:
            return

        self.clear()

        # Get adapter for backend-specific SQL generation
        adapter = self._conn.adapter

        # Build schema list for IN clause
        names = schema_names if schema_names is not None else set(self._conn.schemas)
        self._loaded_schemas = set(names)
        if not names:
            self._loaded = True
            return
        schemas_list = ", ".join(adapter.quote_string(s) for s in names)

        # Load primary keys and foreign keys via adapter methods
        # Note: Both PyMySQL and psycopg use %s placeholders, so escape % as %%
        like_pattern = "'~%%'"

        # load primary key info
        keys = self._conn.query(adapter.load_primary_keys_sql(schemas_list, like_pattern))
        pks = defaultdict(set)
        for key in keys:
            pks[key[0]].add(key[1])

        # load foreign keys
        fk_keys = self._conn.query(
            adapter.load_foreign_keys_sql(schemas_list, like_pattern),
            as_dict=True,
        )

        # add nodes to the graph
        for n, pk in pks.items():
            self.add_node(n, primary_key=pk)

        # Process foreign keys (same for both backends)
        keys = ({k.lower(): v for k, v in elem.items()} for elem in fk_keys)
        fks = defaultdict(lambda: dict(attr_map=dict()))
        for key in keys:
            d = fks[
                (
                    key["constraint_name"],
                    key["referencing_table"],
                    key["referenced_table"],
                )
            ]
            d["referencing_table"] = key["referencing_table"]
            d["referenced_table"] = key["referenced_table"]
            d["attr_map"][key["column_name"]] = key["referenced_column_name"]

        # Add one edge per foreign key, keyed by the FK's child-side attribute
        # names as a tuple (in declaration order). This key uniquely identifies
        # a parallel edge between a pair of tables — every FK to a given parent
        # references its primary key, so parallel FKs differ only in their
        # referencing columns — and is derived purely from the schema (no
        # DB-internal constraint names). A stable, meaningful key makes graph
        # merges (e.g. ``Diagram + Diagram``) idempotent instead of duplicating
        # shared edges, so no synthetic alias node is needed.
        for fk in fks.values():
            props = dict(
                primary=set(fk["attr_map"]) <= set(pks[fk["referencing_table"]]),
                attr_map=fk["attr_map"],
                aliased=any(k != v for k, v in fk["attr_map"].items()),
                multi=set(fk["attr_map"]) != set(pks[fk["referencing_table"]]),
            )
            self.add_edge(
                fk["referenced_table"],
                fk["referencing_table"],
                key=tuple(fk["attr_map"]),
                **props,
            )

        if not nx.is_directed_acyclic_graph(self):
            raise DataJointError("DataJoint can only work with acyclic dependencies")
        self._loaded = True

    def load_all_downstream(self) -> None:
        """
        Load dependencies including all downstream schemas reachable via FK chains.

        Iteratively discovers schemas that reference the currently loaded
        schemas, expanding the dependency graph until no new schemas are
        found. This ensures that cascade delete and drop reach all
        dependent tables, even those in schemas that haven't been
        explicitly activated.

        Called automatically by ``Diagram.cascade()`` and ``Table.drop()``.
        Call manually before constructing a ``Diagram`` to include
        cross-schema dependencies in visualization::

            conn.dependencies.load_all_downstream()
            dj.Diagram(schema)  # now includes all downstream schemas
        """
        adapter = self._conn.adapter
        known_schemas = set(self._conn.schemas)
        if not known_schemas:
            self.load()
            return

        while True:
            schemas_list = ", ".join(adapter.quote_string(s) for s in known_schemas)
            result = self._conn.query(adapter.find_downstream_schemas_sql(schemas_list))
            new_schemas = {row[0] for row in result} - known_schemas
            if not new_schemas:
                break
            known_schemas |= new_schemas

        # Skip the expensive rebuild when the graph already contains every needed
        # schema, so repeated calls within one operation don't reload the whole
        # dependency tree. See #1493.
        if self._loaded and known_schemas <= self._loaded_schemas:
            return
        self.load(force=True, schema_names=known_schemas)

    def load_all_upstream(self, schema_names: set[str] | None = None) -> None:
        """
        Load dependencies including all upstream schemas referenced via FK chains.

        Iteratively discovers schemas that the currently loaded schemas
        reference, expanding the dependency graph until no new schemas
        are found. This ensures that upstream restriction propagation
        (``Diagram.trace()``) reaches all ancestor tables, including
        those in schemas the user has not explicitly activated.

        Called automatically by ``Diagram.trace()``. Symmetric to
        :meth:`load_all_downstream`.

        Parameters
        ----------
        schema_names : set[str], optional
            Schemas to start from in addition to the activated schemas, such
            as the schemas of the tables a new table references.
        """
        adapter = self._conn.adapter
        known_schemas = set(self._conn.schemas) | set(schema_names or ())
        if not known_schemas:
            self.load()
            return

        while True:
            schemas_list = ", ".join(adapter.quote_string(s) for s in known_schemas)
            result = self._conn.query(adapter.find_upstream_schemas_sql(schemas_list))
            new_schemas = {row[0] for row in result} - known_schemas
            if not new_schemas:
                break
            known_schemas |= new_schemas

        # Skip the expensive rebuild when the graph already contains every needed
        # schema, so repeated Diagram.trace() calls within one populate() don't
        # reload the whole dependency tree per key. See #1493.
        if self._loaded and known_schemas <= self._loaded_schemas:
            return
        self.load(force=True, schema_names=known_schemas)

    def topo_sort(self, nodes=None) -> list[str]:
        """
        Return table names in topological order.

        Parameters
        ----------
        nodes : iterable of str, optional
            Tables to sort; all tables if None. See :func:`topo_sort`.

        Returns
        -------
        list[str]
            Table names sorted topologically.
        """
        return topo_sort(self, nodes)

    def parents(self, table_name: str, primary: bool | None = None) -> list[tuple[str, dict]]:
        r"""
        Get tables referenced by this table's foreign keys.

        Parameters
        ----------
        table_name : str
            Full table name (```\`schema\`.\`table\```).
        primary : bool, optional
            If None, return all parents. If True, only FK composed entirely
            of primary key attributes. If False, only FK with at least one
            non-primary attribute.

        Returns
        -------
        list[tuple[str, dict]]
            One ``(parent_table_name, edge_properties)`` pair per foreign key.
            A parent may appear more than once when this table has multiple
            (e.g. renamed) foreign keys to it — each is a distinct parallel edge.
        """
        self.load(force=False)
        return [
            (u, props)
            for u, _, props in self.in_edges(table_name, data=True)
            if primary is None or props["primary"] == primary
        ]

    def children(self, table_name: str, primary: bool | None = None) -> list[tuple[str, dict]]:
        r"""
        Get tables that reference this table through foreign keys.

        Parameters
        ----------
        table_name : str
            Full table name (```\`schema\`.\`table\```).
        primary : bool, optional
            If None, return all children. If True, only FK composed entirely
            of primary key attributes. If False, only FK with at least one
            non-primary attribute.

        Returns
        -------
        list[tuple[str, dict]]
            One ``(child_table_name, edge_properties)`` pair per foreign key.
            A child may appear more than once when it has multiple (e.g. renamed)
            foreign keys to this table — each is a distinct parallel edge.
        """
        self.load(force=False)
        return [
            (v, props)
            for _, v, props in self.out_edges(table_name, data=True)
            if primary is None or props["primary"] == primary
        ]

    def descendants(self, full_table_name: str) -> list[str]:
        r"""
        Get all dependent tables in topological order.

        Parameters
        ----------
        full_table_name : str
            Full table name (```\`schema\`.\`table_name\```).

        Returns
        -------
        list[str]
            Dependent tables in topological order. Self is included first.
        """
        self.load(force=False)
        nodes = self.subgraph(nx.descendants(self, full_table_name))
        return [full_table_name] + nodes.topo_sort()

    def ancestors(self, full_table_name: str) -> list[str]:
        r"""
        Get all ancestor tables in reverse topological order.

        Parameters
        ----------
        full_table_name : str
            Full table name (```\`schema\`.\`table_name\```).

        Returns
        -------
        list[str]
            Ancestor tables in reverse topological order. Self is included last.
        """
        self.load(force=False)
        nodes = self.subgraph(nx.ancestors(self, full_table_name))
        return reversed(nodes.topo_sort() + [full_table_name])
