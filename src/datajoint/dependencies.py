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


def topo_sort(graph: nx.MultiDiGraph) -> list[str]:
    """
    Topological sort keeping part tables with their masters.

    Parameters
    ----------
    graph : nx.MultiDiGraph
        Dependency graph.

    Returns
    -------
    list[str]
        Table names in topological order with parts following masters.
    """

    graph = nx.MultiDiGraph(graph)  # make a mutable copy

    # Add parts' dependencies to their masters' dependencies
    # to ensure correct topological ordering of the masters.
    for part in graph:
        # find the part's master
        if (master := extract_master(part)) in graph:
            for edge in graph.in_edges(part):
                parent = edge[0]
                if master not in (parent, extract_master(parent)):
                    # if parent is neither master nor part of master
                    graph.add_edge(parent, master)
    sorted_nodes = list(nx.topological_sort(graph))

    # bring parts up to their masters
    pos = len(sorted_nodes) - 1
    placed = set()
    while pos > 1:
        part = sorted_nodes[pos]
        if (master := extract_master(part)) not in graph or part in placed:
            pos -= 1
        else:
            placed.add(part)
            insert_pos = sorted_nodes.index(master) + 1
            if pos > insert_pos:
                # move the part to the position immediately after its master
                del sorted_nodes[pos]
                sorted_nodes.insert(insert_pos, part)

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

    def load_all_upstream(self) -> None:
        """
        Load dependencies including all upstream schemas referenced via FK chains.

        Iteratively discovers schemas that the currently loaded schemas
        reference, expanding the dependency graph until no new schemas
        are found. This ensures that upstream restriction propagation
        (``Diagram.trace()``) reaches all ancestor tables, including
        those in schemas the user has not explicitly activated.

        Called automatically by ``Diagram.trace()``. Symmetric to
        :meth:`load_all_downstream`.
        """
        adapter = self._conn.adapter
        known_schemas = set(self._conn.schemas)
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

    def topo_sort(self) -> list[str]:
        """
        Return table names in topological order.

        Returns
        -------
        list[str]
            Table names sorted topologically.
        """
        return topo_sort(self)

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
