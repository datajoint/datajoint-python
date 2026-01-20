"""
Foreign key dependency graph for DataJoint schemas.

This module provides the Dependencies class that tracks foreign key
relationships between tables and supports topological sorting for
proper ordering of operations like delete and drop.
"""

from __future__ import annotations

import itertools
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


def topo_sort(graph: nx.DiGraph) -> list[str]:
    """
    Topological sort keeping part tables with their masters.

    Parameters
    ----------
    graph : nx.DiGraph
        Dependency graph.

    Returns
    -------
    list[str]
        Table names in topological order with parts following masters.
    """

    graph = nx.DiGraph(graph)  # make a copy

    # collapse alias nodes
    alias_nodes = [node for node in graph if node.isdigit()]
    for node in alias_nodes:
        try:
            direct_edge = (
                next(x for x in graph.in_edges(node))[0],
                next(x for x in graph.out_edges(node))[1],
            )
        except StopIteration:
            pass  # a disconnected alias node
        else:
            graph.add_edge(*direct_edge)
    graph.remove_nodes_from(alias_nodes)

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


class Dependencies(nx.DiGraph):
    """
    Graph of foreign key dependencies between loaded tables.

    Extends NetworkX DiGraph to track foreign key relationships and
    support operations like cascade delete and topological ordering.

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
        self._node_alias_count = itertools.count()
        self._loaded = False
        super().__init__(self)

    def clear(self) -> None:
        """Clear the graph and reset loaded state."""
        self._loaded = False
        super().clear()

    def load(self, force: bool = True) -> None:
        """
        Load dependencies for all loaded schemas.

        Called before operations requiring dependencies: delete, drop,
        populate, progress.

        Parameters
        ----------
        force : bool, optional
            If True (default), reload even if already loaded.
        """
        # reload from scratch to prevent duplication of renamed edges
        if self._loaded and not force:
            return

        self.clear()

        # Get adapter for backend-specific SQL generation
        adapter = self._conn.adapter

        # Build schema list for IN clause
        schemas_list = ", ".join(adapter.quote_string(s) for s in self._conn.schemas)

        # Backend-specific queries for primary keys and foreign keys
        # Note: Both PyMySQL and psycopg2 use %s placeholders, so escape % as %%
        like_pattern = "'~%%'"

        if adapter.backend == "mysql":
            # MySQL: use concat() and MySQL-specific information_schema columns
            tab_expr = "concat('`', table_schema, '`.`', table_name, '`')"

            # load primary key info (MySQL uses constraint_name='PRIMARY')
            keys = self._conn.query(
                f"""
                SELECT {tab_expr} as tab, column_name
                FROM information_schema.key_column_usage
                WHERE table_name NOT LIKE {like_pattern}
                    AND table_schema in ({schemas_list})
                    AND constraint_name='PRIMARY'
                """
            )
            pks = defaultdict(set)
            for key in keys:
                pks[key[0]].add(key[1])

            # load foreign keys (MySQL has referenced_* columns)
            ref_tab_expr = "concat('`', referenced_table_schema, '`.`', referenced_table_name, '`')"
            fk_keys = self._conn.query(
                f"""
                SELECT constraint_name,
                    {tab_expr} as referencing_table,
                    {ref_tab_expr} as referenced_table,
                    column_name, referenced_column_name
                FROM information_schema.key_column_usage
                WHERE referenced_table_name NOT LIKE {like_pattern}
                    AND (referenced_table_schema in ({schemas_list})
                         OR referenced_table_schema is not NULL AND table_schema in ({schemas_list}))
                """,
                as_dict=True,
            )
        else:
            # PostgreSQL: use || concatenation and different query structure
            tab_expr = "'\"' || kcu.table_schema || '\".\"' || kcu.table_name || '\"'"

            # load primary key info (PostgreSQL uses constraint_type='PRIMARY KEY')
            keys = self._conn.query(
                f"""
                SELECT {tab_expr} as tab, kcu.column_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc
                    ON kcu.constraint_name = tc.constraint_name
                    AND kcu.table_schema = tc.table_schema
                WHERE kcu.table_name NOT LIKE {like_pattern}
                    AND kcu.table_schema in ({schemas_list})
                    AND tc.constraint_type = 'PRIMARY KEY'
                """
            )
            pks = defaultdict(set)
            for key in keys:
                pks[key[0]].add(key[1])

            # load foreign keys using pg_constraint system catalogs
            # The information_schema approach creates a Cartesian product for composite FKs
            # because constraint_column_usage doesn't have ordinal_position.
            # Using pg_constraint with unnest(conkey, confkey) WITH ORDINALITY gives correct mapping.
            fk_keys = self._conn.query(
                f"""
                SELECT
                    c.conname as constraint_name,
                    '"' || ns1.nspname || '"."' || cl1.relname || '"' as referencing_table,
                    '"' || ns2.nspname || '"."' || cl2.relname || '"' as referenced_table,
                    a1.attname as column_name,
                    a2.attname as referenced_column_name
                FROM pg_constraint c
                JOIN pg_class cl1 ON c.conrelid = cl1.oid
                JOIN pg_namespace ns1 ON cl1.relnamespace = ns1.oid
                JOIN pg_class cl2 ON c.confrelid = cl2.oid
                JOIN pg_namespace ns2 ON cl2.relnamespace = ns2.oid
                CROSS JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS cols(conkey, confkey, ord)
                JOIN pg_attribute a1 ON a1.attrelid = cl1.oid AND a1.attnum = cols.conkey
                JOIN pg_attribute a2 ON a2.attrelid = cl2.oid AND a2.attnum = cols.confkey
                WHERE c.contype = 'f'
                    AND cl1.relname NOT LIKE {like_pattern}
                    AND (ns2.nspname in ({schemas_list})
                         OR ns1.nspname in ({schemas_list}))
                ORDER BY c.conname, cols.ord
                """,
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

        # add edges to the graph
        for fk in fks.values():
            props = dict(
                primary=set(fk["attr_map"]) <= set(pks[fk["referencing_table"]]),
                attr_map=fk["attr_map"],
                aliased=any(k != v for k, v in fk["attr_map"].items()),
                multi=set(fk["attr_map"]) != set(pks[fk["referencing_table"]]),
            )
            if not props["aliased"]:
                self.add_edge(fk["referenced_table"], fk["referencing_table"], **props)
            else:
                # for aliased dependencies, add an extra node in the format '1', '2', etc
                alias_node = "%d" % next(self._node_alias_count)
                self.add_node(alias_node)
                self.add_edge(fk["referenced_table"], alias_node, **props)
                self.add_edge(alias_node, fk["referencing_table"], **props)

        if not nx.is_directed_acyclic_graph(self):
            raise DataJointError("DataJoint can only work with acyclic dependencies")
        self._loaded = True

    def topo_sort(self) -> list[str]:
        """
        Return table names in topological order.

        Returns
        -------
        list[str]
            Table names sorted topologically.
        """
        return topo_sort(self)

    def parents(self, table_name: str, primary: bool | None = None) -> dict:
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
        dict
            Mapping of parent table name to edge properties.
        """
        self.load(force=False)
        return {p[0]: p[2] for p in self.in_edges(table_name, data=True) if primary is None or p[2]["primary"] == primary}

    def children(self, table_name: str, primary: bool | None = None) -> dict:
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
        dict
            Mapping of child table name to edge properties.
        """
        self.load(force=False)
        return {p[1]: p[2] for p in self.out_edges(table_name, data=True) if primary is None or p[2]["primary"] == primary}

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
