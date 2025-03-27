import itertools
import re
from collections import defaultdict

import networkx as nx

from .errors import DataJointError


def extract_master(part_table):
    """
    given a part table name, return master part. None if not a part table
    """
    match = re.match(r"(?P<master>`\w+`.`#?\w+)__\w+`", part_table)
    return match["master"] + "`" if match else None


def topo_sort(graph):
    """
    topological sort of a dependency graph that keeps part tables together with their masters
    :return: list of table names in topological order
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
    The graph of dependencies (foreign keys) between loaded tables.

    Note: the 'connection' argument should normally be supplied;
    Empty use is permitted to facilitate use of networkx algorithms which
    internally create objects with the expectation of empty constructors.
    See also: https://github.com/datajoint/datajoint-python/pull/443
    """

    def __init__(self, connection=None):
        self._conn = connection
        self._node_alias_count = itertools.count()
        self._loaded = False
        super().__init__(self)

    def clear(self):
        self._loaded = False
        super().clear()

    def load(self, force=True):
        """
        Load dependencies for all loaded schemas.
        This method gets called before any operation that requires dependencies: delete, drop, populate, progress.
        """
        # reload from scratch to prevent duplication of renamed edges
        if self._loaded and not force:
            return

        self.clear()

        # load primary key info
        keys = self._conn.query(
            """
                SELECT
                    concat('`', table_schema, '`.`', table_name, '`') as tab, column_name
                FROM information_schema.key_column_usage
                WHERE table_name not LIKE "~%%" AND table_schema in ('{schemas}') AND constraint_name="PRIMARY"
                """.format(
                schemas="','".join(self._conn.schemas)
            )
        )
        pks = defaultdict(set)
        for key in keys:
            pks[key[0]].add(key[1])

        # add nodes to the graph
        for n, pk in pks.items():
            self.add_node(n, primary_key=pk)

        # load foreign keys
        keys = (
            {k.lower(): v for k, v in elem.items()}
            for elem in self._conn.query(
                """
        SELECT constraint_name,
            concat('`', table_schema, '`.`', table_name, '`') as referencing_table,
            concat('`', referenced_table_schema, '`.`',  referenced_table_name, '`') as referenced_table,
            column_name, referenced_column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name NOT LIKE "~%%" AND (referenced_table_schema in ('{schemas}') OR
            referenced_table_schema is not NULL AND table_schema in ('{schemas}'))
        """.format(
                    schemas="','".join(self._conn.schemas)
                ),
                as_dict=True,
            )
        )
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

    def topo_sort(self):
        """:return: list of tables names in topological order"""
        return topo_sort(self)

    def parents(self, table_name, primary=None):
        """
        :param table_name: `schema`.`table`
        :param primary: if None, then all parents are returned. If True, then only foreign keys composed of
            primary key attributes are considered.  If False, the only foreign keys including at least one non-primary
            attribute are considered.
        :return: dict of tables referenced by the foreign keys of table
        """
        self.load(force=False)
        return {
            p[0]: p[2]
            for p in self.in_edges(table_name, data=True)
            if primary is None or p[2]["primary"] == primary
        }

    def children(self, table_name, primary=None):
        """
        :param table_name: `schema`.`table`
        :param primary: if None, then all children are returned. If True, then only foreign keys composed of
            primary key attributes are considered.  If False, the only foreign keys including at least one non-primary
            attribute are considered.
        :return: dict of tables referencing the table through foreign keys
        """
        self.load(force=False)
        return {
            p[1]: p[2]
            for p in self.out_edges(table_name, data=True)
            if primary is None or p[2]["primary"] == primary
        }

    def descendants(self, full_table_name):
        """
        :param full_table_name:  In form `schema`.`table_name`
        :return: all dependent tables sorted in topological order.  Self is included.
        """
        self.load(force=False)
        nodes = self.subgraph(nx.descendants(self, full_table_name))
        return [full_table_name] + nodes.topo_sort()

    def ancestors(self, full_table_name):
        """
        :param full_table_name:  In form `schema`.`table_name`
        :return: all dependent tables sorted in topological order.  Self is included.
        """
        self.load(force=False)
        nodes = self.subgraph(nx.ancestors(self, full_table_name))
        return reversed(nodes.topo_sort() + [full_table_name])
