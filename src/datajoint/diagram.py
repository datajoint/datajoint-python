"""
Diagram visualization for DataJoint schemas.

This module provides the Diagram class for visualizing schema structure
as directed acyclic graphs showing tables and their foreign key relationships.
"""

from __future__ import annotations

import functools
import inspect
import io
import logging

import networkx as nx

from .dependencies import topo_sort
from .errors import DataJointError
from .settings import config
from .table import Table, lookup_class_name
from .user_tables import Computed, Imported, Lookup, Manual, Part, _AliasNode, _get_tier

try:
    from matplotlib import pyplot as plt

    plot_active = True
except ImportError:
    plot_active = False

try:
    from networkx.drawing.nx_pydot import pydot_layout

    diagram_active = True
except ImportError:
    diagram_active = False


logger = logging.getLogger(__name__.split(".")[0])


if not diagram_active:  # noqa: C901

    class Diagram:
        """
        Schema diagram (disabled).

        Diagram visualization requires matplotlib and pygraphviz packages.
        Install them to enable this feature.

        See Also
        --------
        https://docs.datajoint.com/how-to/installation/
        """

        def __init__(self, *args, **kwargs) -> None:
            logger.warning("Please install matplotlib and pygraphviz libraries to enable the Diagram feature.")

else:

    class Diagram(nx.DiGraph):
        """
        Schema diagram as a directed acyclic graph (DAG).

        Visualizes tables and foreign key relationships derived from
        ``connection.dependencies``.

        Parameters
        ----------
        source : Table, Schema, or module
            A table object, table class, schema, or module with a schema.
        context : dict, optional
            Namespace for resolving table class names. If None, uses caller's
            frame globals/locals.

        Examples
        --------
        >>> diag = dj.Diagram(schema.MyTable)
        >>> diag.draw()

        Operators:

        - ``diag1 + diag2`` - union of diagrams
        - ``diag1 - diag2`` - difference of diagrams
        - ``diag1 * diag2`` - intersection of diagrams
        - ``diag + n`` - expand n levels of successors (children)
        - ``diag - n`` - expand n levels of predecessors (parents)

        >>> dj.Diagram(schema.Table) + 1 - 1  # immediate ancestors and descendants

        Notes
        -----
        ``diagram + 1 - 1`` may differ from ``diagram - 1 + 1``.
        Only tables loaded in the connection are displayed.

        Layout direction is controlled via ``dj.config.display.diagram_direction``
        (default ``"TB"``). Use ``dj.config.override()`` to change temporarily::

            with dj.config.override(display_diagram_direction="LR"):
                dj.Diagram(schema).draw()
        """

        def __init__(self, source, context=None) -> None:
            if isinstance(source, Diagram):
                # copy constructor
                self.nodes_to_show = set(source.nodes_to_show)
                self._expanded_nodes = set(source._expanded_nodes)
                self.context = source.context
                super().__init__(source)
                return

            # get the caller's context
            if context is None:
                frame = inspect.currentframe().f_back
                self.context = dict(frame.f_globals, **frame.f_locals)
                del frame
            else:
                self.context = context

            # find connection in the source
            try:
                connection = source.connection
            except AttributeError:
                try:
                    connection = source.schema.connection
                except AttributeError:
                    raise DataJointError("Could not find database connection in %s" % repr(source[0]))

            # initialize graph from dependencies
            connection.dependencies.load()
            super().__init__(connection.dependencies)

            # Enumerate nodes from all the items in the list
            self.nodes_to_show = set()
            try:
                self.nodes_to_show.add(source.full_table_name)
            except AttributeError:
                try:
                    database = source.database
                except AttributeError:
                    try:
                        database = source.schema.database
                    except AttributeError:
                        raise DataJointError("Cannot plot Diagram for %s" % repr(source))
                for node in self:
                    # Handle both MySQL backticks and PostgreSQL double quotes
                    if node.startswith("`%s`" % database) or node.startswith('"%s"' % database):
                        self.nodes_to_show.add(node)
            # All nodes start as expanded
            self._expanded_nodes = set(self.nodes_to_show)

        @classmethod
        def from_sequence(cls, sequence) -> "Diagram":
            """
            Create combined Diagram from a sequence of sources.

            Parameters
            ----------
            sequence : iterable
                Sequence of table objects, classes, or schemas.

            Returns
            -------
            Diagram
                Union of diagrams: ``Diagram(arg1) + ... + Diagram(argn)``.
            """
            return functools.reduce(lambda x, y: x + y, map(Diagram, sequence))

        def add_parts(self) -> "Diagram":
            """
            Add part tables of all masters already in the diagram.

            Returns
            -------
            Diagram
                New diagram with part tables included.
            """

            def is_part(part, master):
                part = [s.strip("`") for s in part.split(".")]
                master = [s.strip("`") for s in master.split(".")]
                return master[0] == part[0] and master[1] + "__" == part[1][: len(master[1]) + 2]

            self = Diagram(self)  # copy
            self.nodes_to_show.update(n for n in self.nodes() if any(is_part(n, m) for m in self.nodes_to_show))
            return self

        def collapse(self) -> "Diagram":
            """
            Mark all nodes in this diagram as collapsed.

            Collapsed nodes are shown as a single node per schema. When combined
            with other diagrams using ``+``, expanded nodes win: if a node is
            expanded in either operand, it remains expanded in the result.

            Returns
            -------
            Diagram
                A copy of this diagram with all nodes collapsed.

            Examples
            --------
            >>> # Show schema1 expanded, schema2 collapsed into single nodes
            >>> dj.Diagram(schema1) + dj.Diagram(schema2).collapse()

            >>> # Collapse all three schemas together
            >>> (dj.Diagram(schema1) + dj.Diagram(schema2) + dj.Diagram(schema3)).collapse()

            >>> # Expand one table from collapsed schema
            >>> dj.Diagram(schema).collapse() + dj.Diagram(SingleTable)
            """
            result = Diagram(self)
            result._expanded_nodes = set()  # All nodes collapsed
            return result

        def __add__(self, arg) -> "Diagram":
            """
            Union or downstream expansion.

            Parameters
            ----------
            arg : Diagram or int
                Another Diagram for union, or positive int for downstream expansion.

            Returns
            -------
            Diagram
                Combined or expanded diagram.
            """
            result = Diagram(self)  # copy
            try:
                # Merge nodes and edges from the other diagram
                result.add_nodes_from(arg.nodes(data=True))
                result.add_edges_from(arg.edges(data=True))
                result.nodes_to_show.update(arg.nodes_to_show)
                # Merge contexts for class name lookups
                result.context = {**result.context, **arg.context}
                # Expanded wins: union of expanded nodes from both operands
                result._expanded_nodes = self._expanded_nodes | arg._expanded_nodes
            except AttributeError:
                try:
                    result.nodes_to_show.add(arg.full_table_name)
                    result._expanded_nodes.add(arg.full_table_name)
                except AttributeError:
                    for i in range(arg):
                        new = nx.algorithms.boundary.node_boundary(result, result.nodes_to_show)
                        if not new:
                            break
                        # add nodes referenced by aliased nodes
                        new.update(nx.algorithms.boundary.node_boundary(result, (a for a in new if a.isdigit())))
                        result.nodes_to_show.update(new)
                    # New nodes from expansion are expanded
                    result._expanded_nodes = result._expanded_nodes | result.nodes_to_show
            return result

        def __sub__(self, arg) -> "Diagram":
            """
            Difference or upstream expansion.

            Parameters
            ----------
            arg : Diagram or int
                Another Diagram for difference, or positive int for upstream expansion.

            Returns
            -------
            Diagram
                Reduced or expanded diagram.
            """
            self = Diagram(self)  # copy
            try:
                self.nodes_to_show.difference_update(arg.nodes_to_show)
            except AttributeError:
                try:
                    self.nodes_to_show.remove(arg.full_table_name)
                except AttributeError:
                    for i in range(arg):
                        graph = nx.DiGraph(self).reverse()
                        new = nx.algorithms.boundary.node_boundary(graph, self.nodes_to_show)
                        if not new:
                            break
                        # add nodes referenced by aliased nodes
                        new.update(nx.algorithms.boundary.node_boundary(graph, (a for a in new if a.isdigit())))
                        self.nodes_to_show.update(new)
            return self

        def __mul__(self, arg) -> "Diagram":
            """
            Intersection of two diagrams.

            Parameters
            ----------
            arg : Diagram
                Another Diagram.

            Returns
            -------
            Diagram
                Diagram with nodes present in both operands.
            """
            self = Diagram(self)  # copy
            self.nodes_to_show.intersection_update(arg.nodes_to_show)
            return self

        def topo_sort(self) -> list[str]:
            """
            Return nodes in topological order.

            Returns
            -------
            list[str]
                Node names in topological order.
            """
            return topo_sort(self)

        def _make_graph(self) -> nx.DiGraph:
            """
            Build graph object ready for drawing.

            Returns
            -------
            nx.DiGraph
                Graph with nodes relabeled to class names.
            """
            # mark "distinguished" tables, i.e. those that introduce new primary key
            # attributes
            # Filter nodes_to_show to only include nodes that exist in the graph
            valid_nodes = self.nodes_to_show.intersection(set(self.nodes()))
            for name in valid_nodes:
                foreign_attributes = set(
                    attr for p in self.in_edges(name, data=True) for attr in p[2]["attr_map"] if p[2]["primary"]
                )
                self.nodes[name]["distinguished"] = (
                    "primary_key" in self.nodes[name] and foreign_attributes < self.nodes[name]["primary_key"]
                )
            # include aliased nodes that are sandwiched between two displayed nodes
            gaps = set(nx.algorithms.boundary.node_boundary(self, valid_nodes)).intersection(
                nx.algorithms.boundary.node_boundary(nx.DiGraph(self).reverse(), valid_nodes)
            )
            nodes = valid_nodes.union(a for a in gaps if a.isdigit())
            # construct subgraph and rename nodes to class names
            graph = nx.DiGraph(nx.DiGraph(self).subgraph(nodes))
            nx.set_node_attributes(graph, name="node_type", values={n: _get_tier(n) for n in graph})
            # relabel nodes to class names
            mapping = {node: lookup_class_name(node, self.context) or node for node in graph.nodes()}
            new_names = list(mapping.values())
            if len(new_names) > len(set(new_names)):
                raise DataJointError("Some classes have identical names. The Diagram cannot be plotted.")
            nx.relabel_nodes(graph, mapping, copy=False)
            return graph

        def _apply_collapse(self, graph: nx.DiGraph) -> tuple[nx.DiGraph, dict[str, str]]:
            """
            Apply collapse logic to the graph.

            Nodes in nodes_to_show but not in _expanded_nodes are collapsed into
            single schema nodes.

            Parameters
            ----------
            graph : nx.DiGraph
                The graph from _make_graph().

            Returns
            -------
            tuple[nx.DiGraph, dict[str, str]]
                Modified graph and mapping of collapsed schema labels to their table count.
            """
            # Filter to valid nodes (those that exist in the underlying graph)
            valid_nodes = self.nodes_to_show.intersection(set(self.nodes()))
            valid_expanded = self._expanded_nodes.intersection(set(self.nodes()))

            # If all nodes are expanded, no collapse needed
            if valid_expanded >= valid_nodes:
                return graph, {}

            # Map full_table_names to class_names
            full_to_class = {node: lookup_class_name(node, self.context) or node for node in valid_nodes}
            class_to_full = {v: k for k, v in full_to_class.items()}

            # Identify expanded class names
            expanded_class_names = {full_to_class.get(node, node) for node in valid_expanded}

            # Identify nodes to collapse (class names)
            nodes_to_collapse = set(graph.nodes()) - expanded_class_names

            if not nodes_to_collapse:
                return graph, {}

            # Group collapsed nodes by schema
            collapsed_by_schema = {}  # schema_name -> list of class_names
            for class_name in nodes_to_collapse:
                full_name = class_to_full.get(class_name)
                if full_name:
                    parts = full_name.replace('"', "`").split("`")
                    if len(parts) >= 2:
                        schema_name = parts[1]
                        if schema_name not in collapsed_by_schema:
                            collapsed_by_schema[schema_name] = []
                        collapsed_by_schema[schema_name].append(class_name)

            if not collapsed_by_schema:
                return graph, {}

            # Determine labels for collapsed schemas
            schema_modules = {}
            for schema_name, class_names in collapsed_by_schema.items():
                schema_modules[schema_name] = set()
                for class_name in class_names:
                    cls = self._resolve_class(class_name)
                    if cls is not None and hasattr(cls, "__module__"):
                        module_name = cls.__module__.split(".")[-1]
                        schema_modules[schema_name].add(module_name)

            # Collect module names for ALL schemas in the diagram (not just collapsed)
            all_schema_modules = {}  # schema_name -> module_name
            for node in graph.nodes():
                full_name = class_to_full.get(node)
                if full_name:
                    parts = full_name.replace('"', "`").split("`")
                    if len(parts) >= 2:
                        db_schema = parts[1]
                        cls = self._resolve_class(node)
                        if cls is not None and hasattr(cls, "__module__"):
                            module_name = cls.__module__.split(".")[-1]
                            all_schema_modules[db_schema] = module_name

            # Check which module names are shared by multiple schemas
            module_to_schemas = {}
            for db_schema, module_name in all_schema_modules.items():
                if module_name not in module_to_schemas:
                    module_to_schemas[module_name] = []
                module_to_schemas[module_name].append(db_schema)

            ambiguous_modules = {m for m, schemas in module_to_schemas.items() if len(schemas) > 1}

            # Determine labels for collapsed schemas
            collapsed_labels = {}  # schema_name -> label
            for schema_name, modules in schema_modules.items():
                if len(modules) == 1:
                    module_name = next(iter(modules))
                    # Use database schema name if module is ambiguous
                    if module_name in ambiguous_modules:
                        label = schema_name
                    else:
                        label = module_name
                else:
                    label = schema_name
                collapsed_labels[schema_name] = label

            # Build counts using final labels
            collapsed_counts = {}  # label -> count of tables
            for schema_name, class_names in collapsed_by_schema.items():
                label = collapsed_labels[schema_name]
                collapsed_counts[label] = len(class_names)

            # Create new graph with collapsed nodes
            new_graph = nx.DiGraph()

            # Map old node names to new names (collapsed nodes -> schema label)
            node_mapping = {}
            for node in graph.nodes():
                full_name = class_to_full.get(node)
                if full_name:
                    parts = full_name.replace('"', "`").split("`")
                    if len(parts) >= 2 and node in nodes_to_collapse:
                        schema_name = parts[1]
                        node_mapping[node] = collapsed_labels[schema_name]
                    else:
                        node_mapping[node] = node
                else:
                    # Alias nodes - check if they should be collapsed
                    # An alias node should be collapsed if ALL its neighbors are collapsed
                    neighbors = set(graph.predecessors(node)) | set(graph.successors(node))
                    if neighbors and neighbors <= nodes_to_collapse:
                        # Get schema from first neighbor
                        neighbor = next(iter(neighbors))
                        full_name = class_to_full.get(neighbor)
                        if full_name:
                            parts = full_name.replace('"', "`").split("`")
                            if len(parts) >= 2:
                                schema_name = parts[1]
                                node_mapping[node] = collapsed_labels[schema_name]
                                continue
                    node_mapping[node] = node

            # Build reverse mapping: label -> schema_name
            label_to_schema = {label: schema for schema, label in collapsed_labels.items()}

            # Add nodes
            added_collapsed = set()
            for old_node, new_node in node_mapping.items():
                if new_node in collapsed_counts:
                    # This is a collapsed schema node
                    if new_node not in added_collapsed:
                        schema_name = label_to_schema.get(new_node, new_node)
                        new_graph.add_node(
                            new_node,
                            node_type=None,
                            collapsed=True,
                            table_count=collapsed_counts[new_node],
                            schema_name=schema_name,
                        )
                        added_collapsed.add(new_node)
                else:
                    new_graph.add_node(new_node, **graph.nodes[old_node])

            # Add edges (avoiding self-loops and duplicates)
            for src, dest, data in graph.edges(data=True):
                new_src = node_mapping[src]
                new_dest = node_mapping[dest]
                if new_src != new_dest and not new_graph.has_edge(new_src, new_dest):
                    new_graph.add_edge(new_src, new_dest, **data)

            return new_graph, collapsed_counts

        def _resolve_class(self, name: str):
            """
            Safely resolve a table class from a dotted name without eval().

            Parameters
            ----------
            name : str
                Dotted class name like "MyTable" or "Module.MyTable".

            Returns
            -------
            type or None
                The table class if found, otherwise None.
            """
            parts = name.split(".")
            obj = self.context.get(parts[0])
            for part in parts[1:]:
                if obj is None:
                    return None
                obj = getattr(obj, part, None)
            if obj is not None and isinstance(obj, type) and issubclass(obj, Table):
                return obj
            return None

        @staticmethod
        def _encapsulate_edge_attributes(graph: nx.DiGraph) -> None:
            """
            Encapsulate edge attr_map in double quotes for pydot compatibility.

            Modifies graph in place.

            See Also
            --------
            https://github.com/pydot/pydot/issues/258#issuecomment-795798099
            """
            for u, v, *_, edgedata in graph.edges(data=True):
                if "attr_map" in edgedata:
                    graph.edges[u, v]["attr_map"] = '"{0}"'.format(edgedata["attr_map"])

        @staticmethod
        def _encapsulate_node_names(graph: nx.DiGraph) -> None:
            """
            Encapsulate node names in double quotes for pydot compatibility.

            Modifies graph in place.

            See Also
            --------
            https://github.com/datajoint/datajoint-python/pull/1176
            """
            nx.relabel_nodes(
                graph,
                {node: '"{0}"'.format(node) for node in graph.nodes()},
                copy=False,
            )

        def make_dot(self):
            """
            Generate a pydot graph object.

            Returns
            -------
            pydot.Dot
                The graph object ready for rendering.

            Notes
            -----
            Layout direction is controlled via ``dj.config.display.diagram_direction``.
            Tables are grouped by schema, with the Python module name shown as the
            group label when available.
            """
            direction = config.display.diagram_direction
            graph = self._make_graph()

            # Apply collapse logic if needed
            graph, collapsed_counts = self._apply_collapse(graph)

            # Build schema mapping: class_name -> schema_name
            # Group by database schema, label with Python module name if 1:1 mapping
            schema_map = {}  # class_name -> schema_name
            schema_modules = {}  # schema_name -> set of module names

            for full_name in self.nodes_to_show:
                # Extract schema from full table name like `schema`.`table` or "schema"."table"
                parts = full_name.replace('"', "`").split("`")
                if len(parts) >= 2:
                    schema_name = parts[1]  # schema is between first pair of backticks
                    class_name = lookup_class_name(full_name, self.context) or full_name
                    schema_map[class_name] = schema_name

                    # Collect all module names for this schema
                    if schema_name not in schema_modules:
                        schema_modules[schema_name] = set()
                    cls = self._resolve_class(class_name)
                    if cls is not None and hasattr(cls, "__module__"):
                        module_name = cls.__module__.split(".")[-1]
                        schema_modules[schema_name].add(module_name)

            # Determine cluster labels: use module name if 1:1, else database schema name
            cluster_labels = {}  # schema_name -> label
            for schema_name, modules in schema_modules.items():
                if len(modules) == 1:
                    cluster_labels[schema_name] = next(iter(modules))
                else:
                    cluster_labels[schema_name] = schema_name

            # Disambiguate labels if multiple schemas share the same module name
            # (e.g., all defined in __main__ in a notebook)
            label_counts = {}
            for label in cluster_labels.values():
                label_counts[label] = label_counts.get(label, 0) + 1

            for schema_name, label in cluster_labels.items():
                if label_counts[label] > 1:
                    # Multiple schemas share this module name - add schema name
                    cluster_labels[schema_name] = f"{label} ({schema_name})"

            # Assign alias nodes (orange dots) to the same schema as their child table
            for node, data in graph.nodes(data=True):
                if data.get("node_type") is _AliasNode:
                    # Find the child (successor) - the table that declares the renamed FK
                    successors = list(graph.successors(node))
                    if successors and successors[0] in schema_map:
                        schema_map[node] = schema_map[successors[0]]

            # Assign collapsed nodes to their schema so they appear in the cluster
            for node, data in graph.nodes(data=True):
                if data.get("collapsed") and data.get("schema_name"):
                    schema_map[node] = data["schema_name"]

            scale = 1.2  # scaling factor for fonts and boxes
            label_props = {  # http://matplotlib.org/examples/color/named_colors.html
                None: dict(
                    shape="circle",
                    color="#FFFF0040",
                    fontcolor="yellow",
                    fontsize=round(scale * 8),
                    size=0.4 * scale,
                    fixed=False,
                ),
                _AliasNode: dict(
                    shape="circle",
                    color="#FF880080",
                    fontcolor="#FF880080",
                    fontsize=round(scale * 0),
                    size=0.05 * scale,
                    fixed=True,
                ),
                Manual: dict(
                    shape="box",
                    color="#00FF0030",
                    fontcolor="darkgreen",
                    fontsize=round(scale * 10),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Lookup: dict(
                    shape="plaintext",
                    color="#00000020",
                    fontcolor="black",
                    fontsize=round(scale * 8),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Computed: dict(
                    shape="ellipse",
                    color="#FF000020",
                    fontcolor="#7F0000A0",
                    fontsize=round(scale * 10),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Imported: dict(
                    shape="ellipse",
                    color="#00007F40",
                    fontcolor="#00007FA0",
                    fontsize=round(scale * 10),
                    size=0.4 * scale,
                    fixed=False,
                ),
                Part: dict(
                    shape="plaintext",
                    color="#00000000",
                    fontcolor="black",
                    fontsize=round(scale * 8),
                    size=0.1 * scale,
                    fixed=False,
                ),
                "collapsed": dict(
                    shape="box3d",
                    color="#80808060",
                    fontcolor="#404040",
                    fontsize=round(scale * 10),
                    size=0.5 * scale,
                    fixed=False,
                ),
            }
            # Build node_props, handling collapsed nodes specially
            node_props = {}
            for node, d in graph.nodes(data=True):
                if d.get("collapsed"):
                    node_props[node] = label_props["collapsed"]
                else:
                    node_props[node] = label_props[d["node_type"]]

            self._encapsulate_node_names(graph)
            self._encapsulate_edge_attributes(graph)
            dot = nx.drawing.nx_pydot.to_pydot(graph)
            dot.set_rankdir(direction)
            for node in dot.get_nodes():
                node.set_shape("circle")
                name = node.get_name().strip('"')
                props = node_props[name]
                node.set_fontsize(props["fontsize"])
                node.set_fontcolor(props["fontcolor"])
                node.set_shape(props["shape"])
                node.set_fontname("arial")
                node.set_fixedsize("shape" if props["fixed"] else False)
                node.set_width(props["size"])
                node.set_height(props["size"])

                # Handle collapsed nodes specially
                node_data = graph.nodes.get(f'"{name}"', {})
                if node_data.get("collapsed"):
                    table_count = node_data.get("table_count", 0)
                    label = f"({table_count} tables)" if table_count != 1 else "(1 table)"
                    node.set_label(label)
                    node.set_tooltip(f"Collapsed schema: {table_count} tables")
                else:
                    cls = self._resolve_class(name)
                    if cls is not None:
                        description = cls().describe(context=self.context).split("\n")
                        description = (
                            (
                                "-" * 30
                                if q.startswith("---")
                                else (q.replace("->", "&#8594;") if "->" in q else q.split(":")[0])
                            )
                            for q in description
                            if not q.startswith("#")
                        )
                        node.set_tooltip("&#13;".join(description))
                    # Strip module prefix from label if it matches the cluster label
                    display_name = name
                    schema_name = schema_map.get(name)
                    if schema_name and "." in name:
                        cluster_label = cluster_labels.get(schema_name)
                        if cluster_label and name.startswith(cluster_label + "."):
                            display_name = name[len(cluster_label) + 1:]
                    node.set_label("<<u>" + display_name + "</u>>" if node.get("distinguished") == "True" else display_name)
                node.set_color(props["color"])
                node.set_style("filled")

            for edge in dot.get_edges():
                # see https://graphviz.org/doc/info/attrs.html
                src = edge.get_source()
                dest = edge.get_destination()
                props = graph.get_edge_data(src, dest)
                if props is None:
                    raise DataJointError("Could not find edge with source '{}' and destination '{}'".format(src, dest))
                edge.set_color("#00000040")
                edge.set_style("solid" if props.get("primary") else "dashed")
                dest_node_type = graph.nodes[dest].get("node_type")
                master_part = dest_node_type is Part and dest.startswith(src + ".")
                edge.set_weight(3 if master_part else 1)
                edge.set_arrowhead("none")
                edge.set_penwidth(0.75 if props.get("multi") else 2)

            # Group nodes into schema clusters (always on)
            if schema_map:
                import pydot

                # Group nodes by schema
                schemas = {}
                for node in list(dot.get_nodes()):
                    name = node.get_name().strip('"')
                    schema_name = schema_map.get(name)
                    if schema_name:
                        if schema_name not in schemas:
                            schemas[schema_name] = []
                        schemas[schema_name].append(node)

                # Create clusters for each schema
                # Use Python module name if 1:1 mapping, otherwise database schema name
                for schema_name, nodes in schemas.items():
                    label = cluster_labels.get(schema_name, schema_name)
                    cluster = pydot.Cluster(
                        f"cluster_{schema_name}",
                        label=label,
                        style="dashed",
                        color="gray",
                        fontcolor="gray",
                    )
                    for node in nodes:
                        cluster.add_node(node)
                    dot.add_subgraph(cluster)

            return dot

        def make_svg(self):
            from IPython.display import SVG

            return SVG(self.make_dot().create_svg())

        def make_png(self):
            return io.BytesIO(self.make_dot().create_png())

        def make_image(self):
            if plot_active:
                return plt.imread(self.make_png())
            else:
                raise DataJointError("pyplot was not imported")

        def make_mermaid(self) -> str:
            """
            Generate Mermaid diagram syntax.

            Produces a flowchart in Mermaid syntax that can be rendered in
            Markdown documentation, GitHub, or https://mermaid.live.

            Returns
            -------
            str
                Mermaid flowchart syntax.

            Notes
            -----
            Layout direction is controlled via ``dj.config.display.diagram_direction``.
            Tables are grouped by schema using Mermaid subgraphs, with the Python
            module name shown as the group label when available.

            Examples
            --------
            >>> print(dj.Diagram(schema).make_mermaid())
            flowchart TB
                subgraph my_pipeline
                    Mouse[Mouse]:::manual
                    Session[Session]:::manual
                    Neuron([Neuron]):::computed
                end
                Mouse --> Session
                Session --> Neuron
            """
            graph = self._make_graph()
            direction = config.display.diagram_direction

            # Apply collapse logic if needed
            graph, collapsed_counts = self._apply_collapse(graph)

            # Build schema mapping for grouping
            schema_map = {}  # class_name -> schema_name
            schema_modules = {}  # schema_name -> set of module names

            for full_name in self.nodes_to_show:
                parts = full_name.replace('"', "`").split("`")
                if len(parts) >= 2:
                    schema_name = parts[1]
                    class_name = lookup_class_name(full_name, self.context) or full_name
                    schema_map[class_name] = schema_name

                    # Collect all module names for this schema
                    if schema_name not in schema_modules:
                        schema_modules[schema_name] = set()
                    cls = self._resolve_class(class_name)
                    if cls is not None and hasattr(cls, "__module__"):
                        module_name = cls.__module__.split(".")[-1]
                        schema_modules[schema_name].add(module_name)

            # Determine cluster labels: use module name if 1:1, else database schema name
            cluster_labels = {}
            for schema_name, modules in schema_modules.items():
                if len(modules) == 1:
                    cluster_labels[schema_name] = next(iter(modules))
                else:
                    cluster_labels[schema_name] = schema_name

            # Assign alias nodes to the same schema as their child table
            for node, data in graph.nodes(data=True):
                if data.get("node_type") is _AliasNode:
                    successors = list(graph.successors(node))
                    if successors and successors[0] in schema_map:
                        schema_map[node] = schema_map[successors[0]]

            lines = [f"flowchart {direction}"]

            # Define class styles matching Graphviz colors
            lines.append("    classDef manual fill:#90EE90,stroke:#006400")
            lines.append("    classDef lookup fill:#D3D3D3,stroke:#696969")
            lines.append("    classDef computed fill:#FFB6C1,stroke:#8B0000")
            lines.append("    classDef imported fill:#ADD8E6,stroke:#00008B")
            lines.append("    classDef part fill:#FFFFFF,stroke:#000000")
            lines.append("    classDef collapsed fill:#808080,stroke:#404040")
            lines.append("")

            # Shape mapping: Manual=box, Computed/Imported=stadium, Lookup/Part=box
            shape_map = {
                Manual: ("[", "]"),  # box
                Lookup: ("[", "]"),  # box
                Computed: ("([", "])"),  # stadium/pill
                Imported: ("([", "])"),  # stadium/pill
                Part: ("[", "]"),  # box
                _AliasNode: ("((", "))"),  # circle
                None: ("((", "))"),  # circle
            }

            tier_class = {
                Manual: "manual",
                Lookup: "lookup",
                Computed: "computed",
                Imported: "imported",
                Part: "part",
                _AliasNode: "",
                None: "",
            }

            # Group nodes by schema into subgraphs (including collapsed nodes)
            schemas = {}
            for node, data in graph.nodes(data=True):
                if data.get("collapsed"):
                    # Collapsed nodes use their schema_name attribute
                    schema_name = data.get("schema_name")
                else:
                    schema_name = schema_map.get(node)
                if schema_name:
                    if schema_name not in schemas:
                        schemas[schema_name] = []
                    schemas[schema_name].append((node, data))

            # Add nodes grouped by schema subgraphs
            for schema_name, nodes in schemas.items():
                label = cluster_labels.get(schema_name, schema_name)
                lines.append(f"    subgraph {label}")
                for node, data in nodes:
                    safe_id = node.replace(".", "_").replace(" ", "_")
                    if data.get("collapsed"):
                        # Collapsed node - show only table count
                        table_count = data.get("table_count", 0)
                        count_text = f"{table_count} tables" if table_count != 1 else "1 table"
                        lines.append(f'        {safe_id}[["({count_text})"]]:::collapsed')
                    else:
                        # Regular node
                        tier = data.get("node_type")
                        left, right = shape_map.get(tier, ("[", "]"))
                        cls = tier_class.get(tier, "")
                        # Strip module prefix from display name if it matches the cluster label
                        display_name = node
                        if "." in node and node.startswith(label + "."):
                            display_name = node[len(label) + 1:]
                        class_suffix = f":::{cls}" if cls else ""
                        lines.append(f"        {safe_id}{left}{display_name}{right}{class_suffix}")
                lines.append("    end")

            lines.append("")

            # Add edges
            for src, dest, data in graph.edges(data=True):
                safe_src = src.replace(".", "_").replace(" ", "_")
                safe_dest = dest.replace(".", "_").replace(" ", "_")
                # Solid arrow for primary FK, dotted for non-primary
                style = "-->" if data.get("primary") else "-.->"
                lines.append(f"    {safe_src} {style} {safe_dest}")

            return "\n".join(lines)

        def _repr_svg_(self):
            return self.make_svg()._repr_svg_()

        def draw(self):
            if plot_active:
                plt.imshow(self.make_image())
                plt.gca().axis("off")
                plt.show()
            else:
                raise DataJointError("pyplot was not imported")

        def save(self, filename: str, format: str | None = None) -> None:
            """
            Save diagram to file.

            Parameters
            ----------
            filename : str
                Output filename.
            format : str, optional
                File format (``'png'``, ``'svg'``, or ``'mermaid'``).
                Inferred from extension if None.

            Raises
            ------
            DataJointError
                If format is unsupported.

            Notes
            -----
            Layout direction is controlled via ``dj.config.display.diagram_direction``.
            Tables are grouped by schema, with the Python module name shown as the
            group label when available.
            """
            if format is None:
                if filename.lower().endswith(".png"):
                    format = "png"
                elif filename.lower().endswith(".svg"):
                    format = "svg"
                elif filename.lower().endswith((".mmd", ".mermaid")):
                    format = "mermaid"
            if format is None:
                raise DataJointError("Could not infer format from filename. Specify format explicitly.")
            if format.lower() == "png":
                with open(filename, "wb") as f:
                    f.write(self.make_png().getbuffer().tobytes())
            elif format.lower() == "svg":
                with open(filename, "w") as f:
                    f.write(self.make_svg().data)
            elif format.lower() == "mermaid":
                with open(filename, "w") as f:
                    f.write(self.make_mermaid())
            else:
                raise DataJointError("Unsupported file format")

        @staticmethod
        def _layout(graph, **kwargs):
            return pydot_layout(graph, prog="dot", **kwargs)
