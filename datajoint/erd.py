import networkx as nx
import numpy as np
from . import schema


def erd(source):
    """
    :param source:  can be a connection, schema, a table, or a collection of such things
    """
    import matplotlib.pyplot as plt
    graph = source.connection.dependencies
    graph.load()

    # get all nodes to plot
    if isinstance(source, schema):
        pass
    else:
        raise NotImplementedError('Only schemas can be plotted at this time')

    # rename nodes to class names
    mapping = {k: v[0] for k, v in schema.table2class.items()}
    graph = nx.relabel_nodes(graph, mapping)

    # order by depths
    pos = {node: [np.random.ranf(), 0] for node in graph.nodes()}
    for root in [node for node in graph.nodes() if not graph.in_edges(node)]:
        for node, depth in nx.shortest_path_length(graph, root).items():
            if depth > pos[node][1]:
                pos[node][1] = depth
    pos = nx.fruchterman_reingold_layout(graph, pos=pos, iterations=10)
    nx.draw_networkx(graph, pos=pos)
    plt.gca().invert_yaxis()
    plt.axis('off')
    return graph
