# Custom Types

In modern scientific research, data pipelines often involve complex workflows that
generate diverse data types. From high-dimensional imaging data to machine learning
models, these data types frequently exceed the basic representations supported by
traditional relational databases. For example:

+ A lab working on neural connectivity might use graph objects to represent brain
  networks.
+ Researchers processing raw imaging data might store custom objects for pre-processing
  configurations.
+ Computational biologists might store fitted machine learning models or parameter
  objects for downstream predictions.

To handle these diverse needs, DataJoint provides the `dj.AttributeAdapter` method. It
enables researchers to store and retrieve complex, non-standard data types—like Python
objects or data structures—in a relational database while maintaining the
reproducibility, modularity, and query capabilities required for scientific workflows.

## Uses in Scientific Research

Imagine a neuroscience lab studying neural connectivity. Researchers might generate
graphs (e.g., networkx.Graph) to represent connections between brain regions, where:

+ Nodes are brain regions.
+ Edges represent connections weighted by signal strength or another metric.

Storing these graph objects in a database alongside other experimental data (e.g.,
subject metadata, imaging parameters) ensures:

1. Centralized Data Management: All experimental data and analysis results are stored
   together for easy access and querying.
2. Reproducibility: The exact graph objects used in analysis can be retrieved later for
   validation or further exploration.
3. Scalability: Graph data can be integrated into workflows for larger datasets or
   across experiments.

However, since graphs are not natively supported by relational databases, here’s where
`dj.AttributeAdapter` becomes essential. It allows researchers to define custom logic for
serializing graphs (e.g., as edge lists) and deserializing them back into Python
objects, bridging the gap between advanced data types and the database.

### Example: Storing Graphs in DataJoint

To store a networkx.Graph object in a DataJoint table, researchers can define a custom
attribute type in a datajoint table class:

```python
import datajoint as dj

class GraphAdapter(dj.AttributeAdapter):

    attribute_type = 'longblob'   # this is how the attribute will be declared

    def put(self, obj):
        # convert the nx.Graph object  into an edge list
        assert isinstance(obj, nx.Graph)
        return list(obj.edges)

    def get(self, value):
        # convert edge list back into an nx.Graph
        return nx.Graph(value)


# instantiate for use as a datajoint type
graph = GraphAdapter()


# define a table with a graph attribute
schema = dj.schema('test_graphs')


@schema
class Connectivity(dj.Manual):
    definition = """
    conn_id : int
    ---
    conn_graph = null : <graph>  # a networkx.Graph object
    """
```
