# Custom Codecs

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

To handle these diverse needs, DataJoint provides the **Codec** system. It
enables researchers to store and retrieve complex, non-standard data types—like Python
objects or data structures—in a relational database while maintaining the
reproducibility, modularity, and query capabilities required for scientific workflows.

## Overview

Custom codecs define bidirectional conversion between:

- **Python objects** (what your code works with)
- **Storage format** (what gets stored in the database)

```
┌─────────────────┐     encode()      ┌─────────────────┐
│  Python Object  │ ───────────────►  │  Storage Type   │
│   (e.g. Graph)  │                   │  (e.g. bytes)   │
└─────────────────┘     decode()      └─────────────────┘
                    ◄───────────────
```

## Defining Custom Codecs

Create a custom codec by subclassing `dj.Codec` and implementing the required
methods. Codecs auto-register when their class is defined:

```python
import datajoint as dj
import networkx as nx

class GraphCodec(dj.Codec):
    """Custom codec for storing networkx graphs."""

    # Required: unique identifier used in table definitions
    name = "graph"

    def get_dtype(self, is_external: bool) -> str:
        """Return the underlying storage type."""
        return "<blob>"  # Delegate to blob for serialization

    def encode(self, graph, *, key=None, store_name=None):
        """Convert graph to storable format (called on INSERT)."""
        return {
            'nodes': list(graph.nodes(data=True)),
            'edges': list(graph.edges(data=True)),
        }

    def decode(self, stored, *, key=None):
        """Convert stored data back to graph (called on FETCH)."""
        G = nx.Graph()
        G.add_nodes_from(stored['nodes'])
        G.add_edges_from(stored['edges'])
        return G
```

### Required Components

| Component | Description |
|-----------|-------------|
| `name` | Unique identifier used in table definitions with `<name>` syntax |
| `get_dtype(is_external)` | Returns underlying storage type (e.g., `"<blob>"`, `"bytes"`, `"json"`) |
| `encode(value, *, key=None, store_name=None)` | Converts Python object to storable format |
| `decode(stored, *, key=None)` | Converts stored data back to Python object |

### Using Custom Codecs in Tables

Once defined, use the codec in table definitions with angle brackets:

```python
@schema
class Connectivity(dj.Manual):
    definition = """
    conn_id : int
    ---
    conn_graph = null : <graph>  # Uses the GraphCodec we defined
    """
```

Insert and fetch work seamlessly:

```python
import networkx as nx

# Insert - encode() is called automatically
g = nx.lollipop_graph(4, 2)
Connectivity.insert1({"conn_id": 1, "conn_graph": g})

# Fetch - decode() is called automatically
result = (Connectivity & "conn_id = 1").fetch1("conn_graph")
assert isinstance(result, nx.Graph)
```

## Auto-Registration

Codecs automatically register when their class is defined. No decorator needed:

```python
# This codec is registered automatically when the class is defined
class MyCodec(dj.Codec):
    name = "mycodec"
    ...
```

### Skipping Registration

For abstract base classes that shouldn't be registered:

```python
class BaseCodec(dj.Codec, register=False):
    """Abstract base - not registered."""
    name = None

class ConcreteCodec(BaseCodec):
    name = "concrete"  # This one IS registered
    ...
```

### Listing Registered Codecs

```python
# List all registered codec names
print(dj.list_codecs())
```

## Validation

Add data validation by overriding the `validate()` method. It's called automatically
before `encode()` during INSERT operations:

```python
class PositiveArrayCodec(dj.Codec):
    name = "positive_array"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def validate(self, value):
        """Ensure all values are positive."""
        import numpy as np
        if not isinstance(value, np.ndarray):
            raise TypeError(f"Expected numpy array, got {type(value).__name__}")
        if np.any(value < 0):
            raise ValueError("Array must contain only positive values")

    def encode(self, array, *, key=None, store_name=None):
        return array

    def decode(self, stored, *, key=None):
        return stored
```

## The `get_dtype()` Method

The `get_dtype()` method specifies how data is stored. The `is_external` parameter
indicates whether the `@` modifier is present:

```python
def get_dtype(self, is_external: bool) -> str:
    """
    Args:
        is_external: True if @ modifier present (e.g., <mycodec@store>)

    Returns:
        - A core type: "bytes", "json", "varchar(N)", etc.
        - Another codec: "<blob>", "<hash>", etc.
    """
```

### Storage Type Options

| Return Value | Use Case | Database Type |
|--------------|----------|---------------|
| `"bytes"` | Raw binary data | LONGBLOB |
| `"json"` | JSON-serializable data | JSON |
| `"varchar(N)"` | String representations | VARCHAR(N) |
| `"int32"` | Integer identifiers | INT |
| `"<blob>"` | Serialized Python objects | Depends on internal/external |
| `"<hash>"` | Large objects with deduplication | JSON (external only) |
| `"<other_codec>"` | Chain to another codec | Varies |

### External Storage

For large data, use external storage with the `@` modifier:

```python
class LargeArrayCodec(dj.Codec):
    name = "large_array"

    def get_dtype(self, is_external: bool) -> str:
        # Use hash-addressed external storage for large data
        return "<hash>" if is_external else "<blob>"

    def encode(self, array, *, key=None, store_name=None):
        import pickle
        return pickle.dumps(array)

    def decode(self, stored, *, key=None):
        import pickle
        return pickle.loads(stored)
```

Usage:
```python
@schema
class Data(dj.Manual):
    definition = '''
    id : int
    ---
    small_array : <large_array>        # Internal (in database)
    big_array : <large_array@>         # External (default store)
    archive : <large_array@coldstore>  # External (specific store)
    '''
```

## Codec Chaining

Custom codecs can build on other codecs by returning `<codec_name>` from `get_dtype()`:

```python
class CompressedGraphCodec(dj.Codec):
    name = "compressed_graph"

    def get_dtype(self, is_external: bool) -> str:
        return "<graph>"  # Chain to the GraphCodec

    def encode(self, graph, *, key=None, store_name=None):
        # Compress before passing to GraphCodec
        return self._compress(graph)

    def decode(self, stored, *, key=None):
        # GraphCodec's decode already ran, decompress result
        return self._decompress(stored)
```

DataJoint automatically resolves the chain to find the final storage type.

### How Chaining Works

When DataJoint encounters `<compressed_graph>`:

1. `CompressedGraphCodec.get_dtype()` returns `"<graph>"`
2. `GraphCodec.get_dtype()` returns `"<blob>"`
3. `BlobCodec.get_dtype()` returns `"bytes"`
4. Final storage type is `bytes` (LONGBLOB in MySQL)

During INSERT, encoders run outer → inner:
1. `CompressedGraphCodec.encode()` → compressed graph
2. `GraphCodec.encode()` → edge list dict
3. `BlobCodec.encode()` → serialized bytes

During FETCH, decoders run inner → outer (reverse order).

## The Key Parameter

The `key` parameter provides access to primary key values during encode/decode
operations. This is useful when the conversion depends on record context:

```python
class ContextAwareCodec(dj.Codec):
    name = "context_aware"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def encode(self, value, *, key=None, store_name=None):
        if key and key.get("version") == 2:
            return self._encode_v2(value)
        return self._encode_v1(value)

    def decode(self, stored, *, key=None):
        if key and key.get("version") == 2:
            return self._decode_v2(stored)
        return self._decode_v1(stored)
```

## Publishing Codecs as Packages

Custom codecs can be distributed as installable packages using Python entry points.
This allows codecs to be automatically discovered when the package is installed.

### Package Structure

```
dj-graph-codecs/
├── pyproject.toml
└── src/
    └── dj_graph_codecs/
        ├── __init__.py
        └── codecs.py
```

### pyproject.toml

```toml
[project]
name = "dj-graph-codecs"
version = "1.0.0"
dependencies = ["datajoint>=2.0", "networkx"]

[project.entry-points."datajoint.codecs"]
graph = "dj_graph_codecs.codecs:GraphCodec"
weighted_graph = "dj_graph_codecs.codecs:WeightedGraphCodec"
```

### Codec Implementation

```python
# src/dj_graph_codecs/codecs.py
import datajoint as dj
import networkx as nx

class GraphCodec(dj.Codec):
    name = "graph"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def encode(self, graph, *, key=None, store_name=None):
        return {
            'nodes': list(graph.nodes(data=True)),
            'edges': list(graph.edges(data=True)),
        }

    def decode(self, stored, *, key=None):
        G = nx.Graph()
        G.add_nodes_from(stored['nodes'])
        G.add_edges_from(stored['edges'])
        return G

class WeightedGraphCodec(dj.Codec):
    name = "weighted_graph"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def encode(self, graph, *, key=None, store_name=None):
        return [(u, v, d) for u, v, d in graph.edges(data=True)]

    def decode(self, edges, *, key=None):
        g = nx.Graph()
        for u, v, d in edges:
            g.add_edge(u, v, **d)
        return g
```

### Usage After Installation

```bash
pip install dj-graph-codecs
```

```python
# Codecs are automatically available after package installation
@schema
class MyTable(dj.Manual):
    definition = """
    id : int
    ---
    network : <graph>
    weighted_network : <weighted_graph>
    """
```

## Complete Example

Here's a complete example demonstrating custom codecs for a neuroscience workflow:

```python
import datajoint as dj
import numpy as np

# Define custom codecs
class SpikeTrainCodec(dj.Codec):
    """Efficient storage for sparse spike timing data."""
    name = "spike_train"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def validate(self, value):
        if not isinstance(value, np.ndarray):
            raise TypeError("Expected numpy array of spike times")
        if value.ndim != 1:
            raise ValueError("Spike train must be 1-dimensional")
        if len(value) > 1 and not np.all(np.diff(value) >= 0):
            raise ValueError("Spike times must be sorted")

    def encode(self, spike_times, *, key=None, store_name=None):
        # Store as differences (smaller values, better compression)
        return np.diff(spike_times, prepend=0).astype(np.float32)

    def decode(self, stored, *, key=None):
        # Reconstruct original spike times
        return np.cumsum(stored).astype(np.float64)


class WaveformCodec(dj.Codec):
    """Storage for spike waveform templates with metadata."""
    name = "waveform"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def encode(self, waveform_dict, *, key=None, store_name=None):
        return {
            "data": waveform_dict["data"].astype(np.float32),
            "sampling_rate": waveform_dict["sampling_rate"],
            "channel_ids": list(waveform_dict["channel_ids"]),
        }

    def decode(self, stored, *, key=None):
        return {
            "data": stored["data"].astype(np.float64),
            "sampling_rate": stored["sampling_rate"],
            "channel_ids": np.array(stored["channel_ids"]),
        }


# Create schema and tables
schema = dj.schema("ephys_analysis")

@schema
class Unit(dj.Manual):
    definition = """
    unit_id : int
    ---
    spike_times : <spike_train>
    waveform : <waveform>
    quality : enum('good', 'mua', 'noise')
    """


# Usage
spike_times = np.array([0.1, 0.15, 0.23, 0.45, 0.67, 0.89])
waveform = {
    "data": np.random.randn(82, 4),
    "sampling_rate": 30000,
    "channel_ids": [10, 11, 12, 13],
}

Unit.insert1({
    "unit_id": 1,
    "spike_times": spike_times,
    "waveform": waveform,
    "quality": "good",
})

# Fetch - automatically decoded
result = (Unit & "unit_id = 1").fetch1()
print(f"Spike times: {result['spike_times']}")
print(f"Waveform shape: {result['waveform']['data'].shape}")
```

## Built-in Codecs

DataJoint includes several built-in codecs:

### `<blob>` - DataJoint Blob Serialization

The `<blob>` codec provides DataJoint's native binary serialization. It supports:

- NumPy arrays (compatible with MATLAB)
- Python dicts, lists, tuples, sets
- datetime objects, Decimals, UUIDs
- Nested data structures
- Optional compression

```python
@schema
class ProcessedData(dj.Manual):
    definition = """
    data_id : int
    ---
    results : <blob>        # Internal (serialized in database)
    large_results : <blob@> # External (hash-addressed storage)
    """
```

### `<hash@>` - Content-Addressed Storage

Stores raw bytes using MD5 content hashing with automatic deduplication.
External storage only.

### `<object@>` - Path-Addressed Storage

Stores files and folders at paths derived from primary keys. Ideal for
Zarr arrays, HDF5 files, and multi-file outputs. External storage only.

### `<attach>` - File Attachments

Stores files with filename preserved. Supports internal and external storage.

### `<filepath@>` - File References

References existing files in configured stores without copying.
External storage only.

## Best Practices

1. **Choose descriptive codec names**: Use lowercase with underscores (e.g., `spike_train`, `graph_embedding`)

2. **Select appropriate storage types**: Use `<blob>` for complex objects, `json` for simple structures, `<hash@>` or `<object@>` for large data

3. **Add validation**: Use `validate()` to catch data errors early

4. **Document your codecs**: Include docstrings explaining the expected input/output formats

5. **Handle None values**: Your encode/decode methods may receive `None` for nullable attributes

6. **Consider versioning**: If your encoding format might change, include version information

7. **Test round-trips**: Ensure `decode(encode(x)) == x` for all valid inputs

```python
def test_graph_codec_roundtrip():
    import networkx as nx
    g = nx.lollipop_graph(4, 2)
    codec = GraphCodec()

    encoded = codec.encode(g)
    decoded = codec.decode(encoded)

    assert set(g.edges) == set(decoded.edges)
```

## API Reference

```python
import datajoint as dj

# List all registered codecs
dj.list_codecs()

# Get a codec instance
codec = dj.get_codec("blob")
codec = dj.get_codec("<blob>")  # Angle brackets optional
codec = dj.get_codec("<blob@store>")  # Store parameter stripped
```

For the complete Codec API specification, see [Codec Specification](codec-spec.md).
