# Custom Attribute Types

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

To handle these diverse needs, DataJoint provides the **AttributeType** system. It
enables researchers to store and retrieve complex, non-standard data types—like Python
objects or data structures—in a relational database while maintaining the
reproducibility, modularity, and query capabilities required for scientific workflows.

## Overview

Custom attribute types define bidirectional conversion between:

- **Python objects** (what your code works with)
- **Storage format** (what gets stored in the database)

```
┌─────────────────┐     encode()      ┌─────────────────┐
│  Python Object  │ ───────────────►  │  Storage Type   │
│   (e.g. Graph)  │                   │  (e.g. blob)    │
└─────────────────┘     decode()      └─────────────────┘
                    ◄───────────────
```

## Defining Custom Types

Create a custom type by subclassing `dj.AttributeType` and implementing the required
methods:

```python
import datajoint as dj
import networkx as nx

@dj.register_type
class GraphType(dj.AttributeType):
    """Custom type for storing networkx graphs."""

    # Required: unique identifier used in table definitions
    type_name = "graph"

    # Required: underlying DataJoint storage type
    dtype = "longblob"

    def encode(self, graph, *, key=None):
        """Convert graph to storable format (called on INSERT)."""
        return list(graph.edges)

    def decode(self, edges, *, key=None):
        """Convert stored data back to graph (called on FETCH)."""
        return nx.Graph(edges)
```

### Required Components

| Component | Description |
|-----------|-------------|
| `type_name` | Unique identifier used in table definitions with `<type_name>` syntax |
| `dtype` | Underlying DataJoint type for storage (e.g., `"longblob"`, `"varchar(255)"`, `"json"`) |
| `encode(value, *, key=None)` | Converts Python object to storable format |
| `decode(stored, *, key=None)` | Converts stored data back to Python object |

### Using Custom Types in Tables

Once registered, use the type in table definitions with angle brackets:

```python
@schema
class Connectivity(dj.Manual):
    definition = """
    conn_id : int
    ---
    conn_graph = null : <graph>  # Uses the GraphType we defined
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

## Type Registration

### Decorator Registration

The simplest way to register a type is with the `@dj.register_type` decorator:

```python
@dj.register_type
class MyType(dj.AttributeType):
    type_name = "my_type"
    ...
```

### Direct Registration

You can also register types explicitly:

```python
class MyType(dj.AttributeType):
    type_name = "my_type"
    ...

dj.register_type(MyType)
```

### Listing Registered Types

```python
# List all registered type names
print(dj.list_types())
```

## Validation

Add data validation by overriding the `validate()` method. It's called automatically
before `encode()` during INSERT operations:

```python
@dj.register_type
class PositiveArrayType(dj.AttributeType):
    type_name = "positive_array"
    dtype = "longblob"

    def validate(self, value):
        """Ensure all values are positive."""
        import numpy as np
        if not isinstance(value, np.ndarray):
            raise TypeError(f"Expected numpy array, got {type(value).__name__}")
        if np.any(value < 0):
            raise ValueError("Array must contain only positive values")

    def encode(self, array, *, key=None):
        return array

    def decode(self, stored, *, key=None):
        return stored
```

## Storage Types (dtype)

The `dtype` property specifies how data is stored in the database:

| dtype | Use Case | Stored Format |
|-------|----------|---------------|
| `"longblob"` | Complex Python objects, arrays | Serialized binary |
| `"blob"` | Smaller objects | Serialized binary |
| `"json"` | JSON-serializable data | JSON string |
| `"varchar(N)"` | String representations | Text |
| `"int"` | Integer identifiers | Integer |
| `"blob@store"` | Large objects in external storage | UUID reference |
| `"object"` | Files/folders in object storage | JSON metadata |
| `"<other_type>"` | Chain to another custom type | Varies |

### External Storage

For large data, use external blob storage:

```python
@dj.register_type
class LargeArrayType(dj.AttributeType):
    type_name = "large_array"
    dtype = "blob@mystore"  # Uses external store named "mystore"

    def encode(self, array, *, key=None):
        return array

    def decode(self, stored, *, key=None):
        return stored
```

## Type Chaining

Custom types can build on other custom types by referencing them in `dtype`:

```python
@dj.register_type
class CompressedGraphType(dj.AttributeType):
    type_name = "compressed_graph"
    dtype = "<graph>"  # Chain to the GraphType

    def encode(self, graph, *, key=None):
        # Compress before passing to GraphType
        return self._compress(graph)

    def decode(self, stored, *, key=None):
        # GraphType's decode already ran
        return self._decompress(stored)
```

DataJoint automatically resolves the chain to find the final storage type.

## The Key Parameter

The `key` parameter provides access to primary key values during encode/decode
operations. This is useful when the conversion depends on record context:

```python
@dj.register_type
class ContextAwareType(dj.AttributeType):
    type_name = "context_aware"
    dtype = "longblob"

    def encode(self, value, *, key=None):
        if key and key.get("version") == 2:
            return self._encode_v2(value)
        return self._encode_v1(value)

    def decode(self, stored, *, key=None):
        if key and key.get("version") == 2:
            return self._decode_v2(stored)
        return self._decode_v1(stored)
```

## Publishing Custom Types as Packages

Custom types can be distributed as installable packages using Python entry points.
This allows types to be automatically discovered when the package is installed.

### Package Structure

```
dj-graph-types/
├── pyproject.toml
└── src/
    └── dj_graph_types/
        ├── __init__.py
        └── types.py
```

### pyproject.toml

```toml
[project]
name = "dj-graph-types"
version = "1.0.0"

[project.entry-points."datajoint.types"]
graph = "dj_graph_types.types:GraphType"
weighted_graph = "dj_graph_types.types:WeightedGraphType"
```

### Type Implementation

```python
# src/dj_graph_types/types.py
import datajoint as dj
import networkx as nx

class GraphType(dj.AttributeType):
    type_name = "graph"
    dtype = "longblob"

    def encode(self, graph, *, key=None):
        return list(graph.edges)

    def decode(self, edges, *, key=None):
        return nx.Graph(edges)

class WeightedGraphType(dj.AttributeType):
    type_name = "weighted_graph"
    dtype = "longblob"

    def encode(self, graph, *, key=None):
        return [(u, v, d) for u, v, d in graph.edges(data=True)]

    def decode(self, edges, *, key=None):
        g = nx.Graph()
        g.add_weighted_edges_from(edges)
        return g
```

### Usage After Installation

```bash
pip install dj-graph-types
```

```python
# Types are automatically available after package installation
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

Here's a complete example demonstrating custom types for a neuroscience workflow:

```python
import datajoint as dj
import numpy as np

# Configure DataJoint
dj.config["database.host"] = "localhost"
dj.config["database.user"] = "root"
dj.config["database.password"] = "password"

# Define custom types
@dj.register_type
class SpikeTrainType(dj.AttributeType):
    """Efficient storage for sparse spike timing data."""
    type_name = "spike_train"
    dtype = "longblob"

    def validate(self, value):
        if not isinstance(value, np.ndarray):
            raise TypeError("Expected numpy array of spike times")
        if value.ndim != 1:
            raise ValueError("Spike train must be 1-dimensional")
        if not np.all(np.diff(value) >= 0):
            raise ValueError("Spike times must be sorted")

    def encode(self, spike_times, *, key=None):
        # Store as differences (smaller values, better compression)
        return np.diff(spike_times, prepend=0).astype(np.float32)

    def decode(self, stored, *, key=None):
        # Reconstruct original spike times
        return np.cumsum(stored).astype(np.float64)


@dj.register_type
class WaveformType(dj.AttributeType):
    """Storage for spike waveform templates with metadata."""
    type_name = "waveform"
    dtype = "longblob"

    def encode(self, waveform_dict, *, key=None):
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

## Migration from AttributeAdapter

The `AttributeAdapter` class is deprecated. Migrate to `AttributeType`:

### Before (deprecated)

```python
class GraphAdapter(dj.AttributeAdapter):
    attribute_type = "longblob"

    def put(self, obj):
        return list(obj.edges)

    def get(self, value):
        return nx.Graph(value)

# Required context-based registration
graph = GraphAdapter()
schema = dj.schema("mydb", context={"graph": graph})
```

### After (recommended)

```python
@dj.register_type
class GraphType(dj.AttributeType):
    type_name = "graph"
    dtype = "longblob"

    def encode(self, obj, *, key=None):
        return list(obj.edges)

    def decode(self, value, *, key=None):
        return nx.Graph(value)

# Global registration - no context needed
schema = dj.schema("mydb")
```

### Key Differences

| Aspect | AttributeAdapter (deprecated) | AttributeType (recommended) |
|--------|-------------------------------|----------------------------|
| Methods | `put()` / `get()` | `encode()` / `decode()` |
| Storage type | `attribute_type` | `dtype` |
| Type name | Variable name in context | `type_name` property |
| Registration | Context dict per schema | Global `@register_type` decorator |
| Validation | Manual | Built-in `validate()` method |
| Distribution | Copy adapter code | Entry point packages |
| Key access | Not available | Optional `key` parameter |

## Best Practices

1. **Choose descriptive type names**: Use lowercase with underscores (e.g., `spike_train`, `graph_embedding`)

2. **Select appropriate storage types**: Use `longblob` for complex objects, `json` for simple structures, external storage for large data

3. **Add validation**: Use `validate()` to catch data errors early

4. **Document your types**: Include docstrings explaining the expected input/output formats

5. **Handle None values**: Your encode/decode methods may receive `None` for nullable attributes

6. **Consider versioning**: If your encoding format might change, include version information

7. **Test round-trips**: Ensure `decode(encode(x)) == x` for all valid inputs

```python
def test_graph_type_roundtrip():
    g = nx.lollipop_graph(4, 2)
    t = GraphType()

    encoded = t.encode(g)
    decoded = t.decode(encoded)

    assert set(g.edges) == set(decoded.edges)
```

## Built-in Types

DataJoint includes a built-in type for explicit blob serialization:

### `<djblob>` - DataJoint Blob Serialization

The `<djblob>` type provides explicit control over DataJoint's native binary
serialization. It supports:

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
    results : <djblob>      # Explicit serialization
    raw_bytes : longblob    # Backward-compatible (auto-serialized)
    """
```

#### When to Use `<djblob>`

- **New tables**: Prefer `<djblob>` for clarity and future-proofing
- **Custom types**: Use `<djblob>` when your type chains to blob storage
- **Migration**: Existing `longblob` columns can be migrated to `<djblob>`

#### Backward Compatibility

For backward compatibility, `longblob` columns without an explicit type
still receive automatic serialization. The behavior is identical to `<djblob>`,
but using `<djblob>` makes the serialization explicit in your code.

## Schema Migration

When upgrading existing schemas to use explicit type declarations, DataJoint
provides migration utilities.

### Analyzing Blob Columns

```python
import datajoint as dj

schema = dj.schema("my_database")

# Check migration status
status = dj.migrate.check_migration_status(schema)
print(f"Blob columns: {status['total_blob_columns']}")
print(f"Already migrated: {status['migrated']}")
print(f"Pending migration: {status['pending']}")
```

### Generating Migration SQL

```python
# Preview migration (dry run)
result = dj.migrate.migrate_blob_columns(schema, dry_run=True)
for sql in result['sql_statements']:
    print(sql)
```

### Applying Migration

```python
# Apply migration
result = dj.migrate.migrate_blob_columns(schema, dry_run=False)
print(f"Migrated {result['migrated']} columns")
```

### Migration Details

The migration updates MySQL column comments to include the type declaration.
This is a **metadata-only** change - the actual blob data format is unchanged.

Before migration:
- Column: `longblob`
- Comment: `user comment`
- Behavior: Auto-serialization (implicit)

After migration:
- Column: `longblob`
- Comment: `:<djblob>:user comment`
- Behavior: Explicit serialization via `<djblob>`

### Updating Table Definitions

After database migration, update your Python table definitions for consistency:

```python
# Before
class MyTable(dj.Manual):
    definition = """
    id : int
    ---
    data : longblob  # stored data
    """

# After
class MyTable(dj.Manual):
    definition = """
    id : int
    ---
    data : <djblob>  # stored data
    """
```

Both definitions work identically after migration, but using `<djblob>` makes
the serialization explicit and documents the intended behavior.
