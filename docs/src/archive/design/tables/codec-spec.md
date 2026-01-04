# Codec Specification

This document specifies the DataJoint Codec API for creating custom attribute types
that extend DataJoint's native type system.

## Overview

Codecs define bidirectional conversion between Python objects and database storage.
They enable storing complex data types (graphs, models, custom formats) while
maintaining DataJoint's query capabilities.

```
┌─────────────────┐                    ┌─────────────────┐
│  Python Object  │  ──── encode ────► │  Storage Type   │
│   (e.g. Graph)  │                    │  (e.g. bytes)   │
│                 │  ◄─── decode ────  │                 │
└─────────────────┘                    └─────────────────┘
```

## Quick Start

```python
import datajoint as dj
import networkx as nx

class GraphCodec(dj.Codec):
    """Store NetworkX graphs."""

    name = "graph"  # Use as <graph> in definitions

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"  # Delegate to blob for serialization

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

# Use in table definition
@schema
class Connectivity(dj.Manual):
    definition = '''
    conn_id : int
    ---
    network : <graph>
    '''
```

## The Codec Base Class

All custom codecs inherit from `dj.Codec`:

```python
class Codec(ABC):
    """Base class for codec types."""

    name: str | None = None  # Required: unique identifier

    def get_dtype(self, is_external: bool) -> str:
        """Return the storage dtype."""
        raise NotImplementedError

    @abstractmethod
    def encode(self, value, *, key=None, store_name=None) -> Any:
        """Encode Python value for storage."""
        ...

    @abstractmethod
    def decode(self, stored, *, key=None) -> Any:
        """Decode stored value back to Python."""
        ...

    def validate(self, value) -> None:
        """Optional: validate value before encoding."""
        pass
```

## Required Components

### 1. The `name` Attribute

The `name` class attribute is a unique identifier used in table definitions with
`<name>` syntax:

```python
class MyCodec(dj.Codec):
    name = "mycodec"  # Use as <mycodec> in definitions
```

Naming conventions:
- Use lowercase with underscores: `spike_train`, `graph_embedding`
- Avoid generic names that might conflict: prefer `lab_model` over `model`
- Names must be unique across all registered codecs

### 2. The `get_dtype()` Method

Returns the underlying storage type. The `is_external` parameter indicates whether
the `@` modifier is present in the table definition:

```python
def get_dtype(self, is_external: bool) -> str:
    """
    Args:
        is_external: True if @ modifier present (e.g., <mycodec@store>)

    Returns:
        - A core type: "bytes", "json", "varchar(N)", "int32", etc.
        - Another codec: "<blob>", "<hash>", etc.

    Raises:
        DataJointError: If external storage not supported but @ is present
    """
```

Examples:

```python
# Simple: always store as bytes
def get_dtype(self, is_external: bool) -> str:
    return "bytes"

# Different behavior for internal/external
def get_dtype(self, is_external: bool) -> str:
    return "<hash>" if is_external else "bytes"

# External-only codec
def get_dtype(self, is_external: bool) -> str:
    if not is_external:
        raise DataJointError("<object> requires @ (external storage only)")
    return "json"
```

### 3. The `encode()` Method

Converts Python objects to the format expected by `get_dtype()`:

```python
def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> Any:
    """
    Args:
        value: The Python object to store
        key: Primary key values (for context-dependent encoding)
        store_name: Target store name (for external storage)

    Returns:
        Value in the format expected by get_dtype()
    """
```

### 4. The `decode()` Method

Converts stored values back to Python objects:

```python
def decode(self, stored: Any, *, key: dict | None = None) -> Any:
    """
    Args:
        stored: Data retrieved from storage
        key: Primary key values (for context-dependent decoding)

    Returns:
        The reconstructed Python object
    """
```

### 5. The `validate()` Method (Optional)

Called automatically before `encode()` during INSERT operations:

```python
def validate(self, value: Any) -> None:
    """
    Args:
        value: The value to validate

    Raises:
        TypeError: If the value has an incompatible type
        ValueError: If the value fails domain validation
    """
    if not isinstance(value, ExpectedType):
        raise TypeError(f"Expected ExpectedType, got {type(value).__name__}")
```

## Auto-Registration

Codecs automatically register when their class is defined. No decorator needed:

```python
# This codec is registered automatically when the class is defined
class MyCodec(dj.Codec):
    name = "mycodec"
    # ...
```

### Skipping Registration

For abstract base classes that shouldn't be registered:

```python
class BaseCodec(dj.Codec, register=False):
    """Abstract base - not registered."""
    name = None  # Or omit entirely

class ConcreteCodec(BaseCodec):
    name = "concrete"  # This one IS registered
    # ...
```

### Registration Timing

Codecs are registered at class definition time. Ensure your codec classes are
imported before any table definitions that use them:

```python
# myproject/codecs.py
class GraphCodec(dj.Codec):
    name = "graph"
    ...

# myproject/tables.py
import myproject.codecs  # Ensure codecs are registered

@schema
class Networks(dj.Manual):
    definition = '''
    id : int
    ---
    network : <graph>
    '''
```

## Codec Composition (Chaining)

Codecs can delegate to other codecs by returning `<codec_name>` from `get_dtype()`.
This enables layered functionality:

```python
class CompressedJsonCodec(dj.Codec):
    """Compress JSON data with zlib."""

    name = "zjson"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"  # Delegate serialization to blob codec

    def encode(self, value, *, key=None, store_name=None):
        import json, zlib
        json_bytes = json.dumps(value).encode('utf-8')
        return zlib.compress(json_bytes)

    def decode(self, stored, *, key=None):
        import json, zlib
        json_bytes = zlib.decompress(stored)
        return json.loads(json_bytes.decode('utf-8'))
```

### How Chaining Works

When DataJoint encounters `<zjson>`:

1. Calls `ZjsonCodec.get_dtype(is_external=False)` → returns `"<blob>"`
2. Calls `BlobCodec.get_dtype(is_external=False)` → returns `"bytes"`
3. Final storage type is `bytes` (LONGBLOB in MySQL)

During INSERT:
1. `ZjsonCodec.encode()` converts Python dict → compressed bytes
2. `BlobCodec.encode()` packs bytes → DJ blob format
3. Stored in database

During FETCH:
1. Read from database
2. `BlobCodec.decode()` unpacks DJ blob → compressed bytes
3. `ZjsonCodec.decode()` decompresses → Python dict

### Built-in Codec Chains

DataJoint's built-in codecs form these chains:

```
<blob>     → bytes (internal)
<blob@>    → <hash@> → json (external)

<attach>   → bytes (internal)
<attach@>  → <hash@> → json (external)

<hash@>    → json (external only)
<object@>  → json (external only)
<filepath@> → json (external only)
```

### Store Name Propagation

When using external storage (`@`), the store name propagates through the chain:

```python
# Table definition
data : <mycodec@coldstore>

# Resolution:
# 1. MyCodec.get_dtype(is_external=True) → "<blob>"
# 2. BlobCodec.get_dtype(is_external=True) → "<hash>"
# 3. HashCodec.get_dtype(is_external=True) → "json"
# 4. store_name="coldstore" passed to HashCodec.encode()
```

## Plugin System (Entry Points)

Codecs can be distributed as installable packages using Python entry points.

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
        return {
            'nodes': list(graph.nodes(data=True)),
            'edges': [(u, v, d) for u, v, d in graph.edges(data=True)],
        }

    def decode(self, stored, *, key=None):
        G = nx.Graph()
        G.add_nodes_from(stored['nodes'])
        for u, v, d in stored['edges']:
            G.add_edge(u, v, **d)
        return G
```

### Usage After Installation

```bash
pip install dj-graph-codecs
```

```python
# Codecs are automatically discovered and available
@schema
class Networks(dj.Manual):
    definition = '''
    network_id : int
    ---
    topology : <graph>
    weights : <weighted_graph>
    '''
```

### Entry Point Discovery

DataJoint loads entry points lazily when a codec is first requested:

1. Check explicit registry (codecs defined in current process)
2. Load entry points from `datajoint.codecs` group
3. Also checks legacy `datajoint.types` group for compatibility

## API Reference

### Module Functions

```python
import datajoint as dj

# List all registered codec names
dj.list_codecs()  # Returns: ['blob', 'hash', 'object', 'attach', 'filepath', ...]

# Get a codec instance by name
codec = dj.get_codec("blob")
codec = dj.get_codec("<blob>")  # Angle brackets are optional
codec = dj.get_codec("<blob@store>")  # Store parameter is stripped
```

### Internal Functions (for advanced use)

```python
from datajoint.codecs import (
    is_codec_registered,  # Check if codec exists
    unregister_codec,     # Remove codec (testing only)
    resolve_dtype,        # Resolve codec chain
    parse_type_spec,      # Parse "<name@store>" syntax
)
```

## Built-in Codecs

DataJoint provides these built-in codecs:

| Codec | Internal | External | Description |
|-------|----------|----------|-------------|
| `<blob>` | `bytes` | `<hash@>` | DataJoint serialization for Python objects |
| `<hash@>` | N/A | `json` | Content-addressed storage with MD5 deduplication |
| `<object@>` | N/A | `json` | Path-addressed storage for files/folders |
| `<attach>` | `bytes` | `<hash@>` | File attachments with filename preserved |
| `<filepath@>` | N/A | `json` | Reference to existing files in store |

## Complete Examples

### Example 1: Simple Serialization

```python
import datajoint as dj
import numpy as np

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
```

### Example 2: External Storage

```python
import datajoint as dj
import pickle

class ModelCodec(dj.Codec):
    """Store ML models with optional external storage."""

    name = "model"

    def get_dtype(self, is_external: bool) -> str:
        # Use hash-addressed storage for large models
        return "<hash>" if is_external else "<blob>"

    def encode(self, model, *, key=None, store_name=None):
        return pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)

    def decode(self, stored, *, key=None):
        return pickle.loads(stored)

    def validate(self, value):
        # Check that model has required interface
        if not hasattr(value, 'predict'):
            raise TypeError("Model must have a predict() method")
```

Usage:
```python
@schema
class Models(dj.Manual):
    definition = '''
    model_id : int
    ---
    small_model : <model>         # Internal storage
    large_model : <model@>        # External (default store)
    archive_model : <model@cold>  # External (specific store)
    '''
```

### Example 3: JSON with Schema Validation

```python
import datajoint as dj
import jsonschema

class ConfigCodec(dj.Codec):
    """Store validated JSON configuration."""

    name = "config"

    SCHEMA = {
        "type": "object",
        "properties": {
            "version": {"type": "integer", "minimum": 1},
            "settings": {"type": "object"},
        },
        "required": ["version", "settings"],
    }

    def get_dtype(self, is_external: bool) -> str:
        return "json"

    def validate(self, value):
        jsonschema.validate(value, self.SCHEMA)

    def encode(self, config, *, key=None, store_name=None):
        return config  # JSON type handles serialization

    def decode(self, stored, *, key=None):
        return stored
```

### Example 4: Context-Dependent Encoding

```python
import datajoint as dj

class VersionedDataCodec(dj.Codec):
    """Handle different encoding versions based on primary key."""

    name = "versioned"

    def get_dtype(self, is_external: bool) -> str:
        return "<blob>"

    def encode(self, value, *, key=None, store_name=None):
        version = key.get("schema_version", 1) if key else 1
        if version >= 2:
            return {"v": 2, "data": self._encode_v2(value)}
        return {"v": 1, "data": self._encode_v1(value)}

    def decode(self, stored, *, key=None):
        version = stored.get("v", 1)
        if version >= 2:
            return self._decode_v2(stored["data"])
        return self._decode_v1(stored["data"])

    def _encode_v1(self, value):
        return value

    def _decode_v1(self, data):
        return data

    def _encode_v2(self, value):
        # New encoding format
        return {"optimized": True, "payload": value}

    def _decode_v2(self, data):
        return data["payload"]
```

### Example 5: External-Only Codec

```python
import datajoint as dj
from pathlib import Path

class ZarrCodec(dj.Codec):
    """Store Zarr arrays in object storage."""

    name = "zarr"

    def get_dtype(self, is_external: bool) -> str:
        if not is_external:
            raise dj.DataJointError("<zarr> requires @ (external storage only)")
        return "<object>"  # Delegate to object storage

    def encode(self, value, *, key=None, store_name=None):
        import zarr
        import tempfile

        # If already a path, pass through
        if isinstance(value, (str, Path)):
            return str(value)

        # If zarr array, save to temp and return path
        if isinstance(value, zarr.Array):
            tmpdir = tempfile.mkdtemp()
            path = Path(tmpdir) / "data.zarr"
            zarr.save(path, value)
            return str(path)

        raise TypeError(f"Expected zarr.Array or path, got {type(value)}")

    def decode(self, stored, *, key=None):
        # ObjectCodec returns ObjectRef, use its fsmap for zarr
        import zarr
        return zarr.open(stored.fsmap, mode='r')
```

## Best Practices

### 1. Choose Appropriate Storage Types

| Data Type | Recommended `get_dtype()` |
|-----------|---------------------------|
| Python objects (dicts, arrays) | `"<blob>"` |
| Large binary data | `"<hash>"` (external) |
| Files/folders (Zarr, HDF5) | `"<object>"` (external) |
| Simple JSON-serializable | `"json"` |
| Short strings | `"varchar(N)"` |
| Numeric identifiers | `"int32"`, `"int64"` |

### 2. Handle None Values

Nullable columns may pass `None` to your codec:

```python
def encode(self, value, *, key=None, store_name=None):
    if value is None:
        return None  # Pass through for nullable columns
    return self._actual_encode(value)

def decode(self, stored, *, key=None):
    if stored is None:
        return None
    return self._actual_decode(stored)
```

### 3. Test Round-Trips

Always verify that `decode(encode(x)) == x`:

```python
def test_codec_roundtrip():
    codec = MyCodec()

    test_values = [
        {"key": "value"},
        [1, 2, 3],
        np.array([1.0, 2.0]),
    ]

    for original in test_values:
        encoded = codec.encode(original)
        decoded = codec.decode(encoded)
        assert decoded == original or np.array_equal(decoded, original)
```

### 4. Include Validation

Catch errors early with `validate()`:

```python
def validate(self, value):
    if not isinstance(value, ExpectedType):
        raise TypeError(f"Expected ExpectedType, got {type(value).__name__}")

    if not self._is_valid(value):
        raise ValueError("Value fails validation constraints")
```

### 5. Document Expected Formats

Include docstrings explaining input/output formats:

```python
class MyCodec(dj.Codec):
    """
    Store MyType objects.

    Input format (encode):
        MyType instance with attributes: x, y, z

    Storage format:
        Dict with keys: 'x', 'y', 'z'

    Output format (decode):
        MyType instance reconstructed from storage
    """
```

### 6. Consider Versioning

If your encoding format might change:

```python
def encode(self, value, *, key=None, store_name=None):
    return {
        "_version": 2,
        "_data": self._encode_v2(value),
    }

def decode(self, stored, *, key=None):
    version = stored.get("_version", 1)
    data = stored.get("_data", stored)

    if version == 1:
        return self._decode_v1(data)
    return self._decode_v2(data)
```

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `Unknown codec: <name>` | Codec not registered | Import module defining codec before table definition |
| `Codec <name> already registered` | Duplicate name | Use unique names; check for conflicts |
| `<codec> requires @` | External-only codec used without @ | Add `@` or `@store` to attribute type |
| `Circular codec reference` | Codec chain forms a loop | Check `get_dtype()` return values |

### Debugging

```python
# Check what codecs are registered
print(dj.list_codecs())

# Inspect a codec
codec = dj.get_codec("mycodec")
print(f"Name: {codec.name}")
print(f"Internal dtype: {codec.get_dtype(is_external=False)}")
print(f"External dtype: {codec.get_dtype(is_external=True)}")

# Resolve full chain
from datajoint.codecs import resolve_dtype
final_type, chain, store = resolve_dtype("<mycodec@store>")
print(f"Final storage type: {final_type}")
print(f"Codec chain: {[c.name for c in chain]}")
print(f"Store: {store}")
```
