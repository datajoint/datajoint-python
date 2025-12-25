# Storage Types Redesign Spec

## Overview

This document proposes a redesign of DataJoint's storage types (`blob`, `attach`, `filepath`, `object`) as a coherent system built on the `AttributeType` base class.

## Current State Analysis

### Existing Types

| Type | DB Column | Storage | Semantics |
|------|-----------|---------|-----------|
| `longblob` | LONGBLOB | Internal | Raw bytes |
| `blob@store` | binary(16) | External | Raw bytes via UUID |
| `attach` | LONGBLOB | Internal | `filename\0contents` |
| `attach@store` | binary(16) | External | File via UUID |
| `filepath@store` | binary(16) | External | Path-addressed file reference |
| `object` | JSON | External | Managed file/folder with ObjectRef |

### Problems with Current Design

1. **Scattered implementation**: Logic split across `declare.py`, `table.py`, `fetch.py`, `external.py`
2. **Inconsistent patterns**: Some types use AttributeType, others are hardcoded
3. **Implicit behaviors**: `longblob` previously auto-serialized, now raw
4. **Overlapping semantics**: `blob@store` vs `attach@store` unclear
5. **No internal object type**: `object` always requires external store

## Proposed Architecture

### Core Concepts

1. **Storage Location** (orthogonal to type):
   - **Internal**: Data stored directly in database column
   - **External**: Data stored in external storage, UUID reference in database

2. **Content Model** (what the type represents):
   - **Binary**: Raw bytes (no interpretation)
   - **Serialized**: Python objects encoded via DJ blob format
   - **File**: Single file with filename metadata
   - **Folder**: Directory structure
   - **Reference**: Pointer to externally-managed file (path-addressed)

3. **AttributeType** handles encoding/decoding between Python values and stored representation

### Type Hierarchy

```
                    AttributeType (base)
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
   BinaryType        SerializedType     FileSystemType
   (passthrough)     (pack/unpack)           │
        │                 │           ┌──────┴──────┐
        │                 │           │             │
    longblob          <djblob>    <attach>    <filepath>
    longblob@store    <djblob@store>  <attach@store>  filepath@store
```

### Proposed Types

#### 1. Raw Binary (`longblob`, `blob`, etc.)

**Not an AttributeType** - these are primitive MySQL types.

- Store/return raw bytes without transformation
- `@store` variant stores externally with content-addressed UUID
- No encoding/decoding needed

```python
# Table definition
class RawData(dj.Manual):
    definition = """
    id : int
    ---
    data : longblob          # raw bytes in DB
    large_data : blob@store  # raw bytes externally
    """

# Usage
table.insert1({'id': 1, 'data': b'raw bytes', 'large_data': b'large raw bytes'})
row = (table & 'id=1').fetch1()
assert row['data'] == b'raw bytes'  # bytes returned
```

#### 2. Serialized Objects (`<djblob>`)

**AttributeType** with DJ blob serialization.

- Input: Any Python object (arrays, dicts, lists, etc.)
- Output: Same Python object reconstructed
- Storage: DJ blob format (mYm/dj0 protocol)

```python
@dj.register_type
class DJBlobType(AttributeType):
    type_name = "djblob"
    dtype = "longblob"  # or "longblob@store" for external

    def encode(self, value, *, key=None) -> bytes:
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        return blob.unpack(stored)
```

```python
# Table definition
class ProcessedData(dj.Manual):
    definition = """
    id : int
    ---
    result : <djblob>           # serialized in DB
    large_result : <djblob@store>  # serialized externally
    """

# Usage
table.insert1({'id': 1, 'result': {'array': np.array([1,2,3]), 'meta': 'info'}})
row = (table & 'id=1').fetch1()
assert row['result']['meta'] == 'info'  # Python dict returned
```

#### 3. File Attachments (`<attach>`)

**AttributeType** for file storage with filename preservation.

- Input: File path (string or Path)
- Output: Local file path after download
- Storage: File contents with filename metadata

```python
@dj.register_type
class AttachType(AttributeType):
    type_name = "attach"
    dtype = "longblob"  # or "longblob@store" for external

    # For internal storage
    def encode(self, filepath, *, key=None) -> bytes:
        path = Path(filepath)
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        filename, contents = stored.split(b"\0", 1)
        # Download to configured path, return local filepath
        ...
```

**Key difference from blob**: Preserves original filename, returns file path not bytes.

```python
# Table definition
class Attachments(dj.Manual):
    definition = """
    id : int
    ---
    config_file : <attach>           # small file in DB
    data_file : <attach@store>       # large file externally
    """

# Usage
table.insert1({'id': 1, 'config_file': '/path/to/config.yaml'})
row = (table & 'id=1').fetch1()
# row['config_file'] == '/downloads/config.yaml'  # local path
```

#### 4. Filepath References (`<filepath>`)

**AttributeType** for tracking externally-managed files.

- Input: File path in staging area
- Output: Local file path after sync
- Storage: Path-addressed (UUID = hash of relative path, not contents)
- Tracks `contents_hash` separately for verification

```python
@dj.register_type
class FilepathType(AttributeType):
    type_name = "filepath"
    dtype = "binary(16)"  # Always external (UUID reference)
    requires_store = True  # Must specify @store

    def encode(self, filepath, *, key=None) -> bytes:
        # Compute UUID from relative path
        # Track contents_hash separately
        ...

    def decode(self, uuid_bytes, *, key=None) -> str:
        # Sync file from remote to local stage
        # Verify contents_hash
        # Return local path
        ...
```

**Key difference from attach**:
- Path-addressed (same path = same UUID, even if contents change)
- Designed for managed file workflows where files may be updated
- Always external (no internal variant)

```python
# Table definition
class ManagedFiles(dj.Manual):
    definition = """
    id : int
    ---
    data_path : <filepath@store>
    """

# Usage - file must be in configured stage directory
table.insert1({'id': 1, 'data_path': '/stage/experiment_001/data.h5'})
row = (table & 'id=1').fetch1()
# row['data_path'] == '/local_stage/experiment_001/data.h5'
```

#### 5. Managed Objects (`<object>`)

**AttributeType** for managed file/folder storage with lazy access.

- Input: File path, folder path, or ObjectRef
- Output: ObjectRef handle (lazy - no automatic download)
- Storage: JSON metadata column
- Supports direct writes (Zarr, HDF5) via fsspec

```python
@dj.register_type
class ObjectType(AttributeType):
    type_name = "object"
    dtype = "json"
    requires_store = True  # Must specify @store

    def encode(self, value, *, key=None) -> str:
        # Upload file/folder to object storage
        # Return JSON metadata
        ...

    def decode(self, json_str, *, key=None) -> ObjectRef:
        # Return ObjectRef handle (no download)
        ...
```

```python
# Table definition
class LargeData(dj.Manual):
    definition = """
    id : int
    ---
    zarr_data : <object@store>
    """

# Usage
table.insert1({'id': 1, 'zarr_data': '/path/to/data.zarr'})
row = (table & 'id=1').fetch1()
ref = row['zarr_data']  # ObjectRef handle
ref.download('/local/path')  # Explicit download
# Or direct access via fsspec
```

### Storage Location Modifier (`@store`)

The `@store` suffix is orthogonal to the type and specifies external storage:

| Type | Without @store | With @store |
|------|---------------|-------------|
| `longblob` | Raw bytes in DB | Raw bytes in external store |
| `<djblob>` | Serialized in DB | Serialized in external store |
| `<attach>` | File in DB | File in external store |
| `<filepath>` | N/A (error) | Path reference in external store |
| `<object>` | N/A (error) | Object in external store |

Implementation:
- `@store` changes the underlying `dtype` to `binary(16)` (UUID)
- Creates FK relationship to `~external_{store}` tracking table
- AttributeType's `encode()`/`decode()` work with the external table transparently

### Extended AttributeType Interface

For types that interact with the filesystem, we extend the base interface:

```python
class FileSystemType(AttributeType):
    """Base for types that work with file paths."""

    # Standard interface
    def encode(self, value, *, key=None) -> bytes | str:
        """Convert input (path or value) to stored representation."""
        ...

    def decode(self, stored, *, key=None) -> str:
        """Convert stored representation to local file path."""
        ...

    # Extended interface for external storage
    def upload(self, filepath: Path, external: ExternalTable) -> uuid.UUID:
        """Upload file to external storage, return UUID."""
        ...

    def download(self, uuid: uuid.UUID, external: ExternalTable,
                 download_path: Path) -> Path:
        """Download from external storage to local path."""
        ...
```

### Configuration

```python
# datajoint config
dj.config['stores'] = {
    'main': {
        'protocol': 's3',
        'endpoint': 's3.amazonaws.com',
        'bucket': 'my-bucket',
        'location': 'datajoint/',
    },
    'archive': {
        'protocol': 'file',
        'location': '/mnt/archive/',
    }
}

dj.config['download_path'] = '/tmp/dj_downloads'  # For attach
dj.config['stage'] = '/data/stage'  # For filepath
```

## Migration Path

### Phase 1: Current State (Done)
- `<djblob>` AttributeType implemented
- `longblob` returns raw bytes
- Legacy `AttributeAdapter` wrapped for backward compat

### Phase 2: Attach as AttributeType
- Implement `<attach>` and `<attach@store>` as AttributeType
- Deprecate bare `attach` type (still works, emits warning)
- Move logic from table.py/fetch.py to AttachType class

### Phase 3: Filepath as AttributeType
- Implement `<filepath@store>` as AttributeType
- Deprecate `filepath@store` syntax (redirect to `<filepath@store>`)

### Phase 4: Object Type Refinement
- Already implemented as separate system
- Ensure consistent with AttributeType patterns
- Consider `<object@store>` syntax

### Phase 5: Cleanup
- Remove scattered type handling from table.py, fetch.py
- Consolidate external storage logic
- Update documentation

## Summary

| Type | Input | Output | Internal | External | Use Case |
|------|-------|--------|----------|----------|----------|
| `longblob` | bytes | bytes | ✓ | ✓ | Raw binary data |
| `<djblob>` | any | any | ✓ | ✓ | Python objects, arrays |
| `<attach>` | path | path | ✓ | ✓ | Files with filename |
| `<filepath>` | path | path | ✗ | ✓ | Managed file workflows |
| `<object>` | path/ref | ObjectRef | ✗ | ✓ | Large files, Zarr, HDF5 |

This design:
1. Makes all custom types consistent AttributeTypes
2. Separates storage location (`@store`) from encoding behavior
3. Provides clear semantics for each type
4. Enables gradual migration from current implementation
