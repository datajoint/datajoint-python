# DataJoint Storage Types Redesign - Implementation Plan

## Executive Summary

This plan describes the implementation of a three-layer type architecture for DataJoint, building on the existing `AttributeType` infrastructure. The key goals are:

1. Establish a clean three-layer type hierarchy (native DB types, core DataJoint types, AttributeTypes)
2. Implement content-addressed storage with deduplication
3. Provide composable, user-friendly types (`<xblob>`, `<xattach>`, `<filepath@store>`)
4. Enable project-wide garbage collection
5. Maintain backward compatibility with existing schemas

---

## Implementation Status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Core Type System | ✅ Complete | CORE_TYPES dict, type chain resolution |
| Phase 2: Content-Addressed Storage | ✅ Complete | Function-based, no registry table |
| Phase 2b: Path-Addressed Storage | ✅ Complete | ObjectType for files/folders |
| Phase 3: User-Defined AttributeTypes | ✅ Complete | AttachType, XAttachType, FilepathType |
| Phase 4: Insert and Fetch Integration | ✅ Complete | Type chain encoding/decoding |
| Phase 5: Garbage Collection | ✅ Complete | gc.py with scan/collect functions |
| Phase 6: Documentation and Testing | ✅ Complete | Test files for all new types |

---

## Phase 1: Core Type System Foundation ✅

**Status**: Complete

### Implemented in `src/datajoint/declare.py`:

```python
CORE_TYPES = {
    # Numeric types (aliased to native SQL)
    "float32": (r"float32$", "float"),
    "float64": (r"float64$", "double"),
    "int64": (r"int64$", "bigint"),
    "uint64": (r"uint64$", "bigint unsigned"),
    "int32": (r"int32$", "int"),
    "uint32": (r"uint32$", "int unsigned"),
    "int16": (r"int16$", "smallint"),
    "uint16": (r"uint16$", "smallint unsigned"),
    "int8": (r"int8$", "tinyint"),
    "uint8": (r"uint8$", "tinyint unsigned"),
    "bool": (r"bool$", "tinyint"),
    # UUID (stored as binary)
    "uuid": (r"uuid$", "binary(16)"),
    # JSON
    "json": (r"json$", None),
    # Binary (blob maps to longblob)
    "blob": (r"blob$", "longblob"),
    # Temporal
    "date": (r"date$", None),
    "datetime": (r"datetime$", None),
    # String types (with parameters)
    "char": (r"char\s*\(\d+\)$", None),
    "varchar": (r"varchar\s*\(\d+\)$", None),
    # Enumeration
    "enum": (r"enum\s*\(.+\)$", None),
}
```

### Key changes:
- Removed `SERIALIZED_TYPES`, `BINARY_TYPES`, `EXTERNAL_TYPES`
- Core types are recorded in field comments with `:type:` syntax
- Non-standard native types pass through with warning
- `parse_type_spec()` handles `<type@store>` syntax
- `resolve_dtype()` returns `(final_dtype, type_chain, store_name)` tuple

---

## Phase 2: Content-Addressed Storage ✅

**Status**: Complete (simplified design)

### Design Decision: Functions vs Class

The original plan proposed a `ContentRegistry` class with a database table. We implemented a simpler, stateless approach using functions in `content_registry.py`:

**Why functions instead of a registry table:**
1. **Simpler** - No additional database table to manage
2. **Decoupled** - Content storage is independent of any schema
3. **GC by scanning** - Garbage collection scans tables for references rather than maintaining reference counts
4. **Less state** - No synchronization issues between registry and actual storage

### Implemented in `src/datajoint/content_registry.py`:

```python
def compute_content_hash(data: bytes) -> str:
    """Compute SHA256 hash of content."""
    return hashlib.sha256(data).hexdigest()

def build_content_path(content_hash: str) -> str:
    """Build path: _content/{hash[:2]}/{hash[2:4]}/{hash}"""
    return f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"

def put_content(data: bytes, store_name: str | None = None) -> dict[str, Any]:
    """Store content with deduplication. Returns {hash, store, size}."""
    ...

def get_content(content_hash: str, store_name: str | None = None) -> bytes:
    """Retrieve content by hash with verification."""
    ...

def content_exists(content_hash: str, store_name: str | None = None) -> bool:
    """Check if content exists."""
    ...

def delete_content(content_hash: str, store_name: str | None = None) -> bool:
    """Delete content (use with caution - verify no references first)."""
    ...
```

### Implemented AttributeTypes in `src/datajoint/attribute_type.py`:

```python
class ContentType(AttributeType):
    """Content-addressed storage. Stores bytes, returns JSON metadata."""
    type_name = "content"
    dtype = "json"

    def encode(self, value: bytes, *, key=None, store_name=None) -> dict:
        return put_content(value, store_name=store_name)

    def decode(self, stored: dict, *, key=None) -> bytes:
        return get_content(stored["hash"], store_name=stored.get("store"))


class XBlobType(AttributeType):
    """External serialized blob using content-addressed storage."""
    type_name = "xblob"
    dtype = "<content>"  # Composition

    def encode(self, value, *, key=None, store_name=None) -> bytes:
        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key=None) -> Any:
        return blob.unpack(stored, squeeze=False)
```

---

## Phase 2b: Path-Addressed Storage (ObjectType) ✅

**Status**: Complete

### Design: Path vs Content Addressing

| Aspect | `<content>` | `<object>` |
|--------|-------------|------------|
| Addressing | Content-hash (SHA256) | Path (from primary key) |
| Path Format | `_content/{hash[:2]}/{hash[2:4]}/{hash}` | `{schema}/{table}/objects/{pk}/{field}_{token}.ext` |
| Deduplication | Yes (same content = same hash) | No (each row has unique path) |
| Deletion | GC when unreferenced | Deleted with row |
| Use case | Serialized blobs, attachments | Zarr, HDF5, folders |

### Implemented in `src/datajoint/builtin_types.py`:

```python
@register_type
class ObjectType(AttributeType):
    """Path-addressed storage for files and folders."""
    type_name = "object"
    dtype = "json"

    def encode(self, value, *, key=None, store_name=None) -> dict:
        # value can be bytes, str path, or Path
        # key contains _schema, _table, _field for path construction
        path, token = build_object_path(schema, table, field, primary_key, ext)
        backend.put_buffer(content, path)  # or put_folder for directories
        return {
            "path": path,
            "store": store_name,
            "size": size,
            "ext": ext,
            "is_dir": is_dir,
            "timestamp": timestamp.isoformat(),
        }

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        # Returns lazy handle for fsspec-based access
        return ObjectRef.from_json(stored, backend=backend)
```

### ObjectRef Features:
- `ref.path` - Storage path
- `ref.read()` - Read file content
- `ref.open()` - Open as file handle
- `ref.fsmap` - For `zarr.open(ref.fsmap)`
- `ref.download(dest)` - Download to local path
- `ref.listdir()` / `ref.walk()` - For directories

### Staged Insert for Object Types

For large objects like Zarr arrays, `staged_insert.py` provides direct writes to storage:

```python
with table.staged_insert1 as staged:
    # 1. Set primary key first (required for path construction)
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # 2. Get storage handle and write directly
    z = zarr.open(staged.store('raw_data', '.zarr'), mode='w')
    z[:] = large_array

    # 3. On exit: metadata computed, record inserted
```

**Flow comparison:**

| Normal Insert | Staged Insert |
|--------------|---------------|
| `ObjectType.encode()` uploads content | Direct writes via `staged.store()` |
| Single operation | Two-phase: write then finalize |
| Good for files/folders | Ideal for Zarr, HDF5, streaming |

Both produce the same JSON metadata format compatible with `ObjectRef.from_json()`.

**Key methods:**
- `staged.store(field, ext)` - Returns `FSMap` for Zarr/xarray
- `staged.open(field, ext)` - Returns file handle for binary writes
- `staged.fs` - Raw fsspec filesystem access

---

## Phase 3: User-Defined AttributeTypes ✅

**Status**: Complete

All built-in AttributeTypes are implemented in `src/datajoint/builtin_types.py`.

### 3.1 XBlobType ✅
External serialized blobs using content-addressed storage. Composes with `<content>`.

### 3.2 AttachType ✅

```python
@register_type
class AttachType(AttributeType):
    """Internal file attachment stored in database."""
    type_name = "attach"
    dtype = "longblob"

    def encode(self, filepath, *, key=None, store_name=None) -> bytes:
        # Returns: filename (UTF-8) + null byte + contents
        return path.name.encode("utf-8") + b"\x00" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        # Extracts to download_path, returns local path
        ...
```

### 3.3 XAttachType ✅

```python
@register_type
class XAttachType(AttributeType):
    """External file attachment using content-addressed storage."""
    type_name = "xattach"
    dtype = "<content>"  # Composes with ContentType
    # Same encode/decode as AttachType, but stored externally with dedup
```

### 3.4 FilepathType ✅

```python
@register_type
class FilepathType(AttributeType):
    """Reference to existing file in configured store."""
    type_name = "filepath"
    dtype = "json"

    def encode(self, relative_path: str, *, key=None, store_name=None) -> dict:
        # Verifies file exists, returns metadata
        return {'path': path, 'store': store_name, 'size': size, ...}

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        # Returns ObjectRef for lazy access
        return ObjectRef.from_json(stored, backend=backend)
```

### Type Comparison

| Type | Storage | Copies File | Dedup | Returns |
|------|---------|-------------|-------|---------|
| `<attach>` | Database | Yes | No | Local path |
| `<xattach>` | External | Yes | Yes | Local path |
| `<filepath>` | Reference | No | N/A | ObjectRef |
| `<object>` | External | Yes | No | ObjectRef |

---

## Phase 4: Insert and Fetch Integration ✅

**Status**: Complete

### Updated in `src/datajoint/table.py`:

```python
def __make_placeholder(self, name, value, ...):
    if attr.adapter:
        from .attribute_type import resolve_dtype
        attr.adapter.validate(value)
        _, type_chain, resolved_store = resolve_dtype(
            f"<{attr.adapter.type_name}>", store_name=attr.store
        )
        # Apply type chain: outermost → innermost
        for attr_type in type_chain:
            try:
                value = attr_type.encode(value, key=None, store_name=resolved_store)
            except TypeError:
                value = attr_type.encode(value, key=None)
```

### Updated in `src/datajoint/fetch.py`:

```python
def _get(connection, attr, data, squeeze, download_path):
    if attr.adapter:
        from .attribute_type import resolve_dtype
        final_dtype, type_chain, _ = resolve_dtype(f"<{attr.adapter.type_name}>")

        # Parse JSON if final storage is JSON
        if final_dtype.lower() == "json":
            data = json.loads(data)

        # Apply type chain in reverse: innermost → outermost
        for attr_type in reversed(type_chain):
            data = attr_type.decode(data, key=None)

        return data
```

---

## Phase 5: Garbage Collection ✅

**Status**: Complete

### Implemented in `src/datajoint/gc.py`:

```python
import datajoint as dj

# Scan schemas and find orphaned content
stats = dj.gc.scan(schema1, schema2, store_name='mystore')

# Remove orphaned content (dry_run=False to actually delete)
stats = dj.gc.collect(schema1, schema2, store_name='mystore', dry_run=True)

# Format statistics for display
print(dj.gc.format_stats(stats))
```

**Key functions:**
- `scan_references(*schemas, store_name=None)` - Scan tables for content hashes
- `list_stored_content(store_name=None)` - List all content in `_content/` directory
- `scan(*schemas, store_name=None)` - Find orphaned content without deleting
- `collect(*schemas, store_name=None, dry_run=True)` - Remove orphaned content
- `format_stats(stats)` - Human-readable statistics output

**GC Process:**
1. Scan all tables in provided schemas for content-type attributes
2. Extract content hashes from JSON metadata in those columns
3. Scan storage `_content/` directory for all stored hashes
4. Compute orphaned = stored - referenced
5. Optionally delete orphaned content (when `dry_run=False`)

---

## Phase 6: Documentation and Testing ✅

**Status**: Complete

### Test files created:
- `tests/test_content_storage.py` - Content-addressed storage functions
- `tests/test_type_composition.py` - Type chain encoding/decoding
- `tests/test_gc.py` - Garbage collection
- `tests/test_attribute_type.py` - AttributeType registry and DJBlobType (existing)

---

## Critical Files Summary

| File | Status | Changes |
|------|--------|---------|
| `src/datajoint/declare.py` | ✅ | CORE_TYPES, type parsing, SQL generation |
| `src/datajoint/heading.py` | ✅ | Simplified attribute properties |
| `src/datajoint/attribute_type.py` | ✅ | Base class, registry, type chain resolution |
| `src/datajoint/builtin_types.py` | ✅ | DJBlobType, ContentType, XBlobType, ObjectType |
| `src/datajoint/content_registry.py` | ✅ | Content storage functions (put, get, delete) |
| `src/datajoint/objectref.py` | ✅ | ObjectRef handle for lazy access |
| `src/datajoint/storage.py` | ✅ | StorageBackend, build_object_path |
| `src/datajoint/staged_insert.py` | ✅ | Staged insert for direct object storage writes |
| `src/datajoint/table.py` | ✅ | Type chain encoding on insert |
| `src/datajoint/fetch.py` | ✅ | Type chain decoding on fetch |
| `src/datajoint/blob.py` | ✅ | Removed bypass_serialization |
| `src/datajoint/gc.py` | ✅ | Garbage collection for content storage |
| `tests/test_content_storage.py` | ✅ | Tests for content_registry.py |
| `tests/test_type_composition.py` | ✅ | Tests for type chain encoding/decoding |
| `tests/test_gc.py` | ✅ | Tests for garbage collection |

---

## Removed/Deprecated

- `src/datajoint/attribute_adapter.py` - Deleted (hard deprecated)
- `bypass_serialization` flag in `blob.py` - Removed
- `database` field in Attribute - Removed (unused)
- `SERIALIZED_TYPES`, `BINARY_TYPES`, `EXTERNAL_TYPES` - Removed
- `is_attachment`, `is_filepath`, `is_object`, `is_external` flags - Removed

---

## Architecture Summary

```
Layer 3: AttributeTypes (user-facing)
         <djblob>, <object>, <content>, <xblob>, <attach>, <xattach>, <filepath@store>
         ↓ encode() / ↑ decode()

Layer 2: Core DataJoint Types
         float32, int64, uuid, json, blob, varchar(n), etc.
         ↓ SQL mapping

Layer 1: Native Database Types
         FLOAT, BIGINT, BINARY(16), JSON, LONGBLOB, VARCHAR(n), etc.
```

**Built-in AttributeTypes:**
```
<djblob>   → longblob (internal serialized storage)
<attach>   → longblob (internal file attachment)
<object>   → json     (path-addressed, for Zarr/HDF5/folders)
<filepath> → json     (reference to existing file in store)
<content>  → json     (content-addressed with deduplication)
<xblob>    → <content> → json (external serialized with dedup)
<xattach>  → <content> → json (external file attachment with dedup)
```

**Type Composition Example:**
```
<xblob> → <content> → json (in DB)

Insert: Python object → blob.pack() → put_content() → JSON metadata
Fetch:  JSON metadata → get_content() → blob.unpack() → Python object
```
