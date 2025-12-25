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
| Phase 3: User-Defined AttributeTypes | 🔲 Pending | AttachType/FilepathType pending |
| Phase 4: Insert and Fetch Integration | ✅ Complete | Type chain encoding/decoding |
| Phase 5: Garbage Collection | 🔲 Pending | |
| Phase 6: Migration Utilities | 🔲 Pending | |
| Phase 7: Documentation and Testing | 🔲 Pending | |

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

---

## Phase 3: User-Defined AttributeTypes

**Status**: Partially complete

### 3.1 XBlobType ✅
Implemented as shown above. Composes with `<content>`.

### 3.2 AttachType and XAttachType 🔲

```python
@register_type
class AttachType(AttributeType):
    """Internal file attachment stored in database."""
    type_name = "attach"
    dtype = "longblob"

    def encode(self, filepath, *, key=None) -> bytes:
        path = Path(filepath)
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        filename, contents = stored.split(b"\0", 1)
        # Write to download_path and return path
        ...

@register_type
class XAttachType(AttributeType):
    """External file attachment using content-addressed storage."""
    type_name = "xattach"
    dtype = "<content>"
    # Similar to AttachType but composes with content storage
```

### 3.3 FilepathType 🔲

```python
@register_type
class FilepathType(AttributeType):
    """Portable relative path reference within configured stores."""
    type_name = "filepath"
    dtype = "json"

    def encode(self, relative_path: str, *, key=None, store_name=None) -> dict:
        """Register reference to file in store."""
        return {'path': relative_path, 'store': store_name}

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        """Return ObjectRef for lazy access."""
        return ObjectRef(store=stored['store'], path=stored['path'])
```

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

## Phase 5: Garbage Collection 🔲

**Status**: Pending

### Design (updated for function-based approach):

Since we don't have a registry table, GC works by scanning:

```python
def scan_content_references(schemas: list) -> set[tuple[str, str]]:
    """
    Scan all schemas for content references.

    Returns:
        Set of (content_hash, store) tuples that are referenced
    """
    referenced = set()
    for schema in schemas:
        for table in schema.tables:
            for attr in table.heading.attributes:
                if uses_content_storage(attr):
                    # Fetch all JSON metadata from this column
                    for row in table.fetch(attr.name):
                        if isinstance(row, dict) and 'hash' in row:
                            referenced.add((row['hash'], row.get('store')))
    return referenced

def list_stored_content(store_name: str) -> set[str]:
    """List all content hashes in a store by scanning _content/ directory."""
    ...

def garbage_collect(schemas: list, store_name: str, dry_run=True) -> dict:
    """
    Remove unreferenced content from storage.

    Returns:
        Stats: {'scanned': N, 'orphaned': M, 'deleted': K, 'bytes_freed': B}
    """
    referenced = scan_content_references(schemas)
    stored = list_stored_content(store_name)
    orphaned = stored - {h for h, s in referenced if s == store_name}

    if not dry_run:
        for content_hash in orphaned:
            delete_content(content_hash, store_name)

    return {'orphaned': len(orphaned), ...}
```

---

## Phase 6: Migration Utilities 🔲

**Status**: Pending

### Key migrations needed:
1. Legacy `~external_{store}` tables → content-addressed storage
2. UUID-based external references → hash-based JSON metadata
3. Legacy `filepath@store` → new `<filepath@store>` with ObjectRef

---

## Phase 7: Documentation and Testing 🔲

**Status**: Pending

### Test files to create:
- `tests/test_content_storage.py` - Content-addressed storage functions
- `tests/test_xblob.py` - XBlobType roundtrip
- `tests/test_type_composition.py` - Type chain encoding/decoding
- `tests/test_gc.py` - Garbage collection

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
| `src/datajoint/table.py` | ✅ | Type chain encoding on insert |
| `src/datajoint/fetch.py` | ✅ | Type chain decoding on fetch |
| `src/datajoint/blob.py` | ✅ | Removed bypass_serialization |
| `src/datajoint/gc.py` | 🔲 | Garbage collection (to be created) |
| `src/datajoint/migrate.py` | 🔲 | Migration utilities |

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
<object>   → json     (path-addressed, for Zarr/HDF5/folders)
<content>  → json     (content-addressed with deduplication)
<xblob>    → <content> → json (external serialized with dedup)
```

**Type Composition Example:**
```
<xblob> → <content> → json (in DB)

Insert: Python object → blob.pack() → put_content() → JSON metadata
Fetch:  JSON metadata → get_content() → blob.unpack() → Python object
```
