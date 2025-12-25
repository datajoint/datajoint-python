# Storage Types Redesign Spec

## Overview

This document defines a layered storage architecture:

1. **MySQL types**: `longblob`, `varchar`, `int`, etc.
2. **Core DataJoint types**: `object`, `content` (and their `@store` variants)
3. **AttributeTypes**: `<djblob>`, `<xblob>`, `<attach>`, etc. (built on top of core types)

## Core Types

### `object` / `object@store` - Path-Addressed Storage

**Already implemented.** OAS (Object-Augmented Schema) storage:

- Path derived from primary key: `{schema}/{table}/{pk}/{attribute}/`
- One-to-one relationship with table row
- Deleted when row is deleted
- Returns `ObjectRef` for lazy access
- Supports direct writes (Zarr, HDF5) via fsspec

```python
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    results : object          # default store
    archive : object@cold     # specific store
    """
```

### `content` / `content@store` - Content-Addressed Storage

**New core type.** Content-addressed storage with deduplication:

- Path derived from content hash: `_content/{hash[:2]}/{hash[2:4]}/{hash}/`
- Many-to-one: multiple rows can reference same content
- Reference counted for garbage collection
- Deduplication: identical content stored once

```
store_root/
├── {schema}/{table}/{pk}/     # object storage (path-addressed)
│   └── {attribute}/
│
└── _content/                   # content storage (content-addressed)
    └── {hash[:2]}/{hash[2:4]}/{hash}/
```

#### Content Type Behavior

The `content` core type:
- Accepts `bytes` on insert
- Computes SHA256 hash of the content
- Stores in `_content/{hash}/` if not already present (deduplication)
- Returns `bytes` on fetch (transparent retrieval)
- Registers in `ContentRegistry` for GC tracking

```python
# Core type behavior (built-in, not an AttributeType)
class ContentType:
    """Core content-addressed storage type."""

    def store(self, data: bytes, store_backend) -> str:
        """Store content, return hash."""
        content_hash = hashlib.sha256(data).hexdigest()
        path = f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"

        if not store_backend.exists(path):
            store_backend.put(path, data)
            ContentRegistry().insert1({
                'content_hash': content_hash,
                'store': store_backend.name,
                'size': len(data)
            })

        return content_hash

    def retrieve(self, content_hash: str, store_backend) -> bytes:
        """Retrieve content by hash."""
        path = f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"
        return store_backend.get(path)
```

#### Database Column

The `content` type stores a `char(64)` hash in the database:

```sql
-- content column
features CHAR(64) NOT NULL  -- SHA256 hex hash
```

## AttributeTypes (Built on Core Types)

### `<djblob>` - Internal Serialized Blob

Serialized Python object stored in database.

```python
@dj.register_type
class DJBlobType(AttributeType):
    type_name = "djblob"
    dtype = "longblob"  # MySQL type

    def encode(self, value, *, key=None) -> bytes:
        from . import blob
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        from . import blob
        return blob.unpack(stored)
```

### `<xblob>` / `<xblob@store>` - External Serialized Blob

Serialized Python object stored in content-addressed storage.

```python
@dj.register_type
class XBlobType(AttributeType):
    type_name = "xblob"
    dtype = "content"  # Core type - uses default store
    # dtype = "content@store" for specific store

    def encode(self, value, *, key=None) -> bytes:
        from . import blob
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        from . import blob
        return blob.unpack(stored)
```

Usage:
```python
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    small_result : <djblob>        # internal (in database)
    large_result : <xblob>         # external (default store)
    archive_result : <xblob@cold>  # external (specific store)
    """
```

### `<attach>` - Internal File Attachment

File stored in database with filename preserved.

```python
@dj.register_type
class AttachType(AttributeType):
    type_name = "attach"
    dtype = "longblob"

    def encode(self, filepath, *, key=None) -> bytes:
        path = Path(filepath)
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        filename, contents = stored.split(b"\0", 1)
        filename = filename.decode()
        download_path = Path(dj.config['download_path']) / filename
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_bytes(contents)
        return str(download_path)
```

### `<xattach>` / `<xattach@store>` - External File Attachment

File stored in content-addressed storage with filename preserved.

```python
@dj.register_type
class XAttachType(AttributeType):
    type_name = "xattach"
    dtype = "content"  # Core type

    def encode(self, filepath, *, key=None) -> bytes:
        path = Path(filepath)
        # Include filename in stored data
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        filename, contents = stored.split(b"\0", 1)
        filename = filename.decode()
        download_path = Path(dj.config['download_path']) / filename
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_bytes(contents)
        return str(download_path)
```

Usage:
```python
class Attachments(dj.Manual):
    definition = """
    attachment_id : int
    ---
    config : <attach>           # internal (small file in DB)
    data_file : <xattach>       # external (default store)
    archive : <xattach@cold>    # external (specific store)
    """
```

## Type Layering Summary

```
┌─────────────────────────────────────────────────────────────┐
│                     AttributeTypes                          │
│  <djblob>   <xblob>   <attach>   <xattach>   <custom>      │
├─────────────────────────────────────────────────────────────┤
│                    Core DataJoint Types                     │
│     longblob        content        object                   │
│                   content@store   object@store              │
├─────────────────────────────────────────────────────────────┤
│                      MySQL Types                            │
│  LONGBLOB    CHAR(64)    JSON    VARCHAR    INT    etc.    │
└─────────────────────────────────────────────────────────────┘
```

## Storage Comparison

| AttributeType | Core Type | Storage Location | Dedup | Returns |
|---------------|-----------|------------------|-------|---------|
| `<djblob>` | `longblob` | Database | No | Python object |
| `<xblob>` | `content` | `_content/{hash}/` | Yes | Python object |
| `<xblob@store>` | `content@store` | `_content/{hash}/` | Yes | Python object |
| `<attach>` | `longblob` | Database | No | Local file path |
| `<xattach>` | `content` | `_content/{hash}/` | Yes | Local file path |
| `<xattach@store>` | `content@store` | `_content/{hash}/` | Yes | Local file path |
| — | `object` | `{schema}/{table}/{pk}/` | No | ObjectRef |
| — | `object@store` | `{schema}/{table}/{pk}/` | No | ObjectRef |

## Reference Counting for Content Type

The `ContentRegistry` table tracks content-addressed objects:

```python
class ContentRegistry:
    definition = """
    # Content-addressed object registry
    content_hash : char(64)          # SHA256 hex
    ---
    store        : varchar(64)       # Store name
    size         : bigint unsigned   # Size in bytes
    created      : timestamp DEFAULT CURRENT_TIMESTAMP
    """
```

Garbage collection finds orphaned content:

```python
def garbage_collect(schema):
    """Remove content not referenced by any table."""
    # Get all registered hashes
    registered = set(ContentRegistry().fetch('content_hash', 'store'))

    # Get all referenced hashes from tables with content-type columns
    referenced = set()
    for table in schema.tables:
        for attr in table.heading.attributes:
            if attr.type in ('content', 'content@...'):
                hashes = table.fetch(attr.name)
                referenced.update((h, attr.store) for h in hashes)

    # Delete orphaned content
    for content_hash, store in (registered - referenced):
        store_backend = get_store(store)
        store_backend.delete(content_path(content_hash))
        (ContentRegistry() & {'content_hash': content_hash}).delete()
```

## Key Design Decisions

1. **Layered architecture**: Core types (`content`, `object`) separate from AttributeTypes
2. **Content type**: New core type for content-addressed, deduplicated storage
3. **Naming convention**:
   - `<djblob>` = internal serialized (database)
   - `<xblob>` = external serialized (content-addressed)
   - `<attach>` = internal file
   - `<xattach>` = external file
4. **Transparent access**: AttributeTypes return Python objects or file paths, not references
5. **Lazy access for objects**: Only `object`/`object@store` returns ObjectRef

## Migration from Legacy Types

| Legacy | New Equivalent |
|--------|----------------|
| `longblob` (auto-serialized) | `<djblob>` |
| `blob@store` | `<xblob@store>` |
| `attach` | `<attach>` |
| `attach@store` | `<xattach@store>` |
| `filepath@store` | Deprecated (use `object@store` or `<xattach@store>`) |

## Open Questions

1. Should `content` without `@store` use a default store, or require explicit store?
2. Should we support `<xblob>` without `@store` syntax (implying default store)?
3. Should `filepath@store` be kept for backward compat or fully deprecated?
