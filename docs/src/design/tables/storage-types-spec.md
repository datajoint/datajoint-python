# Storage Types Redesign Spec

## Overview

This document defines a layered storage architecture:

1. **Database types**: `longblob`, `varchar`, `int`, `json`, etc.
2. **Core DataJoint types**: `object`, `content`, `filepath`, `json` (and `@store` variants where applicable)
3. **AttributeTypes**: `<djblob>`, `<xblob>`, `<attach>`, etc. (built on top of core types)

### OAS Storage Regions

| Region | Path Pattern | Addressing | Use Case |
|--------|--------------|------------|----------|
| Object | `{schema}/{table}/{pk}/` | Primary key | Large objects, Zarr, HDF5 |
| Content | `_content/{hash}` | Content hash | Deduplicated blobs/files |

### External References

`filepath@store` provides portable relative paths within configured stores with lazy ObjectRef access.
For arbitrary URLs that don't need ObjectRef semantics, use `varchar` instead.

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

- **Single blob only**: stores a single file or serialized object (not folders)
- **Per-project scope**: content is shared across all schemas in a project (not per-schema)
- Path derived from content hash: `_content/{hash[:2]}/{hash[2:4]}/{hash}`
- Many-to-one: multiple rows (even across schemas) can reference same content
- Reference counted for garbage collection
- Deduplication: identical content stored once across the entire project
- For folders/complex objects, use `object` type instead

```
store_root/
├── {schema}/{table}/{pk}/     # object storage (path-addressed by PK)
│   └── {attribute}/
│
└── _content/                   # content storage (content-addressed)
    └── {hash[:2]}/{hash[2:4]}/{hash}
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

### `filepath@store` - Portable External Reference

**Upgraded from legacy.** Relative path references within configured stores:

- **Relative paths**: paths within a configured store (portable across environments)
- **Store-aware**: resolves paths against configured store backend
- Returns `ObjectRef` for lazy access via fsspec
- Stores optional checksum for verification

**Key benefit**: Portability. The path is relative to the store, so pipelines can be moved
between environments (dev → prod, cloud → local) by changing store configuration without
updating data.

```python
class RawData(dj.Manual):
    definition = """
    session_id : int
    ---
    recording : filepath@main      # relative path within 'main' store
    """

# Insert - user provides relative path within the store
table.insert1({
    'session_id': 1,
    'recording': 'experiment_001/data.nwb'  # relative to main store root
})

# Fetch - returns ObjectRef (lazy)
row = (table & 'session_id=1').fetch1()
ref = row['recording']           # ObjectRef
ref.download('/local/path')      # explicit download
ref.open()                       # fsspec streaming access
```

#### When to Use `filepath@store` vs `varchar`

| Use Case | Recommended Type |
|----------|------------------|
| Need ObjectRef/lazy access | `filepath@store` |
| Need portability (relative paths) | `filepath@store` |
| Want checksum verification | `filepath@store` |
| Just storing a URL string | `varchar` |
| External URLs you don't control | `varchar` |

For arbitrary URLs (S3, HTTP, etc.) where you don't need ObjectRef semantics,
just use `varchar`. A string is simpler and more transparent.

#### Filepath Type Behavior

```python
# Core type behavior
class FilepathType:
    """Core external reference type with store-relative paths."""

    def store(self, relative_path: str, store_backend, compute_checksum: bool = False) -> dict:
        """Register reference to file in store."""
        metadata = {'path': relative_path}

        if compute_checksum:
            full_path = store_backend.resolve(relative_path)
            if store_backend.exists(full_path):
                metadata['checksum'] = compute_file_checksum(store_backend, full_path)
                metadata['size'] = store_backend.size(full_path)

        return metadata

    def retrieve(self, metadata: dict, store_backend) -> ObjectRef:
        """Return ObjectRef for lazy access."""
        return ObjectRef(
            store=store_backend,
            path=metadata['path'],
            checksum=metadata.get('checksum')  # optional verification
        )
```

#### Database Column

The `filepath` type uses the `json` core type:

```sql
-- filepath column (MySQL)
recording JSON NOT NULL
-- Contains: {"path": "experiment_001/data.nwb", "checksum": "...", "size": ...}

-- filepath column (PostgreSQL)
recording JSONB NOT NULL
```

#### Key Differences from Legacy `filepath@store`

| Feature | Legacy | New |
|---------|--------|-----|
| Access | Copy to local stage | ObjectRef (lazy) |
| Copying | Automatic | Explicit via `ref.download()` |
| Streaming | No | Yes via `ref.open()` |
| Paths | Relative | Relative (unchanged) |
| Store param | Required (`@store`) | Required (`@store`) |

### `json` - Cross-Database JSON Type

**New core type.** JSON storage compatible across MySQL and PostgreSQL:

```sql
-- MySQL
column_name JSON NOT NULL

-- PostgreSQL
column_name JSONB NOT NULL
```

The `json` core type:
- Stores arbitrary JSON-serializable data
- Automatically uses appropriate type for database backend
- Supports JSON path queries where available

## Parameterized AttributeTypes

AttributeTypes can be parameterized with `<type@param>` syntax. The parameter is passed
through to the underlying dtype:

```python
class AttributeType:
    type_name: str      # Name used in <brackets>
    dtype: str          # Base underlying type

    # When user writes <type_name@param>, resolved dtype becomes:
    # f"{dtype}@{param}" if param specified, else dtype
```

**Resolution examples:**
```
<xblob>       → dtype = "content"       → default store
<xblob@cold>  → dtype = "content@cold"  → cold store
<djblob>      → dtype = "longblob"      → database
<djblob@x>    → ERROR: longblob doesn't support parameters
```

This means `<xblob>` and `<xblob@store>` share the same AttributeType class - the
parameter flows through to the core type, which validates whether it supports `@store`.

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
┌───────────────────────────────────────────────────────────────────┐
│                        AttributeTypes                              │
│  <djblob>   <xblob>   <attach>   <xattach>   <custom>             │
├───────────────────────────────────────────────────────────────────┤
│                     Core DataJoint Types                           │
│   longblob     content      object    filepath@s      json         │
│              content@s    object@s                                 │
├───────────────────────────────────────────────────────────────────┤
│                    Database Types                                  │
│   LONGBLOB     CHAR(64)     JSON      JSON/JSONB    VARCHAR  etc. │
│                           (MySQL)    (PostgreSQL)                  │
└───────────────────────────────────────────────────────────────────┘
```

## Storage Comparison

| Type | Core Type | Storage Location | Dedup | Returns |
|------|-----------|------------------|-------|---------|
| `<djblob>` | `longblob` | Database | No | Python object |
| `<xblob>` | `content` | `_content/{hash}` | Yes | Python object |
| `<xblob@s>` | `content@s` | `_content/{hash}` | Yes | Python object |
| `<attach>` | `longblob` | Database | No | Local file path |
| `<xattach>` | `content` | `_content/{hash}` | Yes | Local file path |
| `<xattach@s>` | `content@s` | `_content/{hash}` | Yes | Local file path |
| `object` | — | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `object@s` | — | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `filepath@s` | `json` | Configured store (relative path) | No | ObjectRef |

## Reference Counting for Content Type

The `ContentRegistry` is a **project-level** table that tracks content-addressed objects
across all schemas. This differs from the legacy `~external_*` tables which were per-schema.

```python
class ContentRegistry:
    """
    Project-level content registry.
    Stored in a designated database (e.g., `{project}_content`).
    """
    definition = """
    # Content-addressed object registry (project-wide)
    content_hash : char(64)          # SHA256 hex
    ---
    store        : varchar(64)       # Store name
    size         : bigint unsigned   # Size in bytes
    created      : timestamp DEFAULT CURRENT_TIMESTAMP
    """
```

Garbage collection scans **all schemas** in the project:

```python
def garbage_collect(project):
    """Remove content not referenced by any table in any schema."""
    # Get all registered hashes
    registered = set(ContentRegistry().fetch('content_hash', 'store'))

    # Get all referenced hashes from ALL schemas in the project
    referenced = set()
    for schema in project.schemas:
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

## Core Type Comparison

| Feature | `object` | `content` | `filepath@store` |
|---------|----------|-----------|------------------|
| Location | OAS store | OAS store | Configured store |
| Addressing | Primary key | Content hash | Relative path |
| Path control | DataJoint | DataJoint | User |
| Deduplication | No | Yes | No |
| Structure | Files, folders, Zarr | Single blob only | Any (via fsspec) |
| Access | ObjectRef (lazy) | Transparent (bytes) | ObjectRef (lazy) |
| GC | Deleted with row | Reference counted | N/A (user managed) |
| Integrity | DataJoint managed | DataJoint managed | User managed |

**When to use each:**
- **`object`**: Large/complex objects where DataJoint controls organization (Zarr, HDF5)
- **`content`**: Deduplicated serialized data or file attachments via `<xblob>`, `<xattach>`
- **`filepath@store`**: Portable references to files in configured stores
- **`varchar`**: Arbitrary URLs/paths where ObjectRef semantics aren't needed

## Key Design Decisions

1. **Layered architecture**: Core types (`object`, `content`, `filepath@store`, `json`) separate from AttributeTypes
2. **Two OAS regions**: object (PK-addressed) and content (hash-addressed) within managed stores
3. **Filepath for portability**: `filepath@store` uses relative paths within stores for environment portability
4. **No `uri` type**: For arbitrary URLs, use `varchar`—simpler and more transparent
5. **Content type**: Single-blob, content-addressed, deduplicated storage
6. **JSON core type**: Cross-database compatible (MySQL JSON, PostgreSQL JSONB)
7. **Parameterized types**: `<type@param>` passes parameter to underlying dtype
8. **Naming convention**:
   - `<djblob>` = internal serialized (database)
   - `<xblob>` = external serialized (content-addressed)
   - `<attach>` = internal file (single file)
   - `<xattach>` = external file (single file)
9. **Transparent access**: AttributeTypes return Python objects or file paths
10. **Lazy access**: `object`, `object@store`, and `filepath@store` return ObjectRef

## Migration from Legacy Types

| Legacy | New Equivalent |
|--------|----------------|
| `longblob` (auto-serialized) | `<djblob>` |
| `blob@store` | `<xblob@store>` |
| `attach` | `<attach>` |
| `attach@store` | `<xattach@store>` |
| `filepath@store` (copy-based) | `filepath@store` (ObjectRef-based, upgraded) |

### Migration from Legacy `~external_*` Stores

Legacy external storage used per-schema `~external_{store}` tables. Migration to the new
per-project `ContentRegistry` requires:

```python
def migrate_external_store(schema, store_name):
    """
    Migrate legacy ~external_{store} to new ContentRegistry.

    1. Read all entries from ~external_{store}
    2. For each entry:
       - Fetch content from legacy location
       - Compute SHA256 hash
       - Copy to _content/{hash}/ if not exists
       - Update table column from UUID to hash
       - Register in ContentRegistry
    3. After all schemas migrated, drop ~external_{store} tables
    """
    external_table = schema.external[store_name]

    for entry in external_table.fetch(as_dict=True):
        legacy_uuid = entry['hash']

        # Fetch content from legacy location
        content = external_table.get(legacy_uuid)

        # Compute new content hash
        content_hash = hashlib.sha256(content).hexdigest()

        # Store in new location if not exists
        new_path = f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"
        store = get_store(store_name)
        if not store.exists(new_path):
            store.put(new_path, content)

        # Register in project-wide ContentRegistry
        ContentRegistry().insert1({
            'content_hash': content_hash,
            'store': store_name,
            'size': len(content)
        }, skip_duplicates=True)

        # Update referencing tables (UUID -> hash)
        # ... update all tables that reference this UUID ...

    # After migration complete for all schemas:
    # DROP TABLE `{schema}`.`~external_{store}`
```

**Migration considerations:**
- Legacy UUIDs were based on content hash but stored as `binary(16)`
- New system uses `char(64)` SHA256 hex strings
- Migration can be done incrementally per schema
- Backward compatibility layer can read both formats during transition

## Open Questions

1. Should `content` without `@store` use a default store, or require explicit store?
2. Should we support `<xblob>` without `@store` syntax (implying default store)?
3. How long should the backward compatibility layer support legacy `~external_*` format?
