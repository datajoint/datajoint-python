# Storage Types Redesign Spec

## Overview

This document defines a three-layer type architecture:

1. **Native database types** - Backend-specific (`FLOAT`, `TINYINT UNSIGNED`, `LONGBLOB`). Discouraged for direct use.
2. **Core DataJoint types** - Standardized across backends, scientist-friendly (`float32`, `uint8`, `bool`, `json`).
3. **AttributeTypes** - Programmatic types with `encode()`/`decode()` semantics. Composable.

```
┌───────────────────────────────────────────────────────────────────┐
│                     AttributeTypes (Layer 3)                       │
│                                                                    │
│  Built-in:  <djblob>  <object>  <content>  <filepath@s>  <xblob>  │
│  User:      <custom>  <mytype>   ...                               │
├───────────────────────────────────────────────────────────────────┤
│                 Core DataJoint Types (Layer 2)                     │
│                                                                    │
│  float32  float64  int64  uint64  int32  uint32  int16  uint16    │
│  int8  uint8  bool  uuid  json  bytes  date  datetime  text       │
│  char(n)  varchar(n)  enum(...)  decimal(n,f)                      │
├───────────────────────────────────────────────────────────────────┤
│               Native Database Types (Layer 1)                      │
│                                                                    │
│  MySQL:      TINYINT  SMALLINT  INT  BIGINT  FLOAT  DOUBLE  ...   │
│  PostgreSQL: SMALLINT INTEGER   BIGINT  REAL  DOUBLE PRECISION    │
│  (pass through with warning for non-standard types)                │
└───────────────────────────────────────────────────────────────────┘
```

**Syntax distinction:**
- Core types: `int32`, `float64`, `varchar(255)` - no brackets
- AttributeTypes: `<object>`, `<djblob>`, `<filepath@main>` - angle brackets

### OAS Storage Regions

| Region | Path Pattern | Addressing | Use Case |
|--------|--------------|------------|----------|
| Object | `{schema}/{table}/{pk}/` | Primary key | Large objects, Zarr, HDF5 |
| Content | `_content/{hash}` | Content hash | Deduplicated blobs/files |

### External References

`<filepath@store>` provides portable relative paths within configured stores with lazy ObjectRef access.
For arbitrary URLs that don't need ObjectRef semantics, use `varchar` instead.

## Core DataJoint Types (Layer 2)

Core types provide a standardized, scientist-friendly interface that works identically across
MySQL and PostgreSQL backends. Users should prefer these over native database types.

**All core types are recorded in field comments using `:type:` syntax for reconstruction.**

### Numeric Types

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `int8` | 8-bit signed | `TINYINT` | `SMALLINT` |
| `int16` | 16-bit signed | `SMALLINT` | `SMALLINT` |
| `int32` | 32-bit signed | `INT` | `INTEGER` |
| `int64` | 64-bit signed | `BIGINT` | `BIGINT` |
| `uint8` | 8-bit unsigned | `TINYINT UNSIGNED` | `SMALLINT` |
| `uint16` | 16-bit unsigned | `SMALLINT UNSIGNED` | `INTEGER` |
| `uint32` | 32-bit unsigned | `INT UNSIGNED` | `BIGINT` |
| `uint64` | 64-bit unsigned | `BIGINT UNSIGNED` | `NUMERIC(20)` |
| `float32` | 32-bit float | `FLOAT` | `REAL` |
| `float64` | 64-bit float | `DOUBLE` | `DOUBLE PRECISION` |
| `decimal(n,f)` | Fixed-point | `DECIMAL(n,f)` | `NUMERIC(n,f)` |

### String Types

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `char(n)` | Fixed-length | `CHAR(n)` | `CHAR(n)` |
| `varchar(n)` | Variable-length | `VARCHAR(n)` | `VARCHAR(n)` |
| `text` | Unlimited text | `TEXT` | `TEXT` |

**Encoding:** All strings use UTF-8 (`utf8mb4` in MySQL, `UTF8` in PostgreSQL).
See [Encoding and Collation Policy](#encoding-and-collation-policy) for details.

### Boolean

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `bool` | True/False | `TINYINT` | `BOOLEAN` |

### Date/Time Types

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `date` | Date only | `DATE` | `DATE` |
| `datetime` | Date and time | `DATETIME` | `TIMESTAMP` |

**Timezone policy:** All `datetime` values should be stored as **UTC**. Timezone conversion is a
presentation concern handled by the application layer, not the database. This ensures:
- Reproducible computations regardless of server or client timezone settings
- Simple arithmetic on temporal values (no DST ambiguity)
- Portable data across systems and regions

Use `CURRENT_TIMESTAMP` for auto-populated creation times:
```
created_at : datetime = CURRENT_TIMESTAMP
```

### Binary Types

The core `bytes` type stores raw bytes without any serialization. Use `<djblob>` AttributeType
for serialized Python objects.

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `bytes` | Raw bytes | `LONGBLOB` | `BYTEA` |

### Other Types

| Core Type | Description | MySQL | PostgreSQL |
|-----------|-------------|-------|------------|
| `json` | JSON document | `JSON` | `JSONB` |
| `uuid` | UUID | `BINARY(16)` | `UUID` |
| `enum(...)` | Enumeration | `ENUM(...)` | `CREATE TYPE ... AS ENUM` |

### Native Passthrough Types

Users may use native database types directly (e.g., `mediumint`, `tinyblob`),
but these will generate a warning about non-standard usage. Native types are not recorded
in field comments and may have portability issues across database backends.

### Type Modifiers Policy

DataJoint table definitions have their own syntax for constraints and metadata. SQL type
modifiers are **not allowed** in type specifications because they conflict with DataJoint's
declarative syntax:

| Modifier | Status | DataJoint Alternative |
|----------|--------|----------------------|
| `NOT NULL` / `NULL` | ❌ Not allowed | Position above/below `---` determines nullability |
| `DEFAULT value` | ❌ Not allowed | Use `= value` syntax after type |
| `PRIMARY KEY` | ❌ Not allowed | Position above `---` line |
| `UNIQUE` | ❌ Not allowed | Use DataJoint index syntax |
| `COMMENT 'text'` | ❌ Not allowed | Use `# comment` syntax |
| `CHARACTER SET` | ❌ Not allowed | Database-level configuration |
| `COLLATE` | ❌ Not allowed | Database-level configuration |
| `AUTO_INCREMENT` | ⚠️ Discouraged | Allowed with native types only, generates warning |
| `UNSIGNED` | ✅ Allowed | Part of type semantics (use `uint*` core types) |

**Auto-increment policy:** DataJoint discourages `AUTO_INCREMENT` / `SERIAL` because:
- Breaks reproducibility (IDs depend on insertion order)
- Makes pipelines non-deterministic
- Complicates data migration and replication
- Primary keys should be meaningful, not arbitrary

If required, use native types: `int auto_increment` or `serial` (with warning).

### Encoding and Collation Policy

Character encoding and collation are **database-level configuration**, not part of type
definitions. This ensures consistent behavior across all tables and simplifies portability.

**Configuration** (in `dj.config` or `datajoint.json`):
```json
{
    "database.charset": "utf8mb4",
    "database.collation": "utf8mb4_bin"
}
```

**Defaults:**

| Setting | MySQL | PostgreSQL |
|---------|-------|------------|
| Charset | `utf8mb4` | `UTF8` |
| Collation | `utf8mb4_bin` | `C` |

**Policy:**
- **UTF-8 required**: DataJoint validates charset is UTF-8 compatible at connection time
- **Case-sensitive by default**: Binary collation (`utf8mb4_bin` / `C`) ensures predictable comparisons
- **No per-column overrides**: `CHARACTER SET` and `COLLATE` are rejected in type definitions
- **Like timezone**: Encoding is infrastructure configuration, not part of the data model

## AttributeTypes (Layer 3)

AttributeTypes provide `encode()`/`decode()` semantics on top of core types. They are
composable and can be built-in or user-defined.

### `<object>` / `<object@store>` - Path-Addressed Storage

**Built-in AttributeType.** OAS (Object-Augmented Schema) storage:

- Path derived from primary key: `{schema}/{table}/{pk}/{attribute}/`
- One-to-one relationship with table row
- Deleted when row is deleted
- Returns `ObjectRef` for lazy access
- Supports direct writes (Zarr, HDF5) via fsspec
- **dtype**: `json` (stores path, store name, metadata)

```python
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    results : <object>          # default store
    archive : <object@cold>     # specific store
    """
```

#### Implementation

```python
class ObjectType(AttributeType):
    """Built-in AttributeType for path-addressed OAS storage."""
    type_name = "object"
    dtype = "json"

    def encode(self, value, *, key=None, store_name=None) -> dict:
        store = get_store(store_name or dj.config['stores']['default'])
        path = self._compute_path(key)  # {schema}/{table}/{pk}/{attr}/
        store.put(path, value)
        return {
            "path": path,
            "store": store_name,
            # Additional metadata (size, timestamps, etc.)
        }

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        return ObjectRef(
            store=get_store(stored["store"]),
            path=stored["path"]
        )
```

### `<content>` / `<content@store>` - Content-Addressed Storage

**Built-in AttributeType.** Content-addressed storage with deduplication:

- **Single blob only**: stores a single file or serialized object (not folders)
- **Per-project scope**: content is shared across all schemas in a project (not per-schema)
- Path derived from content hash: `_content/{hash[:2]}/{hash[2:4]}/{hash}`
- Many-to-one: multiple rows (even across schemas) can reference same content
- Reference counted for garbage collection
- Deduplication: identical content stored once across the entire project
- For folders/complex objects, use `object` type instead
- **dtype**: `json` (stores hash, store name, size, metadata)

```
store_root/
├── {schema}/{table}/{pk}/     # object storage (path-addressed by PK)
│   └── {attribute}/
│
└── _content/                   # content storage (content-addressed)
    └── {hash[:2]}/{hash[2:4]}/{hash}
```

#### Implementation

```python
class ContentType(AttributeType):
    """Built-in AttributeType for content-addressed storage."""
    type_name = "content"
    dtype = "json"

    def encode(self, data: bytes, *, key=None, store_name=None) -> dict:
        """Store content, return metadata as JSON."""
        content_hash = hashlib.sha256(data).hexdigest()
        store = get_store(store_name or dj.config['stores']['default'])
        path = f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"

        if not store.exists(path):
            store.put(path, data)
            ContentRegistry().insert1({
                'content_hash': content_hash,
                'store': store_name,
                'size': len(data)
            }, skip_duplicates=True)

        return {
            "hash": content_hash,
            "store": store_name,
            "size": len(data)
        }

    def decode(self, stored: dict, *, key=None) -> bytes:
        """Retrieve content by hash."""
        store = get_store(stored["store"])
        path = f"_content/{stored['hash'][:2]}/{stored['hash'][2:4]}/{stored['hash']}"
        return store.get(path)
```

#### Database Column

The `<content>` type stores JSON metadata:

```sql
-- content column (MySQL)
features JSON NOT NULL
-- Contains: {"hash": "abc123...", "store": "main", "size": 12345}

-- content column (PostgreSQL)
features JSONB NOT NULL
```

### `<filepath@store>` - Portable External Reference

**Built-in AttributeType.** Relative path references within configured stores:

- **Relative paths**: paths within a configured store (portable across environments)
- **Store-aware**: resolves paths against configured store backend
- Returns `ObjectRef` for lazy access via fsspec
- Stores optional checksum for verification
- **dtype**: `json` (stores path, store name, checksum, metadata)

**Key benefit**: Portability. The path is relative to the store, so pipelines can be moved
between environments (dev → prod, cloud → local) by changing store configuration without
updating data.

```python
class RawData(dj.Manual):
    definition = """
    session_id : int32
    ---
    recording : <filepath@main>      # relative path within 'main' store
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

#### When to Use `<filepath@store>` vs `varchar`

| Use Case | Recommended Type |
|----------|------------------|
| Need ObjectRef/lazy access | `<filepath@store>` |
| Need portability (relative paths) | `<filepath@store>` |
| Want checksum verification | `<filepath@store>` |
| Just storing a URL string | `varchar` |
| External URLs you don't control | `varchar` |

For arbitrary URLs (S3, HTTP, etc.) where you don't need ObjectRef semantics,
just use `varchar`. A string is simpler and more transparent.

#### Implementation

```python
class FilepathType(AttributeType):
    """Built-in AttributeType for store-relative file references."""
    type_name = "filepath"
    dtype = "json"

    def encode(self, relative_path: str, *, key=None, store_name=None,
               compute_checksum: bool = False) -> dict:
        """Register reference to file in store."""
        store = get_store(store_name)  # store_name required for filepath
        metadata = {'path': relative_path, 'store': store_name}

        if compute_checksum:
            full_path = store.resolve(relative_path)
            if store.exists(full_path):
                metadata['checksum'] = compute_file_checksum(store, full_path)
                metadata['size'] = store.size(full_path)

        return metadata

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        """Return ObjectRef for lazy access."""
        return ObjectRef(
            store=get_store(stored['store']),
            path=stored['path'],
            checksum=stored.get('checksum')  # optional verification
        )
```

#### Database Column

```sql
-- filepath column (MySQL)
recording JSON NOT NULL
-- Contains: {"path": "experiment_001/data.nwb", "store": "main", "checksum": "...", "size": ...}

-- filepath column (PostgreSQL)
recording JSONB NOT NULL
```

#### Key Differences from Legacy `filepath@store` (now `<filepath@store>`)

| Feature | Legacy | New |
|---------|--------|-----|
| Access | Copy to local stage | ObjectRef (lazy) |
| Copying | Automatic | Explicit via `ref.download()` |
| Streaming | No | Yes via `ref.open()` |
| Paths | Relative | Relative (unchanged) |
| Store param | Required (`@store`) | Required (`@store`) |

## Database Types

### `json` - Cross-Database JSON Type

JSON storage compatible across MySQL and PostgreSQL:

```sql
-- MySQL
column_name JSON NOT NULL

-- PostgreSQL (uses JSONB for better indexing)
column_name JSONB NOT NULL
```

The `json` database type:
- Used as dtype by built-in AttributeTypes (`<object>`, `<content>`, `<filepath@store>`)
- Stores arbitrary JSON-serializable data
- Automatically uses appropriate type for database backend
- Supports JSON path queries where available

## Parameterized AttributeTypes

AttributeTypes can be parameterized with `<type@param>` syntax. The parameter specifies
which store to use:

```python
class AttributeType:
    type_name: str      # Name used in <brackets> or as bare type
    dtype: str          # Database type or built-in AttributeType

    # When user writes type_name@param, resolved store becomes param
```

**Resolution examples:**
```
<xblob>        → uses <content> type   → default store
<xblob@cold>   → uses <content> type   → cold store
<djblob>       → dtype = "longblob"    → database (no store)
<object@cold>  → uses <object> type    → cold store
```

AttributeTypes can use other AttributeTypes as their dtype (composition):
- `<xblob>` uses `<content>` - adds djblob serialization on top of content-addressed storage
- `<xattach>` uses `<content>` - adds filename preservation on top of content-addressed storage

## User-Defined AttributeTypes

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

## Storage Comparison

| Type | dtype | Storage Location | Dedup | Returns |
|------|-------|------------------|-------|---------|
| `<object>` | `json` | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `<object@s>` | `json` | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `<content>` | `json` | `_content/{hash}` | Yes | bytes |
| `<content@s>` | `json` | `_content/{hash}` | Yes | bytes |
| `<filepath@s>` | `json` | Configured store (relative path) | No | ObjectRef |
| `<djblob>` | `longblob` | Database | No | Python object |
| `<xblob>` | `<content>` | `_content/{hash}` | Yes | Python object |
| `<xblob@s>` | `<content@s>` | `_content/{hash}` | Yes | Python object |
| `<attach>` | `longblob` | Database | No | Local file path |
| `<xattach>` | `<content>` | `_content/{hash}` | Yes | Local file path |
| `<xattach@s>` | `<content@s>` | `_content/{hash}` | Yes | Local file path |

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

## Built-in AttributeType Comparison

| Feature | `<object>` | `<content>` | `<filepath@store>` |
|---------|------------|-------------|---------------------|
| dtype | `json` | `json` | `json` |
| Location | OAS store | OAS store | Configured store |
| Addressing | Primary key | Content hash | Relative path |
| Path control | DataJoint | DataJoint | User |
| Deduplication | No | Yes | No |
| Structure | Files, folders, Zarr | Single blob only | Any (via fsspec) |
| Access | ObjectRef (lazy) | Transparent (bytes) | ObjectRef (lazy) |
| GC | Deleted with row | Reference counted | N/A (user managed) |
| Integrity | DataJoint managed | DataJoint managed | User managed |

**When to use each:**
- **`<object>`**: Large/complex objects where DataJoint controls organization (Zarr, HDF5)
- **`<content>`**: Deduplicated serialized data or file attachments via `<xblob>`, `<xattach>`
- **`<filepath@store>`**: Portable references to files in configured stores
- **`varchar`**: Arbitrary URLs/paths where ObjectRef semantics aren't needed

## Key Design Decisions

1. **Three-layer architecture**:
   - Layer 1: Native database types (backend-specific, discouraged)
   - Layer 2: Core DataJoint types (standardized, scientist-friendly)
   - Layer 3: AttributeTypes (encode/decode, composable)
2. **Core types are scientist-friendly**: `float32`, `uint8`, `bool` instead of `FLOAT`, `TINYINT UNSIGNED`, `TINYINT(1)`
3. **AttributeTypes use angle brackets**: `<object>`, `<djblob>`, `<filepath@store>` - distinguishes from core types
4. **AttributeTypes are composable**: `<xblob>` uses `<content>`, which uses `json`
5. **Built-in AttributeTypes use JSON dtype**: Stores metadata (path, hash, store name, etc.)
6. **Two OAS regions**: object (PK-addressed) and content (hash-addressed) within managed stores
7. **Filepath for portability**: `<filepath@store>` uses relative paths within stores for environment portability
8. **No `uri` type**: For arbitrary URLs, use `varchar`—simpler and more transparent
9. **Content type**: Single-blob, content-addressed, deduplicated storage
10. **Parameterized types**: `<type@param>` passes store parameter
11. **Naming convention**:
    - `<djblob>` = internal serialized (database)
    - `<xblob>` = external serialized (content-addressed)
    - `<attach>` = internal file (single file)
    - `<xattach>` = external file (single file)
12. **Transparent access**: AttributeTypes return Python objects or file paths
13. **Lazy access**: `<object>`, `<object@store>`, and `<filepath@store>` return ObjectRef

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
