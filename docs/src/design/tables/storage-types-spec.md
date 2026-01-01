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
│  Built-in:  <blob>  <attach>  <object@>  <hash@>  <filepath@>  │
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
- AttributeTypes: `<blob>`, `<object@store>`, `<filepath@main>` - angle brackets
- The `@` character indicates external storage (object store vs database)

### OAS Storage Regions

| Region | Path Pattern | Addressing | Use Case |
|--------|--------------|------------|----------|
| Object | `{schema}/{table}/{pk}/` | Primary key | Large objects, Zarr, HDF5 |
| Hash | `_hash/{hash}` | SHA256 hash | Deduplicated blobs/files |

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

The core `bytes` type stores raw bytes without any serialization. Use `<blob>` AttributeType
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

### Storage Mode: `@` Convention

The `@` character in AttributeType syntax indicates **external storage** (object store):

- **No `@`**: Internal storage (database) - e.g., `<blob>`, `<attach>`
- **`@` present**: External storage (object store) - e.g., `<blob@>`, `<attach@store>`
- **`@` alone**: Use default store - e.g., `<blob@>`
- **`@name`**: Use named store - e.g., `<blob@cold>`

Some types support both modes (`<blob>`, `<attach>`), others are external-only (`<object@>`, `<hash@>`, `<filepath@>`).

### Type Resolution and Chaining

AttributeTypes resolve to core types through chaining. The `get_dtype(is_external)` method
returns the appropriate dtype based on storage mode:

```
Resolution at declaration time:

<blob>         → get_dtype(False) → "bytes"     → LONGBLOB/BYTEA
<blob@>        → get_dtype(True)  → "<hash>" → json → JSON/JSONB
<blob@cold>    → get_dtype(True)  → "<hash>" → json (store=cold)

<attach>       → get_dtype(False) → "bytes"     → LONGBLOB/BYTEA
<attach@>      → get_dtype(True)  → "<hash>" → json → JSON/JSONB

<object@>      → get_dtype(True)  → "json"      → JSON/JSONB
<object>       → get_dtype(False) → ERROR (external only)

<hash@>     → get_dtype(True)  → "json"      → JSON/JSONB
<filepath@s>   → get_dtype(True)  → "json"      → JSON/JSONB
```

### `<object@>` / `<object@store>` - Path-Addressed Storage

**Built-in AttributeType. External only.**

OAS (Object-Augmented Schema) storage for files and folders:

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
    results : <object@>         # default store
    archive : <object@cold>     # specific store
    """
```

#### Implementation

```python
class ObjectType(AttributeType):
    """Path-addressed OAS storage. External only."""
    type_name = "object"

    def get_dtype(self, is_external: bool) -> str:
        if not is_external:
            raise DataJointError("<object> requires @ (external storage only)")
        return "json"

    def encode(self, value, *, key=None, store_name=None) -> dict:
        store = get_store(store_name or dj.config['stores']['default'])
        path = self._compute_path(key)  # {schema}/{table}/{pk}/{attr}/
        store.put(path, value)
        return {"path": path, "store": store_name, ...}

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        return ObjectRef(store=get_store(stored["store"]), path=stored["path"])
```

### `<hash@>` / `<hash@store>` - Hash-Addressed Storage

**Built-in AttributeType. External only.**

Hash-addressed storage with deduplication:

- **Single blob only**: stores a single file or serialized object (not folders)
- **Per-project scope**: content is shared across all schemas in a project (not per-schema)
- Path derived from content hash: `_hash/{hash[:2]}/{hash[2:4]}/{hash}`
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
└── _hash/                   # content storage (hash-addressed)
    └── {hash[:2]}/{hash[2:4]}/{hash}
```

#### Implementation

```python
class HashType(AttributeType):
    """Hash-addressed storage. External only."""
    type_name = "hash"

    def get_dtype(self, is_external: bool) -> str:
        if not is_external:
            raise DataJointError("<hash> requires @ (external storage only)")
        return "json"

    def encode(self, data: bytes, *, key=None, store_name=None) -> dict:
        """Store content, return metadata as JSON."""
        hash_id = hashlib.sha256(data).hexdigest()
        store = get_store(store_name or dj.config['stores']['default'])
        path = f"_hash/{hash_id[:2]}/{hash_id[2:4]}/{hash_id}"

        if not store.exists(path):
            store.put(path, data)
            HashRegistry().insert1({
                'hash_id': hash_id,
                'store': store_name,
                'size': len(data)
            }, skip_duplicates=True)

        return {"hash": hash_id, "store": store_name, "size": len(data)}

    def decode(self, stored: dict, *, key=None) -> bytes:
        """Retrieve content by hash."""
        store = get_store(stored["store"])
        path = f"_hash/{stored['hash'][:2]}/{stored['hash'][2:4]}/{stored['hash']}"
        return store.get(path)
```

#### Database Column

The `<hash@>` type stores JSON metadata:

```sql
-- content column (MySQL)
features JSON NOT NULL
-- Contains: {"hash": "abc123...", "store": "main", "size": 12345}

-- content column (PostgreSQL)
features JSONB NOT NULL
```

### `<filepath@store>` - Portable External Reference

**Built-in AttributeType. External only (store required).**

Relative path references within configured stores:

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
    """Store-relative file references. External only."""
    type_name = "filepath"

    def get_dtype(self, is_external: bool) -> str:
        if not is_external:
            raise DataJointError("<filepath> requires @store")
        return "json"

    def encode(self, relative_path: str, *, key=None, store_name=None) -> dict:
        """Register reference to file in store."""
        store = get_store(store_name)  # store_name required for filepath
        return {'path': relative_path, 'store': store_name}

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        """Return ObjectRef for lazy access."""
        return ObjectRef(store=get_store(stored['store']), path=stored['path'])
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
- Used as dtype by built-in AttributeTypes (`<object@>`, `<hash@>`, `<filepath@store>`)
- Stores arbitrary JSON-serializable data
- Automatically uses appropriate type for database backend
- Supports JSON path queries where available

## Built-in AttributeTypes

### `<blob>` / `<blob@>` - Serialized Python Objects

**Supports both internal and external storage.**

Serializes Python objects (NumPy arrays, dicts, lists, etc.) using DataJoint's
blob format. Compatible with MATLAB.

- **`<blob>`**: Stored in database (`bytes` → `LONGBLOB`/`BYTEA`)
- **`<blob@>`**: Stored externally via `<hash@>` with deduplication
- **`<blob@store>`**: Stored in specific named store

```python
@dj.register_type
class BlobType(AttributeType):
    """Serialized Python objects. Supports internal and external."""
    type_name = "blob"

    def get_dtype(self, is_external: bool) -> str:
        return "<hash>" if is_external else "bytes"

    def encode(self, value, *, key=None, store_name=None) -> bytes:
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
    small_result : <blob>          # internal (in database)
    large_result : <blob@>         # external (default store)
    archive_result : <blob@cold>   # external (specific store)
    """
```

### `<attach>` / `<attach@>` - File Attachments

**Supports both internal and external storage.**

Stores files with filename preserved. On fetch, extracts to configured download path.

- **`<attach>`**: Stored in database (`bytes` → `LONGBLOB`/`BYTEA`)
- **`<attach@>`**: Stored externally via `<hash@>` with deduplication
- **`<attach@store>`**: Stored in specific named store

```python
@dj.register_type
class AttachType(AttributeType):
    """File attachment with filename. Supports internal and external."""
    type_name = "attach"

    def get_dtype(self, is_external: bool) -> str:
        return "<hash>" if is_external else "bytes"

    def encode(self, filepath, *, key=None, store_name=None) -> bytes:
        path = Path(filepath)
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        filename, contents = stored.split(b"\0", 1)
        filename = filename.decode()
        download_path = Path(dj.config['download_path']) / filename
        download_path.write_bytes(contents)
        return str(download_path)
```

Usage:
```python
class Attachments(dj.Manual):
    definition = """
    attachment_id : int32
    ---
    config : <attach>           # internal (small file in DB)
    data_file : <attach@>       # external (default store)
    archive : <attach@cold>     # external (specific store)
    """
```

## User-Defined AttributeTypes

Users can define custom AttributeTypes for domain-specific data:

```python
@dj.register_type
class GraphType(AttributeType):
    """Store NetworkX graphs. Internal only (no external support)."""
    type_name = "graph"

    def get_dtype(self, is_external: bool) -> str:
        if is_external:
            raise DataJointError("<graph> does not support external storage")
        return "<blob>"  # Chain to blob for serialization

    def encode(self, graph, *, key=None, store_name=None):
        return {'nodes': list(graph.nodes()), 'edges': list(graph.edges())}

    def decode(self, stored, *, key=None):
        G = nx.Graph()
        G.add_nodes_from(stored['nodes'])
        G.add_edges_from(stored['edges'])
        return G
```

Custom types can support both modes by returning different dtypes:

```python
@dj.register_type
class ImageType(AttributeType):
    """Store images. Supports both internal and external."""
    type_name = "image"

    def get_dtype(self, is_external: bool) -> str:
        return "<hash>" if is_external else "bytes"

    def encode(self, image, *, key=None, store_name=None) -> bytes:
        # Convert PIL Image to PNG bytes
        buffer = io.BytesIO()
        image.save(buffer, format='PNG')
        return buffer.getvalue()

    def decode(self, stored: bytes, *, key=None):
        return PIL.Image.open(io.BytesIO(stored))
```

## Storage Comparison

| Type | get_dtype | Resolves To | Storage Location | Dedup | Returns |
|------|-----------|-------------|------------------|-------|---------|
| `<blob>` | `bytes` | `LONGBLOB`/`BYTEA` | Database | No | Python object |
| `<blob@>` | `<hash>` | `json` | `_hash/{hash}` | Yes | Python object |
| `<blob@s>` | `<hash>` | `json` | `_hash/{hash}` | Yes | Python object |
| `<attach>` | `bytes` | `LONGBLOB`/`BYTEA` | Database | No | Local file path |
| `<attach@>` | `<hash>` | `json` | `_hash/{hash}` | Yes | Local file path |
| `<attach@s>` | `<hash>` | `json` | `_hash/{hash}` | Yes | Local file path |
| `<object@>` | `json` | `JSON`/`JSONB` | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `<object@s>` | `json` | `JSON`/`JSONB` | `{schema}/{table}/{pk}/` | No | ObjectRef |
| `<hash@>` | `json` | `JSON`/`JSONB` | `_hash/{hash}` | Yes | bytes |
| `<hash@s>` | `json` | `JSON`/`JSONB` | `_hash/{hash}` | Yes | bytes |
| `<filepath@s>` | `json` | `JSON`/`JSONB` | Configured store | No | ObjectRef |

## Reference Counting for Hash Type

The `HashRegistry` is a **project-level** table that tracks hash-addressed objects
across all schemas. This differs from the legacy `~external_*` tables which were per-schema.

```python
class HashRegistry:
    """
    Project-level hash registry.
    Stored in a designated database (e.g., `{project}_hash`).
    """
    definition = """
    # Hash-addressed object registry (project-wide)
    hash_id : char(64)          # SHA256 hex
    ---
    store        : varchar(64)       # Store name
    size         : bigint unsigned   # Size in bytes
    created      : timestamp DEFAULT CURRENT_TIMESTAMP
    """
```

Garbage collection scans **all schemas** in the project:

```python
def garbage_collect(project):
    """Remove data not referenced by any table in any schema."""
    # Get all registered hashes
    registered = set(HashRegistry().fetch('hash_id', 'store'))

    # Get all referenced hashes from ALL schemas in the project
    referenced = set()
    for schema in project.schemas:
        for table in schema.tables:
            for attr in table.heading.attributes:
                if attr.type in ('hash', 'hash@...'):
                    hashes = table.fetch(attr.name)
                    referenced.update((h, attr.store) for h in hashes)

    # Delete orphaned data
    for hash_id, store in (registered - referenced):
        store_backend = get_store(store)
        store_backend.delete(hash_path(hash_id))
        (HashRegistry() & {'hash_id': hash_id}).delete()
```

## Built-in AttributeType Comparison

| Feature | `<blob>` | `<attach>` | `<object@>` | `<hash@>` | `<filepath@>` |
|---------|----------|------------|-------------|--------------|---------------|
| Storage modes | Both | Both | External only | External only | External only |
| Internal dtype | `bytes` | `bytes` | N/A | N/A | N/A |
| External dtype | `<hash>` | `<hash>` | `json` | `json` | `json` |
| Addressing | Hash | Hash | Primary key | Hash | Relative path |
| Deduplication | Yes (external) | Yes (external) | No | Yes | No |
| Structure | Single blob | Single file | Files, folders | Single blob | Any |
| Returns | Python object | Local path | ObjectRef | bytes | ObjectRef |
| GC | Ref counted | Ref counted | With row | Ref counted | User managed |

**When to use each:**
- **`<blob>`**: Serialized Python objects (NumPy arrays, dicts). Use `<blob@>` for large/duplicated data
- **`<attach>`**: File attachments with filename preserved. Use `<attach@>` for large files
- **`<object@>`**: Large/complex file structures (Zarr, HDF5) where DataJoint controls organization
- **`<hash@>`**: Raw bytes with deduplication (typically used via `<blob@>` or `<attach@>`)
- **`<filepath@store>`**: Portable references to externally-managed files
- **`varchar`**: Arbitrary URLs/paths where ObjectRef semantics aren't needed

## Key Design Decisions

1. **Three-layer architecture**:
   - Layer 1: Native database types (backend-specific, discouraged)
   - Layer 2: Core DataJoint types (standardized, scientist-friendly)
   - Layer 3: AttributeTypes (encode/decode, composable)
2. **Core types are scientist-friendly**: `float32`, `uint8`, `bool`, `bytes` instead of `FLOAT`, `TINYINT UNSIGNED`, `LONGBLOB`
3. **AttributeTypes use angle brackets**: `<blob>`, `<object@store>`, `<filepath@main>` - distinguishes from core types
4. **`@` indicates external storage**: No `@` = database, `@` present = object store
5. **`get_dtype(is_external)` method**: Types resolve dtype at declaration time based on storage mode
6. **AttributeTypes are composable**: `<blob@>` uses `<hash@>`, which uses `json`
7. **Built-in external types use JSON dtype**: Stores metadata (path, hash, store name, etc.)
8. **Two OAS regions**: object (PK-addressed) and hash (hash-addressed) within managed stores
9. **Filepath for portability**: `<filepath@store>` uses relative paths within stores for environment portability
10. **No `uri` type**: For arbitrary URLs, use `varchar`—simpler and more transparent
11. **Naming conventions**:
    - `@` = external storage (object store)
    - No `@` = internal storage (database)
    - `@` alone = default store
    - `@name` = named store
12. **Dual-mode types**: `<blob>` and `<attach>` support both internal and external storage
13. **External-only types**: `<object@>`, `<hash@>`, `<filepath@>` require `@`
14. **Transparent access**: AttributeTypes return Python objects or file paths
15. **Lazy access**: `<object@>` and `<filepath@store>` return ObjectRef

## Migration from Legacy Types

| Legacy | New Equivalent |
|--------|----------------|
| `longblob` (auto-serialized) | `<blob>` |
| `blob@store` | `<blob@store>` |
| `attach` | `<attach>` |
| `attach@store` | `<attach@store>` |
| `filepath@store` (copy-based) | `<filepath@store>` (ObjectRef-based) |

### Migration from Legacy `~external_*` Stores

Legacy external storage used per-schema `~external_{store}` tables. Migration to the new
per-project `HashRegistry` requires:

```python
def migrate_external_store(schema, store_name):
    """
    Migrate legacy ~external_{store} to new HashRegistry.

    1. Read all entries from ~external_{store}
    2. For each entry:
       - Fetch content from legacy location
       - Compute SHA256 hash
       - Copy to _hash/{hash}/ if not exists
       - Update table column from UUID to hash
       - Register in HashRegistry
    3. After all schemas migrated, drop ~external_{store} tables
    """
    external_table = schema.external[store_name]

    for entry in external_table.fetch(as_dict=True):
        legacy_uuid = entry['hash']

        # Fetch content from legacy location
        content = external_table.get(legacy_uuid)

        # Compute new content hash
        hash_id = hashlib.sha256(content).hexdigest()

        # Store in new location if not exists
        new_path = f"_hash/{hash_id[:2]}/{hash_id[2:4]}/{hash_id}"
        store = get_store(store_name)
        if not store.exists(new_path):
            store.put(new_path, content)

        # Register in project-wide HashRegistry
        HashRegistry().insert1({
            'hash_id': hash_id,
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

1. How long should the backward compatibility layer support legacy `~external_*` format?
2. Should `<hash@>` (without store name) use a default store or require explicit store name?
