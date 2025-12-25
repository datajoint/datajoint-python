# Storage Types Redesign Spec

## Overview

This document proposes a unified storage architecture where all external storage uses the Object-Augmented Schema (OAS) paradigm, with a special content-addressable region for deduplicated objects.

## Architecture

### Two Storage Modes within OAS

```
store_root/
├── {schema}/{table}/{pk}/           # Path-addressed (regular OAS)
│   └── {attribute}/                 # Derived from primary key
│       └── ...                      # Files, folders, Zarr, etc.
│
└── _content/                        # Content-addressed (deduplicated)
    └── {hash[:2]}/{hash[2:4]}/
        └── {hash}/                  # Full SHA256 hash
            └── ...                  # Object contents
```

### 1. Path-Addressed Objects (`object@store`)

**Already implemented.** Regular OAS behavior:
- Path derived from primary key
- One-to-one relationship with table row
- Deleted when row is deleted
- Returns `ObjectRef` for lazy access

```python
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    results : object@main
    """
```

### 2. Content-Addressed Objects (`<djblob@store>`, `<attach@store>`)

**New.** Stored in `_content/` region with deduplication:
- Path derived from content hash (SHA256)
- Many-to-one: multiple rows can reference same object
- Reference counted for garbage collection
- **Transparent access**: Returns same type as internal variant (Python object or file path)

```python
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    features : <djblob@main>     # Returns Python object (fetched transparently)
    source_file : <attach@main>  # Returns local file path (downloaded transparently)
    """
```

## Content-Addressed Storage Design

### Storage Path

```python
def content_path(content_hash: str) -> str:
    """Generate path for content-addressed object."""
    return f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"

# Example: hash "a1b2c3d4..." -> "_content/a1/b2/a1b2c3d4..."
```

### Reference Registry

A schema-level table tracks content-addressed objects for reference counting:

```python
class ContentRegistry:
    """
    Tracks content-addressed objects for garbage collection.
    One per schema, created automatically when content-addressed types are used.
    """
    definition = """
    # Content-addressed object registry
    content_hash : char(64)          # SHA256 hex
    ---
    store        : varchar(64)       # Store name
    size         : bigint unsigned   # Object size in bytes
    created      : timestamp DEFAULT CURRENT_TIMESTAMP
    """
```

### Reference Counting

Reference counting is implicit via database queries:

```python
def find_orphans(schema) -> list[tuple[str, str]]:
    """Find content hashes not referenced by any table."""

    # Get all registered hashes
    registered = set(ContentRegistry().fetch('content_hash', 'store'))

    # Get all referenced hashes from tables
    referenced = set()
    for table in schema.tables:
        for attr in table.heading.attributes:
            if attr.is_content_addressed:
                hashes = table.fetch(attr.name)
                referenced.update((h, attr.store) for h in hashes)

    return registered - referenced

def garbage_collect(schema):
    """Remove orphaned content-addressed objects."""
    for content_hash, store in find_orphans(schema):
        # Delete from storage
        store_backend = get_store(store)
        store_backend.delete(content_path(content_hash))
        # Delete from registry
        (ContentRegistry() & {'content_hash': content_hash}).delete()
```

### Transparent Access for Content-Addressed Objects

Content-addressed objects return the same types as their internal counterparts:

```python
row = (ProcessedData & key).fetch1()

# <djblob@store> returns Python object (like <djblob>)
features = row['features']         # dict, array, etc. - fetched and deserialized

# <attach@store> returns local file path (like <attach>)
local_path = row['source_file']    # '/downloads/data.csv' - downloaded automatically

# Only object@store returns ObjectRef for explicit lazy access
ref = row['results']               # ObjectRef - user controls when to download
```

This makes external storage transparent - users work with Python objects and file paths,
not storage references. The `@store` suffix only affects where data is stored, not how
it's accessed.

## AttributeType Implementations

### `<djblob>` - Internal Serialized Blob

```python
@dj.register_type
class DJBlobType(AttributeType):
    type_name = "djblob"
    dtype = "longblob"

    def encode(self, value, *, key=None) -> bytes:
        from . import blob
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        from . import blob
        return blob.unpack(stored)
```

### `<djblob@store>` - External Serialized Blob (Content-Addressed)

```python
@dj.register_type
class DJBlobExternalType(AttributeType):
    type_name = "djblob"
    dtype = "char(64)"  # Content hash stored in column
    is_content_addressed = True

    def encode(self, value, *, key=None, store=None) -> str:
        from . import blob
        data = blob.pack(value, compress=True)
        content_hash = hashlib.sha256(data).hexdigest()

        # Upload if not exists (deduplication)
        path = content_path(content_hash)
        if not store.exists(path):
            store.put(path, data)
            ContentRegistry().insert1({
                'content_hash': content_hash,
                'store': store.name,
                'size': len(data)
            })

        return content_hash

    def decode(self, content_hash, *, key=None, store=None) -> Any:
        # Fetch and deserialize - transparent to user
        from . import blob
        path = content_path(content_hash)
        data = store.get(path)
        return blob.unpack(data)
```

### `<attach>` - Internal File Attachment

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

### `<attach@store>` - External File Attachment (Content-Addressed)

```python
@dj.register_type
class AttachExternalType(AttributeType):
    type_name = "attach"
    dtype = "char(64)"  # Content hash stored in column
    is_content_addressed = True

    def encode(self, filepath, *, key=None, store=None) -> str:
        path = Path(filepath)
        data = path.read_bytes()
        # Hash includes filename for uniqueness
        content_hash = hashlib.sha256(
            path.name.encode() + b"\0" + data
        ).hexdigest()

        # Store with original filename preserved
        obj_path = content_path(content_hash)
        if not store.exists(obj_path):
            store.put(f"{obj_path}/{path.name}", data)
            ContentRegistry().insert1({
                'content_hash': content_hash,
                'store': store.name,
                'size': len(data)
            })

        return content_hash

    def decode(self, content_hash, *, key=None, store=None) -> str:
        # Download and return local path - transparent to user
        obj_path = content_path(content_hash)
        # List to get filename (stored as {hash}/{filename})
        filename = store.list(obj_path)[0]
        download_path = Path(dj.config['download_path']) / filename
        download_path.parent.mkdir(parents=True, exist_ok=True)
        store.download(f"{obj_path}/{filename}", download_path)
        return str(download_path)
```

## ObjectRef Interface (for `object@store` only)

Only `object@store` returns `ObjectRef` for explicit lazy access. This is intentional -
large files and folders (Zarr, HDF5, etc.) benefit from user-controlled download/access.

```python
class ObjectRef:
    """Lazy reference to stored object (object@store only)."""

    def __init__(self, path, store):
        self.path = path
        self.store = store

    def download(self, local_path=None) -> Path:
        """Download object to local filesystem."""
        if local_path is None:
            local_path = Path(dj.config['download_path']) / Path(self.path).name
        self.store.download(self.path, local_path)
        return local_path

    def open(self, mode='rb'):
        """Open via fsspec for streaming/direct access."""
        return self.store.open(self.path, mode)

    def exists(self) -> bool:
        """Check if object exists in store."""
        return self.store.exists(self.path)
```

## Summary

| Type | Storage | Column | Dedup | Returns |
|------|---------|--------|-------|---------|
| `object@store` | `{schema}/{table}/{pk}/` | JSON | No | ObjectRef (lazy) |
| `<djblob>` | Internal DB | LONGBLOB | No | Python object |
| `<djblob@store>` | `_content/{hash}/` | char(64) | Yes | Python object |
| `<attach>` | Internal DB | LONGBLOB | No | Local file path |
| `<attach@store>` | `_content/{hash}/` | char(64) | Yes | Local file path |

## Key Design Decisions

1. **Unified OAS paradigm**: All external storage uses OAS infrastructure
2. **Content-addressed region**: `_content/` folder for deduplicated objects
3. **Reference counting**: Via `ContentRegistry` table + query-based orphan detection
4. **Transparent access**: `<djblob@store>` and `<attach@store>` return same types as internal variants
5. **Lazy access for objects**: Only `object@store` returns ObjectRef (for large files/folders)
6. **Deduplication**: Content hash determines identity; identical content stored once

## Migration from Legacy `~external_*`

For existing schemas with `~external_*` tables:

1. Read legacy external references
2. Re-upload to `_content/` region
3. Update column values to content hashes
4. Drop `~external_*` tables
5. Create `ContentRegistry` entries

## Open Questions

1. **Hash collision**: SHA256 is effectively collision-free, but should we verify on fetch?
2. **Partial uploads**: How to handle interrupted uploads? Temp path then rename?
3. **Cross-schema deduplication**: Should `_content/` be per-schema or global?
4. **Backward compat**: How long to support reading from legacy `~external_*`?
