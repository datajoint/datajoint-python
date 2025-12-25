# Storage Types Redesign Spec

## Overview

This document proposes a redesign of DataJoint's storage types as AttributeTypes, with clear separation between:

1. **Object-Augmented Schemas (OAS)** - New paradigm with managed stores, integrity constraints, and prescribed organization
2. **Legacy External Storage** - Content-addressed blob/attach storage with deduplication
3. **Internal Blob Types** - AttributeTypes that serialize into database blob columns

## Type Categories

### 1. Object-Augmented Schemas (`object`, `object@store`)

**Already implemented.** A distinct system where stores are treated as part of the database:

- Robust integrity constraints
- Prescribed path organization (derived from primary key)
- Multiple store support via config
- Returns `ObjectRef` for lazy access
- Supports direct writes (Zarr, HDF5) via fsspec

```python
# Table definition
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    results : object@main      # stored in 'main' OAS store
    """

# Usage
row = (Analysis & key).fetch1()
ref = row['results']           # ObjectRef handle (lazy)
ref.download('/local/path')    # explicit download
data = ref.open()              # fsspec access
```

**This type is NOT part of the AttributeType redesign** - it has its own implementation path.

---

### 2. Serialized Blobs (`<djblob>`)

**Already implemented.** AttributeType for Python object serialization.

- Input: Any Python object (arrays, dicts, lists, etc.)
- Output: Same Python object reconstructed
- Storage: DJ blob format (mYm/dj0 protocol) in LONGBLOB column

```python
class DJBlobType(AttributeType):
    type_name = "djblob"
    dtype = "longblob"

    def encode(self, value, *, key=None) -> bytes:
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        return blob.unpack(stored)
```

---

### 3. File Attachments (`<attach>`) - TO IMPLEMENT

AttributeType for serializing files into internal blob columns.

- Input: File path (string or Path)
- Output: Local file path after extraction
- Storage: `filename\0contents` in LONGBLOB column

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
        download_path = Path(dj.config['download_path']) / filename
        download_path.parent.mkdir(parents=True, exist_ok=True)
        download_path.write_bytes(contents)
        return str(download_path)
```

**Usage:**
```python
class Configs(dj.Manual):
    definition = """
    config_id : int
    ---
    config_file : <attach>    # file serialized into DB
    """

# Insert
table.insert1({'config_id': 1, 'config_file': '/path/to/config.yaml'})

# Fetch - file extracted to download_path
row = (table & 'config_id=1').fetch1()
local_path = row['config_file']  # '/downloads/config.yaml'
```

---

### 4. External Content-Addressed Storage (`<djblob@store>`, `<attach@store>`) - TO DESIGN

These types store content externally with deduplication via content hashing.

#### Design Option A: Leverage OAS Stores

Store content-addressed blobs within OAS stores under a reserved folder:

```
store_root/
├── _external/           # Reserved for content-addressed storage
│   ├── blobs/           # For <djblob@store>
│   │   └── ab/cd/abcd1234...  # Path derived from content hash
│   └── attach/          # For <attach@store>
│       └── ef/gh/efgh5678.../filename.ext
└── schema_name/         # Normal OAS paths
    └── table_name/
        └── pk_value/
```

**Advantages:**
- Reuses OAS infrastructure (fsspec, store config)
- DataJoint fully controls paths
- Deduplication via content hash
- No separate `~external_*` tracking tables needed

**Implementation:**

```python
class ContentAddressedType(AttributeType):
    """Base class for content-addressed external storage."""

    subfolder: str  # 'blobs' or 'attach'

    def _content_hash(self, data: bytes) -> str:
        """Compute content hash for deduplication."""
        return hashlib.sha256(data).hexdigest()

    def _store_path(self, content_hash: str) -> str:
        """Generate path within _external folder."""
        return f"_external/{self.subfolder}/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"


@dj.register_type
class DJBlobExternalType(ContentAddressedType):
    type_name = "djblob"  # Same name, different dtype triggers external
    dtype = "varchar(64)"  # Store content hash as string
    subfolder = "blobs"

    def encode(self, value, *, key=None, store=None) -> str:
        data = blob.pack(value, compress=True)
        content_hash = self._content_hash(data)
        path = self._store_path(content_hash)
        # Upload to store if not exists (deduplication)
        store.put_if_absent(path, data)
        return content_hash

    def decode(self, content_hash, *, key=None, store=None) -> Any:
        path = self._store_path(content_hash)
        data = store.get(path)
        return blob.unpack(data)


@dj.register_type
class AttachExternalType(ContentAddressedType):
    type_name = "attach"
    dtype = "varchar(64)"
    subfolder = "attach"

    def encode(self, filepath, *, key=None, store=None) -> str:
        path = Path(filepath)
        # Hash includes filename for uniqueness
        data = path.name.encode() + b"\0" + path.read_bytes()
        content_hash = self._content_hash(data)
        store_path = self._store_path(content_hash) + "/" + path.name
        store.put_if_absent(store_path, path.read_bytes())
        return content_hash

    def decode(self, content_hash, *, key=None, store=None) -> str:
        # List files in hash folder to get filename
        ...
```

#### Design Option B: Separate Tracking Tables (Current Approach)

Keep `~external_{store}` tables for tracking:

```sql
-- ~external_main
hash           : binary(16)  # UUID from content hash
---
size           : bigint
attachment_name: varchar(255)  # for attach only
timestamp      : timestamp
```

**Disadvantages:**
- Separate infrastructure from OAS
- Additional table maintenance
- More complex cleanup/garbage collection

#### Recommendation

**Option A (OAS integration)** is cleaner:
- Single storage paradigm
- Simpler mental model
- Content hash stored directly in column (no UUID indirection)
- Deduplication at storage level

---

### 5. Reference Tracking (`<ref@store>`) - TO DESIGN

Repurpose `filepath@store` as a general reference type, borrowing from ObjRef:

**Current `filepath@store` limitations:**
- Path-addressed (hash of path, not contents)
- Requires staging area
- Archaic copy-to/copy-from model

**Proposed `<ref@store>`:**
- Track references to external resources
- Support multiple reference types (file path, URL, object key)
- Borrow lazy access patterns from ObjRef
- Optional content verification

```python
@dj.register_type
class RefType(AttributeType):
    type_name = "ref"
    dtype = "json"

    def encode(self, value, *, key=None, store=None) -> str:
        if isinstance(value, str):
            # Treat as path/URL
            return json.dumps({
                'type': 'path',
                'path': value,
                'store': store.name,
                'content_hash': self._compute_hash(value) if verify else None
            })
        elif isinstance(value, RefSpec):
            return json.dumps(value.to_dict())

    def decode(self, json_str, *, key=None, store=None) -> Ref:
        data = json.loads(json_str)
        return Ref(data, store=store)


class Ref:
    """Reference handle (similar to ObjectRef)."""

    def __init__(self, data, store):
        self.path = data['path']
        self.store = store
        self._content_hash = data.get('content_hash')

    def download(self, local_path):
        """Download referenced file."""
        self.store.download(self.path, local_path)
        if self._content_hash:
            self._verify(local_path)

    def open(self, mode='rb'):
        """Open via fsspec (lazy)."""
        return self.store.open(self.path, mode)
```

**Usage:**
```python
class ExternalData(dj.Manual):
    definition = """
    data_id : int
    ---
    source : <ref@archive>    # reference to external file
    """

# Insert - just tracks the reference
table.insert1({'data_id': 1, 'source': '/archive/experiment_001/data.h5'})

# Fetch - returns Ref handle
row = (table & 'data_id=1').fetch1()
ref = row['source']
ref.download('/local/data.h5')  # explicit download
```

---

## Summary of Types

| Type | Storage | Column | Input | Output | Dedup |
|------|---------|--------|-------|--------|-------|
| `object@store` | OAS store | JSON | path/ref | ObjectRef | By path |
| `<djblob>` | Internal | LONGBLOB | any | any | No |
| `<djblob@store>` | OAS `_external/` | varchar(64) | any | any | By content |
| `<attach>` | Internal | LONGBLOB | path | path | No |
| `<attach@store>` | OAS `_external/` | varchar(64) | path | path | By content |
| `<ref@store>` | OAS store | JSON | path/ref | Ref | No (tracks) |

## Open Questions

1. **Store syntax**: Should external AttributeTypes use `<djblob@store>` or detect externality from dtype?

2. **Backward compatibility**: How to handle existing `blob@store` and `attach@store` columns with `~external_*` tables?

3. **Deduplication scope**: Per-store or global across stores?

4. **Ref vs filepath**: Deprecate `filepath@store` entirely or keep as alias?

5. **Content hash format**: SHA256 hex (64 chars) or shorter hash?

## Implementation Phases

### Phase 1: `<attach>` Internal
- Implement AttachType for internal blob storage
- Deprecate bare `attach` keyword (still works, warns)

### Phase 2: Content-Addressed External
- Implement ContentAddressedType base
- Add `<djblob@store>` and `<attach@store>`
- Store in OAS `_external/` folder

### Phase 3: Reference Type
- Implement `<ref@store>` with Ref handle
- Deprecate `filepath@store`

### Phase 4: Migration Tools
- Tools to migrate `~external_*` data to new format
- Backward compat layer for reading old format
