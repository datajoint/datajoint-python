# DataJoint Storage Types Redesign - Implementation Plan

## Executive Summary

This plan describes the implementation of a three-layer type architecture for DataJoint, building on the existing `AttributeType` infrastructure. The key goals are:

1. Establish a clean three-layer type hierarchy (native DB types, core DataJoint types, AttributeTypes)
2. Implement content-addressed storage with deduplication
3. Provide composable, user-friendly types (`<xblob>`, `<xattach>`, `<filepath@store>`)
4. Enable project-wide garbage collection via `ContentRegistry`
5. Maintain backward compatibility with existing schemas

---

## Phase 1: Core Type System Foundation

**Goal**: Establish the complete Layer 2 core type mappings and enhance the AttributeType infrastructure.

### 1.1 Expand Core Type Mappings

**Files to modify:**
- `src/datajoint/declare.py`

**Current state**: `SQL_TYPE_ALIASES` already maps some types (float32, int32, etc.)

**Changes needed**:
1. Complete the type mappings as per spec:
   ```
   Core Type -> MySQL Type
   int8      -> TINYINT
   uint8     -> TINYINT UNSIGNED
   int16     -> SMALLINT
   ...
   json      -> JSON
   uuid      -> BINARY(16) or CHAR(36)
   decimal   -> DECIMAL(p,s)
   ```

2. Add PostgreSQL mappings for future support (can be placeholder initially)

**Dependencies**: None

### 1.2 Enhance AttributeType with Store Parameter Support

**Files to modify:**
- `src/datajoint/attribute_type.py`

**Current state**: Types don't support `@store` parameter syntax

**Changes needed**:
1. Add `store_name` property to `AttributeType`
2. Modify `resolve_dtype()` to handle `<type@store>` syntax
3. Add `get_type_with_store(name_with_store)` helper that parses `xblob@cold` format

```python
def parse_type_spec(spec: str) -> tuple[str, str | None]:
    """Parse '<type@store>' or '<type>' into (type_name, store_name)."""
    spec = spec.strip("<>")
    if "@" in spec:
        type_name, store_name = spec.split("@", 1)
        return type_name, store_name
    return spec, None
```

**Dependencies**: None

### 1.3 Update Heading and Declaration Parsing

**Files to modify:**
- `src/datajoint/heading.py`
- `src/datajoint/declare.py`

**Changes needed**:
1. Update `TYPE_PATTERN` to recognize new AttributeType patterns
2. Store `store_name` in attribute metadata for parameterized types
3. Update `compile_attribute()` to handle `<type@store>` syntax
4. Update `_init_from_database()` to reconstruct store information

**Dependencies**: Phase 1.2

---

## Phase 2: Content-Addressed Storage Implementation

**Goal**: Implement the `<content>` type with content-addressed storage and deduplication.

### 2.1 Create ContentRegistry Table

**New file to create:**
- `src/datajoint/content_registry.py`

**Implementation**:
```python
class ContentRegistry:
    """
    Project-level content registry for content-addressed storage.
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

Key features:
- Auto-create the registry database on first use
- Methods: `insert_content()`, `get_content()`, `increment_ref()`, `decrement_ref()`
- Thread-safe reference counting (if needed)

**Dependencies**: None

### 2.2 Implement ContentType AttributeType

**Files to modify:**
- `src/datajoint/attribute_type.py`

**New built-in type**:
```python
class ContentType(AttributeType):
    """Built-in AttributeType for content-addressed storage."""
    type_name = "content"
    dtype = "json"

    def encode(self, data: bytes, *, key=None, store_name=None) -> dict:
        """Store content, return metadata as JSON."""
        content_hash = hashlib.sha256(data).hexdigest()
        path = f"_content/{content_hash[:2]}/{content_hash[2:4]}/{content_hash}"
        # Store if not exists, register in ContentRegistry
        ...
        return {"hash": content_hash, "store": store_name, "size": len(data)}

    def decode(self, stored: dict, *, key=None) -> bytes:
        """Retrieve content by hash."""
        ...
```

**Dependencies**: Phase 2.1

### 2.3 Implement Content Storage Backend Methods

**Files to modify:**
- `src/datajoint/storage.py`

**Changes needed**:
1. Add `put_content()` method with deduplication
2. Add `get_content()` method with hash verification
3. Add `compute_content_hash()` utility
4. Add content path generation: `_content/{hash[:2]}/{hash[2:4]}/{hash}`

**Dependencies**: None

---

## Phase 3: User-Defined AttributeTypes

**Goal**: Implement the standard user-facing types that compose with `<content>` and `<object>`.

### 3.1 Implement XBlobType (External Blob)

**Files to modify:**
- `src/datajoint/attribute_type.py`

```python
@register_type
class XBlobType(AttributeType):
    """External serialized blob using content-addressed storage."""
    type_name = "xblob"
    dtype = "<content>"  # Composition: uses ContentType

    def encode(self, value, *, key=None) -> bytes:
        from . import blob
        return blob.pack(value, compress=True)

    def decode(self, stored, *, key=None) -> Any:
        from . import blob
        return blob.unpack(stored)
```

**Key behavior**: Serializes to djblob format, stores via content-addressed storage

**Dependencies**: Phase 2.2

### 3.2 Implement AttachType and XAttachType

**Files to modify:**
- `src/datajoint/attribute_type.py`

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

    def encode(self, filepath, *, key=None) -> bytes:
        path = Path(filepath)
        return path.name.encode() + b"\0" + path.read_bytes()

    def decode(self, stored, *, key=None) -> str:
        # Same as AttachType.decode()
        ...
```

**Dependencies**: Phase 2.2

### 3.3 Implement FilepathType

**Files to modify:**
- `src/datajoint/attribute_type.py`

```python
@register_type
class FilepathType(AttributeType):
    """Portable relative path reference within configured stores."""
    type_name = "filepath"
    dtype = "json"

    def encode(self, relative_path: str, *, key=None, store_name=None,
               compute_checksum: bool = False) -> dict:
        """Register reference to file in store."""
        store = get_store(store_name)  # Required for filepath
        metadata = {'path': relative_path, 'store': store_name}
        if compute_checksum:
            # Compute checksum and size
            ...
        return metadata

    def decode(self, stored: dict, *, key=None) -> ObjectRef:
        """Return ObjectRef for lazy access."""
        return ObjectRef(
            store=get_store(stored['store']),
            path=stored['path'],
            checksum=stored.get('checksum')
        )
```

**Key difference from legacy**: Returns `ObjectRef` instead of copying to local stage

**Dependencies**: Existing `ObjectRef` and `StorageBackend`

---

## Phase 4: Insert and Fetch Integration

**Goal**: Update the data path to handle the new type system seamlessly.

### 4.1 Update Insert Processing

**Files to modify:**
- `src/datajoint/table.py`

**Changes needed in `__make_placeholder()`**:
1. Handle type composition (resolve full type chain)
2. Pass `store_name` to `encode()` when applicable
3. Handle `<content>` type's special behavior
4. Process `<filepath@store>` with store parameter

```python
def __make_placeholder(self, name, value, ...):
    attr = self.heading[name]
    if attr.adapter:
        # Resolve type chain and pass store_name
        final_dtype, type_chain = resolve_dtype(attr.adapter.dtype)
        store_name = attr.store

        # Apply type chain: outer -> inner
        for attr_type in type_chain:
            value = attr_type.encode(value, key=key, store_name=store_name)

        # Continue with final_dtype processing
        ...
```

**Dependencies**: Phases 1-3

### 4.2 Update Fetch Processing

**Files to modify:**
- `src/datajoint/fetch.py`

**Changes needed in `_get()`**:
1. Handle `<content>` type: retrieve from content store
2. Handle type composition: apply decoders in reverse order
3. Handle `<filepath@store>`: return `ObjectRef` instead of downloading

```python
def _get(connection, attr, data, squeeze, download_path):
    if attr.adapter:
        final_dtype, type_chain = resolve_dtype(attr.adapter.dtype)

        # Process based on final_dtype
        if final_dtype == "json":
            data = json.loads(data)
        elif final_dtype == "longblob":
            # Handle content retrieval if needed
            ...

        # Apply type chain in reverse: inner -> outer
        for attr_type in reversed(type_chain):
            data = attr_type.decode(data, key=key)

        return data
```

**Dependencies**: Phases 1-3

### 4.3 Update Heading Attribute Properties

**Files to modify:**
- `src/datajoint/heading.py`

**Changes needed**:
1. Add `is_content` property for content-addressed attributes
2. Update property detection logic for new types
3. Store composed type information for fetch/insert

**Dependencies**: Phase 1.3

---

## Phase 5: Garbage Collection

**Goal**: Implement project-wide garbage collection for content-addressed storage.

### 5.1 Implement GC Scanner

**New file to create:**
- `src/datajoint/gc.py`

```python
def scan_content_references(project) -> set[tuple[str, str]]:
    """
    Scan all schemas in project for content references.

    Returns:
        Set of (content_hash, store) tuples that are referenced
    """
    referenced = set()
    for schema in project.schemas:
        for table in schema.tables:
            for attr in table.heading.attributes:
                if attr.type in ('content', 'xblob', 'xattach'):
                    hashes = table.fetch(attr.name)
                    for h in hashes:
                        if isinstance(h, dict):
                            referenced.add((h['hash'], h.get('store')))
    return referenced

def garbage_collect(project, dry_run=True) -> dict:
    """
    Remove unreferenced content from storage.

    Returns:
        Stats: {'scanned': N, 'orphaned': M, 'deleted': K, 'bytes_freed': B}
    """
    ...
```

**Dependencies**: Phase 2.1

### 5.2 Add GC CLI Commands

**Files to modify:**
- CLI or management interface

**New commands**:
- `dj gc scan` - Scan and report orphaned content
- `dj gc clean` - Remove orphaned content
- `dj gc status` - Show content registry status

**Dependencies**: Phase 5.1

---

## Phase 6: Migration Utilities

**Goal**: Provide tools to migrate existing schemas to the new type system.

### 6.1 Enhance Migration Module

**Files to modify:**
- `src/datajoint/migrate.py`

**New functions**:

```python
def analyze_external_stores(schema) -> list[dict]:
    """Analyze legacy ~external_* tables for migration."""
    ...

def migrate_external_to_content(schema, store_name, dry_run=True) -> dict:
    """
    Migrate legacy ~external_{store} to new ContentRegistry.

    Steps:
    1. Read entries from ~external_{store}
    2. For each entry: fetch content, compute SHA256
    3. Copy to _content/{hash}/ if not exists
    4. Update referencing tables (UUID -> hash JSON)
    5. Register in ContentRegistry
    """
    ...

def migrate_blob_to_djblob(schema, dry_run=True) -> dict:
    """Update implicit blob columns to use <djblob>."""
    ...

def migrate_filepath_to_new(schema, dry_run=True) -> dict:
    """
    Migrate legacy filepath@store to new <filepath@store>.

    Changes:
    - UUID column -> JSON column
    - Copy-based access -> ObjectRef-based access
    """
    ...
```

### 6.2 Create Migration CLI

**New commands**:
- `dj migrate analyze <schema>` - Analyze migration needs
- `dj migrate external <schema> <store>` - Migrate external store
- `dj migrate blobs <schema>` - Migrate blob columns
- `dj migrate status <schema>` - Show migration status

**Dependencies**: Phase 6.1

---

## Phase 7: Documentation and Testing

### 7.1 Unit Tests

**New test files:**
- `tests/test_content_type.py` - Content-addressed storage tests
- `tests/test_xblob.py` - XBlob type tests
- `tests/test_attach_types.py` - Attachment type tests
- `tests/test_filepath_new.py` - New filepath tests
- `tests/test_gc.py` - Garbage collection tests
- `tests/test_migration.py` - Migration utility tests

**Existing test files to update:**
- `tests/test_attribute_type.py` - Add new type tests
- `tests/test_object.py` - Verify object type unchanged

### 7.2 Integration Tests

**Test scenarios**:
1. Insert/fetch roundtrip for all new types
2. Type composition (xblob using content)
3. Multi-schema content deduplication
4. GC with cross-schema references
5. Migration from legacy external stores
6. Backward compatibility with existing schemas

### 7.3 Documentation

**Files to update:**
- `docs/src/design/tables/storage-types-spec.md` - Already exists
- Create user guide for new types
- Create migration guide
- Update API reference

---

## Implementation Order and Dependencies

```
Phase 1: Core Type System Foundation
├── 1.1 Expand Core Type Mappings (no deps)
├── 1.2 Enhance AttributeType with Store Parameter (no deps)
└── 1.3 Update Heading and Declaration Parsing (depends on 1.2)

Phase 2: Content-Addressed Storage
├── 2.1 Create ContentRegistry Table (no deps)
├── 2.2 Implement ContentType (depends on 2.1)
└── 2.3 Content Storage Backend Methods (no deps)

Phase 3: User-Defined AttributeTypes (depends on Phase 2)
├── 3.1 Implement XBlobType (depends on 2.2)
├── 3.2 Implement AttachType and XAttachType (depends on 2.2)
└── 3.3 Implement FilepathType (no deps)

Phase 4: Insert and Fetch Integration (depends on Phases 1-3)
├── 4.1 Update Insert Processing
├── 4.2 Update Fetch Processing
└── 4.3 Update Heading Attribute Properties

Phase 5: Garbage Collection (depends on Phase 2)
├── 5.1 Implement GC Scanner
└── 5.2 Add GC CLI Commands

Phase 6: Migration Utilities (depends on Phases 2-4)
├── 6.1 Enhance Migration Module
└── 6.2 Create Migration CLI

Phase 7: Documentation and Testing (ongoing)
```

---

## Critical Files Summary

| File | Changes |
|------|---------|
| `src/datajoint/attribute_type.py` | All new AttributeTypes: `ContentType`, `XBlobType`, `AttachType`, `XAttachType`, `FilepathType` |
| `src/datajoint/declare.py` | Type pattern parsing, SQL generation, `<type@store>` syntax |
| `src/datajoint/heading.py` | Attribute metadata, composed type information |
| `src/datajoint/table.py` | Insert logic with type composition |
| `src/datajoint/fetch.py` | Fetch logic with type chain decoding |
| `src/datajoint/content_registry.py` | **New**: ContentRegistry table and methods |
| `src/datajoint/gc.py` | **New**: Garbage collection scanner |
| `src/datajoint/migrate.py` | Migration utilities |

---

## Risk Mitigation

### Backward Compatibility
1. All existing types (`longblob`, `blob@store`, `attach@store`, `filepath@store`) continue to work
2. Legacy `~external_*` tables remain functional during transition
3. Implicit blob serialization preserved for existing schemas
4. Migration is opt-in and reversible

### Performance Considerations
1. Content hashing uses SHA256 (fast, widely supported)
2. Deduplication reduces storage costs
3. Lazy ObjectRef prevents unnecessary I/O
4. GC runs on-demand, not automatically

### Error Handling
1. Content hash verification on fetch (optional)
2. Graceful handling of missing content
3. Transaction safety for multi-table operations
4. Clear error messages for misconfiguration

---

## Estimated Effort

| Phase | Estimated Days | Risk |
|-------|----------------|------|
| Phase 1 | 3-4 days | Low |
| Phase 2 | 4-5 days | Medium |
| Phase 3 | 3-4 days | Low |
| Phase 4 | 4-5 days | Medium |
| Phase 5 | 2-3 days | Low |
| Phase 6 | 3-4 days | Medium |
| Phase 7 | 5-7 days | Low |
| **Total** | **24-32 days** | |
