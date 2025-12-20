# File Column Type Specification

## Overview

The `file` type introduces a new paradigm for managed file storage in DataJoint. Unlike existing `attach@store` and `filepath@store` types that reference named stores, the `file` type uses a **unified storage backend** that is tightly coupled with the schema and configured at the pipeline level.

The `file` type supports both **files and folders**. Content is copied to storage at insert time, referenced via handle on fetch, and deleted when the record is deleted.

### Immutability Contract

Files stored via the `file` type are **immutable**. Users agree to:
- **Insert**: Copy content to storage (only way to create)
- **Fetch**: Read content via handle (no modification)
- **Delete**: Remove content when record is deleted (only way to remove)

Users must not directly modify files in the object store.

## Storage Architecture

### Single Storage Backend Per Pipeline

Each DataJoint pipeline has **one** associated storage backend configured in `datajoint.json`. DataJoint fully controls the path structure within this backend.

### Supported Backends

DataJoint uses **[`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/)** to ensure compatibility across multiple storage backends:

- **Local storage** – POSIX-compliant file systems (e.g., NFS, SMB)
- **Cloud-based object storage** – Amazon S3, Google Cloud Storage, Azure Blob, MinIO

## Project Structure

A DataJoint project creates a structured hierarchical storage pattern:

```
📁 project_name/
├── datajoint.json
├── 📁 schema_name1/
├── 📁 schema_name2/
├── 📁 schema_name3/
│   ├── schema.py
│   ├── 📁 tables/
│   │   ├── table1/key1-value1.parquet
│   │   ├── table2/key2-value2.parquet
│   │   ...
│   ├── 📁 objects/
│   │   ├── table1-field1/key3-value3.zarr
│   │   ├── table1-field2/key3-value3.gif
│   │   ...
```

### Object Storage Keys

When using cloud object storage:

```
s3://bucket/project_name/schema_name3/objects/table1/key1-value1.parquet
s3://bucket/project_name/schema_name3/objects/table1-field1/key3-value3.zarr
```

## Configuration

### Settings Structure

Object storage is configured in `datajoint.json` using the existing settings system:

```json
{
    "database.host": "localhost",
    "database.user": "datajoint",

    "object_storage.project_name": "my_project",
    "object_storage.protocol": "s3",
    "object_storage.endpoint": "s3.amazonaws.com",
    "object_storage.bucket": "my-bucket",
    "object_storage.location": "my_project",
    "object_storage.partition_pattern": "{subject_id}/{session_id}"
}
```

For local filesystem storage:

```json
{
    "object_storage.project_name": "my_project",
    "object_storage.protocol": "file",
    "object_storage.location": "/data/my_project",
    "object_storage.partition_pattern": "{subject_id}/{session_id}"
}
```

### Settings Schema

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `object_storage.project_name` | string | Yes | Unique project identifier (must match store metadata) |
| `object_storage.protocol` | string | Yes | Storage backend: `file`, `s3`, `gcs`, `azure` |
| `object_storage.location` | string | Yes | Base path or bucket prefix |
| `object_storage.bucket` | string | For cloud | Bucket name (S3, GCS, Azure) |
| `object_storage.endpoint` | string | For S3 | S3 endpoint URL |
| `object_storage.partition_pattern` | string | No | Path pattern with `{attribute}` placeholders |
| `object_storage.token_length` | int | No | Random suffix length for filenames (default: 8, range: 4-16) |
| `object_storage.access_key` | string | For cloud | Access key (can use secrets file) |
| `object_storage.secret_key` | string | For cloud | Secret key (can use secrets file) |

### Environment Variables

Settings can be overridden via environment variables:

```bash
DJ_OBJECT_STORAGE_PROTOCOL=s3
DJ_OBJECT_STORAGE_BUCKET=my-bucket
DJ_OBJECT_STORAGE_LOCATION=my_project
DJ_OBJECT_STORAGE_PARTITION_PATTERN="subject{subject_id}/session{session_id}"
```

### Secrets

Credentials can be stored in the `.secrets/` directory:

```
.secrets/
├── object_storage.access_key
└── object_storage.secret_key
```

### Partition Pattern

The partition pattern is configured **per pipeline** (one per settings file). Placeholders use `{attribute_name}` syntax and are replaced with primary key values.

```json
{
    "object_storage.partition_pattern": "subject{subject_id}/session{session_id}"
}
```

**Example with partitioning:**

```
s3://my-bucket/my_project/subject123/session45/schema_name/objects/Recording-raw_data/recording.dat
```

If no partition pattern is specified, files are organized directly under `{location}/{schema}/objects/`.

## Store Metadata (`dj-store-meta.json`)

Each object store contains a metadata file at its root that identifies the store and enables verification by DataJoint clients.

### Location

```
{location}/dj-store-meta.json
```

For cloud storage:
```
s3://bucket/my_project/dj-store-meta.json
```

### Content

```json
{
    "project_name": "my_project",
    "created": "2025-01-15T10:30:00Z",
    "format_version": "1.0",
    "datajoint_version": "0.15.0",
    "schemas": ["schema1", "schema2"]
}
```

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_name` | string | Yes | Unique project identifier |
| `created` | string | Yes | ISO 8601 timestamp of store creation |
| `format_version` | string | Yes | Store format version for compatibility |
| `datajoint_version` | string | Yes | DataJoint version that created the store |
| `schemas` | array | No | List of schemas using this store (updated on schema creation) |

### Store Initialization

The store metadata file is created when the first `file` attribute is used:

```
┌─────────────────────────────────────────────────────────┐
│ 1. Client attempts first file operation                 │
├─────────────────────────────────────────────────────────┤
│ 2. Check if dj-store-meta.json exists                   │
│    ├─ If exists: verify project_name matches            │
│    └─ If not: create with current project_name          │
├─────────────────────────────────────────────────────────┤
│ 3. On mismatch: raise DataJointError                    │
└─────────────────────────────────────────────────────────┘
```

### Client Verification

All DataJoint clients must use **identical `project_name`** settings to ensure store-database cohesion:

1. **On connect**: Client reads `dj-store-meta.json` from store
2. **Verify**: `project_name` in client settings matches store metadata
3. **On mismatch**: Raise `DataJointError` with descriptive message

```python
# Example error
DataJointError: Object store project name mismatch.
  Client configured: "project_a"
  Store metadata: "project_b"
  Ensure all clients use the same object_storage.project_name setting.
```

### Schema Registration

When a schema first uses the `file` type, it is added to the `schemas` list in the metadata:

```python
# After creating Recording table with file attribute in my_schema
# dj-store-meta.json is updated:
{
    "project_name": "my_project",
    "schemas": ["my_schema"]  # my_schema added
}
```

This provides a record of which schemas have data in the store.

## Syntax

```python
@schema
class Recording(dj.Manual):
    definition = """
    subject_id : int
    session_id : int
    ---
    raw_data : file          # managed file storage
    processed : file         # another file attribute
    """
```

Note: No `@store` suffix needed - storage is determined by pipeline configuration.

## Database Storage

The `file` type is stored as a `JSON` column in MySQL containing:

**File example:**
```json
{
    "path": "my_schema/objects/Recording/subject_id=123/session_id=45/raw_data/recording_Ax7bQ2kM.dat",
    "size": 12345,
    "hash": "sha256:abcdef1234...",
    "original_name": "recording.dat",
    "is_folder": false,
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

**Folder example:**
```json
{
    "path": "my_schema/objects/Recording/subject_id=123/session_id=45/raw_data/data_folder_pL9nR4wE",
    "size": 567890,
    "hash": "sha256:fedcba9876...",
    "original_name": "data_folder",
    "is_folder": true,
    "timestamp": "2025-01-15T10:30:00Z",
    "file_count": 42
}
```

### JSON Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Full path/key within storage backend (includes token) |
| `size` | integer | Yes | Total size in bytes (sum for folders) |
| `hash` | string | Yes | Content hash with algorithm prefix |
| `original_name` | string | Yes | Original file/folder name at insert time |
| `is_folder` | boolean | Yes | True if stored content is a directory |
| `timestamp` | string | Yes | ISO 8601 upload timestamp |
| `mime_type` | string | No | MIME type (files only, auto-detected or provided) |
| `file_count` | integer | No | Number of files (folders only) |

## Path Generation

Storage paths are **deterministically constructed** from record metadata, enabling bidirectional lookup between database records and stored files.

### Path Components

1. **Location** - from configuration (`object_storage.location`)
2. **Partition attributes** - promoted PK attributes (if `partition_pattern` configured)
3. **Schema name** - from the table's schema
4. **Object directory** - `objects/`
5. **Table name** - the table class name
6. **Primary key encoding** - remaining PK attributes and values
7. **Field name** - the attribute name
8. **Suffixed filename** - original name with random token suffix

### Path Template

**Without partitioning:**
```
{location}/{schema}/objects/{Table}/{pk_attr1}={pk_val1}/{pk_attr2}={pk_val2}/.../field/{basename}_{token}.{ext}
```

**With partitioning:**
```
{location}/{partition_attr}={val}/.../schema/objects/{Table}/{remaining_pk_attrs}/.../field/{basename}_{token}.{ext}
```

### Partitioning

The **partition pattern** allows promoting certain primary key attributes to the beginning of the path (after `location`). This organizes storage by high-level attributes like subject or experiment, enabling:
- Efficient data locality for related records
- Easier manual browsing of storage
- Potential for storage tiering by partition

**Configuration:**
```json
{
    "object_storage.partition_pattern": "{subject_id}/{experiment_id}"
}
```

Partition attributes are extracted from the primary key and placed at the path root. Remaining PK attributes appear in their normal position.

### Example Without Partitioning

For a table:
```python
@schema
class Recording(dj.Manual):
    definition = """
    subject_id : int
    session_id : int
    ---
    raw_data : file
    """
```

Inserting `{"subject_id": 123, "session_id": 45, "raw_data": "/path/to/recording.dat"}` produces:

```
my_project/my_schema/objects/Recording/subject_id=123/session_id=45/raw_data/recording_Ax7bQ2kM.dat
```

### Example With Partitioning

With `partition_pattern = "{subject_id}"`:

```
my_project/subject_id=123/my_schema/objects/Recording/session_id=45/raw_data/recording_Ax7bQ2kM.dat
```

The `subject_id` is promoted to the path root, grouping all files for subject 123 together regardless of schema or table.

### Deterministic Bidirectional Mapping

The path structure (excluding the random token) is fully deterministic:
- **Record → File**: Given a record's primary key, construct the path prefix to locate its file
- **File → Record**: Parse the path to extract schema, table, field, and primary key values

This enables:
- Finding all files for a specific record
- Identifying which record a file belongs to
- Auditing storage against database contents

The **random token** is stored in the JSON metadata to complete the full path.

### Primary Key Value Encoding

Primary key values are encoded directly in paths when they are simple, path-safe types:
- **Integers**: Used directly (`subject_id=123`)
- **Dates**: ISO format (`session_date=2025-01-15`)
- **Timestamps**: ISO format with safe separators (`created=2025-01-15T10-30-00`)
- **Simple strings**: Used directly if path-safe (`experiment=baseline`)

**Conversion to path-safe strings** is applied only when necessary:
- Strings containing `/`, `\`, or other path-unsafe characters
- Very long strings (truncated with hash suffix)
- Binary or complex types (hashed)

```python
# Direct encoding (no conversion needed)
subject_id=123
session_date=2025-01-15
trial_type=control

# Converted encoding (path-unsafe characters)
filename=my%2Ffile.dat          # "/" encoded
description=a1b2c3d4_abc123     # long string truncated + hash
```

### Filename Collision Avoidance

To prevent filename collisions, each stored file receives a **random token suffix** appended to its basename:

```
original: recording.dat
stored:   recording_Ax7bQ2kM.dat

original: image.analysis.tiff
stored:   image.analysis_pL9nR4wE.tiff
```

#### Token Suffix Specification

- **Alphabet**: URL-safe and filename-safe Base64 characters: `A-Z`, `a-z`, `0-9`, `-`, `_`
- **Length**: Configurable via `object_storage.token_length` (default: 8, range: 4-16)
- **Generation**: Cryptographically random using `secrets.token_urlsafe()`

At 8 characters with 64 possible values per character: 64^8 = 281 trillion combinations.

#### Rationale

- Avoids collisions without requiring existence checks
- Preserves original filename for human readability
- URL-safe for web-based access to cloud storage
- Filesystem-safe across all supported platforms

### No Deduplication

Each insert stores a separate copy of the file, even if identical content was previously stored. This ensures:
- Clear 1:1 relationship between records and files
- Simplified delete behavior
- No reference counting complexity

## Insert Behavior

At insert time, the `file` attribute accepts:

1. **File path** (string or `Path`): Path to an existing file
2. **Folder path** (string or `Path`): Path to an existing directory
3. **Stream object**: File-like object with `read()` method
4. **Tuple of (name, stream)**: Stream with explicit filename

```python
# From file path
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/recording.dat"
})

# From folder path
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/data_folder/"
})

# From stream with explicit name
with open("/local/path/data.bin", "rb") as f:
    Recording.insert1({
        "subject_id": 123,
        "session_id": 45,
        "raw_data": ("custom_name.dat", f)
    })
```

### Insert Processing Steps

1. Validate input (file/folder exists, stream is readable)
2. Generate deterministic storage path with random token
3. **Copy content to storage backend** via `fsspec`
4. **If copy fails: abort insert** (no database operation attempted)
5. Compute content hash (SHA-256)
6. Build JSON metadata structure
7. Execute database INSERT

### Copy-First Semantics

The file/folder is copied to storage **before** the database insert is attempted:
- If the copy fails, the insert does not proceed
- If the copy succeeds but the database insert fails, an orphaned file may remain
- Orphaned files are acceptable due to the random token (no collision with future inserts)

## Transaction Handling

Since storage backends don't support distributed transactions with MySQL, DataJoint uses a **copy-first** strategy.

### Insert Transaction Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Validate input and generate storage path with token  │
├─────────────────────────────────────────────────────────┤
│ 2. Copy file/folder to storage backend                  │
│    └─ On failure: raise error, INSERT not attempted     │
├─────────────────────────────────────────────────────────┤
│ 3. Compute hash and build JSON metadata                 │
├─────────────────────────────────────────────────────────┤
│ 4. Execute database INSERT                              │
│    └─ On failure: orphaned file remains (acceptable)    │
├─────────────────────────────────────────────────────────┤
│ 5. Commit database transaction                          │
│    └─ On failure: orphaned file remains (acceptable)    │
└─────────────────────────────────────────────────────────┘
```

### Failure Scenarios

| Scenario | Result | Orphaned File? |
|----------|--------|----------------|
| Copy fails | Clean failure, no INSERT | No |
| DB insert fails | Error raised | Yes (acceptable) |
| DB commit fails | Error raised | Yes (acceptable) |

### Orphaned Files

Orphaned files (files in storage without corresponding database records) may accumulate due to:
- Failed database inserts after successful copy
- Process crashes
- Network failures

**This is acceptable** because:
- Random tokens prevent collisions with future inserts
- Orphaned files can be identified by comparing storage contents with database records
- A separate cleanup procedure removes orphaned files during maintenance

### Orphan Cleanup Procedure

Orphan cleanup is a **separate maintenance operation** that must be performed during maintenance windows to avoid race conditions with concurrent inserts.

```python
# Maintenance utility methods
schema.file_storage.find_orphaned()     # List files not referenced in DB
schema.file_storage.cleanup_orphaned()  # Delete orphaned files
```

**Important considerations:**
- Should be run during low-activity periods
- Uses transactions or locking to avoid race conditions with concurrent inserts
- Files recently uploaded (within a grace period) are excluded to handle in-flight inserts
- Provides dry-run mode to preview deletions before execution

## Fetch Behavior

On fetch, the `file` type returns a **handle** (`FileRef` object) to the stored content. **The file is not copied** - all operations access the storage backend directly.

```python
record = Recording.fetch1()
file_ref = record["raw_data"]

# Access metadata (no I/O)
print(file_ref.path)           # Full storage path
print(file_ref.size)           # File size in bytes
print(file_ref.hash)           # Content hash
print(file_ref.original_name)  # Original filename
print(file_ref.is_folder)      # True if stored content is a folder

# Read content directly from storage backend
content = file_ref.read()      # Returns bytes (files only)

# Open as fsspec file object (files only)
with file_ref.open() as f:
    data = f.read()

# List contents (folders only)
contents = file_ref.listdir()  # Returns list of relative paths

# Access specific file within folder
with file_ref.open("subdir/file.dat") as f:
    data = f.read()
```

### No Automatic Download

Unlike `attach@store`, the `file` type does **not** automatically download content to a local path. Users access content directly through the `FileRef` handle, which streams from the storage backend.

For local copies, users explicitly download:

```python
# Download file to local destination
local_path = file_ref.download("/local/destination/")

# Download specific file from folder
local_path = file_ref.download("/local/destination/", "subdir/file.dat")
```

## Implementation Components

### 1. Settings Extension (`settings.py`)

New `ObjectStorageSettings` class:

```python
class ObjectStorageSettings(BaseSettings):
    """Object storage configuration for file columns."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_OBJECT_STORAGE_",
        extra="forbid",
        validate_assignment=True,
    )

    project_name: str | None = None  # Must match store metadata
    protocol: Literal["file", "s3", "gcs", "azure"] | None = None
    location: str | None = None
    bucket: str | None = None
    endpoint: str | None = None
    partition_pattern: str | None = None
    token_length: int = Field(default=8, ge=4, le=16)
    access_key: str | None = None
    secret_key: SecretStr | None = None
```

Add to main `Config` class:

```python
object_storage: ObjectStorageSettings = Field(default_factory=ObjectStorageSettings)
```

### 2. Storage Backend (`storage.py` - new module)

- `StorageBackend` class wrapping `fsspec`
- Methods: `upload()`, `download()`, `open()`, `exists()`, `delete()`
- Path generation with partition support

### 3. Type Declaration (`declare.py`)

- Add `FILE` pattern: `file$`
- Add to `SPECIAL_TYPES`
- Substitute to `JSON` type in database

### 4. Schema Integration (`schemas.py`)

- Associate storage backend with schema
- Validate storage configuration on schema creation

### 5. Insert Processing (`table.py`)

- New `__process_file_attribute()` method
- Path generation using primary key and partition pattern
- Upload via storage backend

### 6. Fetch Processing (`fetch.py`)

- New `FileRef` class
- Lazy loading from storage backend
- Metadata access interface

### 7. FileRef Class (`fileref.py` - new module)

```python
@dataclass
class FileRef:
    """Handle to a file or folder stored in the pipeline's storage backend."""

    path: str
    size: int
    hash: str
    original_name: str
    is_folder: bool
    timestamp: datetime
    mime_type: str | None      # files only
    file_count: int | None     # folders only
    _backend: StorageBackend   # internal reference

    # File operations
    def read(self) -> bytes: ...
    def open(self, subpath: str | None = None, mode: str = "rb") -> IO: ...

    # Folder operations
    def listdir(self, subpath: str = "") -> list[str]: ...
    def walk(self) -> Iterator[tuple[str, list[str], list[str]]]: ...

    # Common operations
    def download(self, destination: Path | str, subpath: str | None = None) -> Path: ...
    def exists(self, subpath: str | None = None) -> bool: ...
```

## Dependencies

New dependency: `fsspec` with optional backend-specific packages:

```toml
[project.dependencies]
fsspec = ">=2023.1.0"

[project.optional-dependencies]
s3 = ["s3fs"]
gcs = ["gcsfs"]
azure = ["adlfs"]
```

## Comparison with Existing Types

| Feature | `attach@store` | `filepath@store` | `file` |
|---------|----------------|------------------|--------|
| Store config | Per-attribute | Per-attribute | Per-pipeline |
| Path control | DataJoint | User-managed | DataJoint |
| DB column | binary(16) UUID | binary(16) UUID | JSON |
| Backend | File/S3 only | File/S3 only | fsspec (any) |
| Partitioning | Hash-based | User path | Configurable |
| Metadata | External table | External table | Inline JSON |
| Deduplication | By content | By path | None |

### Legacy Type Deprecation

The existing `attach@store` and `filepath@store` types will be:
- **Maintained** for backward compatibility with existing pipelines
- **Deprecated** in future releases with migration warnings
- **Eventually removed** after a transition period

New pipelines should use the `file` type exclusively.

## Delete Behavior

When a record with a `file` attribute is deleted:

1. **Database delete executes first** (within transaction)
2. **File delete is attempted** after successful DB commit
3. **File delete is best-effort** - the delete transaction succeeds even if file deletion fails

### Delete Transaction Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Execute database DELETE                              │
├─────────────────────────────────────────────────────────┤
│ 2. Commit database transaction                          │
│    └─ On failure: rollback, files unchanged             │
├─────────────────────────────────────────────────────────┤
│ 3. Issue delete command to storage backend              │
│    └─ On failure: log warning, transaction still OK     │
└─────────────────────────────────────────────────────────┘
```

### Stale Files

If file deletion fails (network error, permissions, etc.), **stale files** may remain in storage. This is acceptable because:
- The database record is already deleted (authoritative source)
- Random tokens prevent any collision with future inserts
- Stale files can be identified and cleaned via orphan detection utilities

### No Reference Counting

Each record owns its file exclusively. There is no deduplication or reference counting, simplifying delete logic.

## Migration Path

- Existing `attach@store` and `filepath@store` remain unchanged
- `file` type is additive - new tables only
- Future: Migration utilities to convert existing external storage

## Future Extensions

- [ ] Compression options (gzip, lz4, zstd)
- [ ] Encryption at rest
- [ ] Versioning support
- [ ] Streaming upload for large files
- [ ] Checksum verification on fetch
- [ ] Cache layer for frequently accessed files
- [ ] Parallel upload/download for large folders
