# File Column Type Specification

## Overview

The `file` type introduces a new paradigm for managed file storage in DataJoint. Unlike existing `attach@store` and `filepath@store` types that reference named stores, the `file` type uses a **unified storage backend** that is tightly coupled with the schema and configured at the pipeline level.

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

    "object_storage.protocol": "s3",
    "object_storage.endpoint": "s3.amazonaws.com",
    "object_storage.bucket": "my-bucket",
    "object_storage.location": "my_project",
    "object_storage.partition_pattern": "subject{subject_id}/session{session_id}"
}
```

For local filesystem storage:

```json
{
    "object_storage.protocol": "file",
    "object_storage.location": "/data/my_project",
    "object_storage.partition_pattern": "subject{subject_id}/session{session_id}"
}
```

### Settings Schema

| Setting | Type | Required | Description |
|---------|------|----------|-------------|
| `object_storage.protocol` | string | Yes | Storage backend: `file`, `s3`, `gcs`, `azure` |
| `object_storage.location` | string | Yes | Base path or bucket prefix |
| `object_storage.bucket` | string | For cloud | Bucket name (S3, GCS, Azure) |
| `object_storage.endpoint` | string | For S3 | S3 endpoint URL |
| `object_storage.partition_pattern` | string | No | Path pattern with `{attribute}` placeholders |
| `object_storage.hash_length` | int | No | Random suffix length for filenames (default: 8, range: 4-16) |
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

```json
{
    "path": "subject123/session45/schema_name/objects/Recording-raw_data/recording_Ax7bQ2kM.dat",
    "size": 12345,
    "hash": "sha256:abcdef1234...",
    "original_name": "recording.dat",
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

### JSON Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Full path/key within storage backend |
| `size` | integer | Yes | File size in bytes |
| `hash` | string | Yes | Content hash with algorithm prefix |
| `original_name` | string | Yes | Original filename at insert time |
| `timestamp` | string | Yes | ISO 8601 upload timestamp |
| `mime_type` | string | No | MIME type (auto-detected or provided) |

## Path Generation

DataJoint generates storage paths using:

1. **Location** - from configuration (`object_storage.location`)
2. **Partition values** - from primary key (if `partition_pattern` configured)
3. **Schema name** - from the table's schema
4. **Object directory** - `objects/`
5. **Table-field identifier** - `{TableName}-{field_name}/`
6. **Suffixed filename** - original name with random hash suffix

Example path construction:

```
{location}/{partition}/{schema}/objects/{Table}-{field}/{basename}_{hash}.{ext}
```

### Filename Collision Avoidance

To prevent filename collisions, each stored file receives a **random hash suffix** appended to its basename:

```
original: recording.dat
stored:   recording_Ax7bQ2kM.dat

original: image.analysis.tiff
stored:   image.analysis_pL9nR4wE.tiff
```

#### Hash Suffix Specification

- **Alphabet**: URL-safe and filename-safe Base64 characters: `A-Z`, `a-z`, `0-9`, `-`, `_`
- **Length**: Configurable via `object_storage.hash_length` (default: 8, range: 4-16)
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
2. **Stream object**: File-like object with `read()` method
3. **Tuple of (name, stream)**: Stream with explicit filename

```python
# From file path
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/recording.dat"
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

1. Resolve storage backend from pipeline configuration
2. Read file content (from path or stream)
3. Compute content hash (SHA-256)
4. Generate storage path with random suffix
5. Upload file to storage backend via `fsspec`
6. Build JSON metadata structure
7. Store JSON in database column

## Transaction Handling

File uploads and database inserts must be coordinated to maintain consistency. Since storage backends don't support distributed transactions with MySQL, DataJoint uses a **upload-first** strategy with cleanup on failure.

### Insert Transaction Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Validate input and generate storage path             │
├─────────────────────────────────────────────────────────┤
│ 2. Upload file to storage backend                       │
│    └─ On failure: raise error (nothing to clean up)     │
├─────────────────────────────────────────────────────────┤
│ 3. Build JSON metadata with storage path                │
├─────────────────────────────────────────────────────────┤
│ 4. Execute database INSERT                              │
│    └─ On failure: delete uploaded file, raise error     │
├─────────────────────────────────────────────────────────┤
│ 5. Commit database transaction                          │
│    └─ On failure: delete uploaded file, raise error     │
└─────────────────────────────────────────────────────────┘
```

### Failure Scenarios

| Scenario | State Before | Recovery Action | Result |
|----------|--------------|-----------------|--------|
| Upload fails | No file, no record | None needed | Clean failure |
| DB insert fails | File exists, no record | Delete file | Clean failure |
| DB commit fails | File exists, no record | Delete file | Clean failure |
| Cleanup fails | File exists, no record | Log warning | Orphaned file |

### Orphaned File Handling

In rare cases (e.g., process crash, network failure during cleanup), orphaned files may remain in storage. These can be identified and cleaned:

```python
# Future utility method
schema.external_storage.find_orphaned()  # List files not referenced in DB
schema.external_storage.cleanup_orphaned()  # Delete orphaned files
```

### Batch Insert Handling

For batch inserts with multiple `file` attributes:

1. Upload all files first (collect paths)
2. Execute batch INSERT with all metadata
3. On any failure: delete all uploaded files from this batch

This ensures atomicity at the batch level - either all records are inserted with their files, or none are.

## Fetch Behavior

On fetch, the `file` type returns a `FileRef` object:

```python
record = Recording.fetch1()
file_ref = record["raw_data"]

# Access metadata
print(file_ref.path)           # Full storage path
print(file_ref.size)           # File size in bytes
print(file_ref.hash)           # Content hash
print(file_ref.original_name)  # Original filename

# Read content directly (streams from backend)
content = file_ref.read()      # Returns bytes

# Download to local path
local_path = file_ref.download("/local/destination/")

# Open as fsspec file object
with file_ref.open() as f:
    data = f.read()
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

    protocol: Literal["file", "s3", "gcs", "azure"] | None = None
    location: str | None = None
    bucket: str | None = None
    endpoint: str | None = None
    partition_pattern: str | None = None
    hash_length: int = Field(default=8, ge=4, le=16)
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
    """Reference to a file stored in the pipeline's storage backend."""

    path: str
    size: int
    hash: str
    original_name: str
    timestamp: datetime
    mime_type: str | None
    _backend: StorageBackend  # internal reference

    def read(self) -> bytes: ...
    def open(self, mode: str = "rb") -> IO: ...
    def download(self, destination: Path | str) -> Path: ...
    def exists(self) -> bool: ...
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

## Delete Behavior

When a record with a `file` attribute is deleted:
- The corresponding file in storage is also deleted
- No reference counting (each record owns its file)

## Migration Path

- Existing `attach@store` and `filepath@store` remain unchanged
- `file` type is additive - new tables only
- Future: Migration utilities to convert existing external storage

## Future Extensions

- [ ] Directory/folder support (store entire directories)
- [ ] Compression options (gzip, lz4, zstd)
- [ ] Encryption at rest
- [ ] Versioning support
- [ ] Streaming upload for large files
- [ ] Checksum verification options
- [ ] Cache layer for frequently accessed files
