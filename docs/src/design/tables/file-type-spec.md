# File Column Type Specification

## Overview

The `file` type introduces a new paradigm for managed file storage in DataJoint. Unlike existing `attach@store` and `filepath@store` types that reference named stores, the `file` type uses a **unified storage backend** that is tightly coupled with the schema and configured at the pipeline level.

## Storage Architecture

### Single Storage Backend Per Pipeline

Each DataJoint pipeline has **one** associated storage backend configured in `datajoint.toml`. DataJoint fully controls the path structure within this backend.

### Supported Backends

DataJoint uses **[`fsspec`](https://filesystem-spec.readthedocs.io/en/latest/)** to ensure compatibility across multiple storage backends:

- **Local storage** – POSIX-compliant file systems (e.g., NFS, SMB)
- **Cloud-based object storage** – Amazon S3, Google Cloud Storage, Azure Blob, MinIO
- **Hybrid storage** – Combining local and cloud storage for flexibility

## Project Structure

A DataJoint project creates a structured hierarchical storage pattern:

```
📁 project_name/
├── datajoint.toml
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

### `datajoint.toml` Structure

```toml
[project]
name = "my_project"

[storage]
backend = "s3"  # or "file", "gcs", "azure"
bucket = "my-bucket"
# For local: path = "/data/my_project"

[storage.credentials]
# Backend-specific credentials (or reference to secrets manager)

[object_storage]
partition_pattern = "subject{subject_id}/session{session_id}"
```

### Partition Pattern

The organizational structure of stored objects is configurable, allowing partitioning based on **primary key attributes**.

```toml
[object_storage]
partition_pattern = "subject{subject_id}/session{session_id}"
```

Placeholders `{subject_id}` and `{session_id}` are dynamically replaced with actual primary key values.

**Example with partitioning:**

```
s3://my-bucket/project_name/subject123/session45/schema_name3/objects/table1/key1-value1/image1.tiff
s3://my-bucket/project_name/subject123/session45/schema_name3/objects/table2/key2-value2/movie2.zarr
```

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
    "path": "subject123/session45/schema_name/objects/Recording-raw_data/...",
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

1. **Project name** - from configuration
2. **Partition values** - from primary key (if configured)
3. **Schema name** - from the table's schema
4. **Object directory** - `objects/`
5. **Table-field identifier** - `{table_name}-{field_name}/`
6. **Key identifier** - derived from primary key values
7. **Original filename** - preserved from insert

Example path construction:

```
{project}/{partition}/{schema}/objects/{table}-{field}/{key_hash}/{original_name}
```

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

1. Resolve storage backend from schema's pipeline configuration
2. Read file content (from path or stream)
3. Compute content hash (SHA-256)
4. Generate storage path using partition pattern and primary key
5. Upload file to storage backend via `fsspec`
6. Build JSON metadata structure
7. Store JSON in database column

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

### 1. Storage Backend (`storage.py` - new module)

- `StorageBackend` class wrapping `fsspec`
- Methods: `upload()`, `download()`, `open()`, `exists()`, `delete()`
- Path generation with partition support
- Configuration loading from `datajoint.toml`

### 2. Type Declaration (`declare.py`)

- Add `FILE` pattern: `file$`
- Add to `SPECIAL_TYPES`
- Substitute to `JSON` type in database

### 3. Schema Integration (`schemas.py`)

- Associate storage backend with schema
- Load configuration on schema creation

### 4. Insert Processing (`table.py`)

- New `__process_file_attribute()` method
- Path generation using primary key and partition pattern
- Upload via storage backend

### 5. Fetch Processing (`fetch.py`)

- New `FileRef` class
- Lazy loading from storage backend
- Metadata access interface

### 6. FileRef Class (`fileref.py` - new module)

```python
class FileRef:
    """Reference to a file stored in the pipeline's storage backend."""

    path: str
    size: int
    hash: str
    original_name: str
    timestamp: datetime
    mime_type: str | None

    def read(self) -> bytes: ...
    def open(self, mode="rb") -> IO: ...
    def download(self, destination: Path) -> Path: ...
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
| Backend | File/S3 | File/S3 | fsspec (any) |
| Partitioning | Hash-based | User path | Configurable |
| Metadata | External table | External table | Inline JSON |

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
