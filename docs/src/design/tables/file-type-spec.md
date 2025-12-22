# Object Column Type Specification

## Overview

The `object` type introduces a new paradigm for managed file storage in DataJoint. Unlike existing `attach@store` and `filepath@store` types that reference named stores, the `object` type uses a **unified storage backend** that is tightly coupled with the schema and configured at the pipeline level.

The `object` type supports both **files and folders**. Content is copied to storage at insert time, referenced via handle on fetch, and deleted when the record is deleted.

### Immutability Contract

Objects stored via the `object` type are **immutable after finalization**. Users agree to:
- **Insert (copy)**: Copy existing content to storage
- **Insert (staged)**: Reserve path, write directly, then finalize
- **Fetch**: Read content via handle (no modification)
- **Delete**: Remove content when record is deleted (only way to remove)

Once an object is **finalized** (either via copy-insert or staged-insert completion), users must not directly modify it in the object store.

#### Two Insert Modes

| Mode | Use Case | Workflow |
|------|----------|----------|
| **Copy** | Small files, existing data | Local file → copy to storage → insert record |
| **Staged** | Large objects, Zarr/HDF5 | Reserve path → write directly to storage → finalize record |

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
├── datajoint_store.json         # store metadata (not client config)
├── 📁 schema_name/
│   ├── 📁 Table1/
│   │   ├── data.parquet         # tabular data export (future)
│   │   └── 📁 objects/          # object storage for this table
│   │       ├── pk1=val1/pk2=val2/field1_token.dat
│   │       └── pk1=val1/pk2=val2/field2_token.zarr
│   ├── 📁 Table2/
│   │   ├── data.parquet
│   │   └── 📁 objects/
│   │       └── ...
```

### Object Storage Keys

When using cloud object storage:

```
s3://bucket/project_name/schema_name/Table1/objects/pk1=val1/field_token.dat
s3://bucket/project_name/schema_name/Table1/objects/pk1=val1/field_token.zarr
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
s3://my-bucket/my_project/subject_id=123/session_id=45/schema_name/Recording/objects/raw_data_Ax7bQ2kM.dat
```

If no partition pattern is specified, files are organized directly under `{location}/{schema}/{Table}/objects/`.

## Store Metadata (`datajoint_store.json`)

Each object store contains a metadata file at its root that identifies the store and enables verification by DataJoint clients. This file is named `datajoint_store.json` to distinguish it from client configuration files (`datajoint.json`).

### Location

```
{location}/datajoint_store.json
```

For cloud storage:
```
s3://bucket/my_project/datajoint_store.json
```

### Content

```json
{
    "project_name": "my_project",
    "created": "2025-01-15T10:30:00Z",
    "format_version": "1.0",
    "datajoint_version": "0.15.0",
    "database_host": "db.example.com",
    "database_name": "my_project_db"
}
```

### Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `project_name` | string | Yes | Unique project identifier |
| `created` | string | Yes | ISO 8601 timestamp of store creation |
| `format_version` | string | Yes | Store format version for compatibility |
| `datajoint_version` | string | Yes | DataJoint version that created the store |
| `database_host` | string | No | Database server hostname (for bidirectional mapping) |
| `database_name` | string | No | Database name (for bidirectional mapping) |

The optional `database_host` and `database_name` fields enable bidirectional mapping between object stores and databases. This is informational only - not enforced at runtime. Administrators can alternatively ensure unique `project_name` values across their namespace, and managed platforms may handle this mapping externally.

### Store Initialization

The store metadata file is created when the first `object` attribute is used:

```
┌─────────────────────────────────────────────────────────┐
│ 1. Client attempts first file operation                 │
├─────────────────────────────────────────────────────────┤
│ 2. Check if datajoint_store.json exists                 │
│    ├─ If exists: verify project_name matches            │
│    └─ If not: create with current project_name          │
├─────────────────────────────────────────────────────────┤
│ 3. On mismatch: raise DataJointError                    │
└─────────────────────────────────────────────────────────┘
```

### Client Verification

DataJoint performs a basic verification on connect to ensure store-database cohesion:

1. **On connect**: Client reads `datajoint_store.json` from store
2. **Verify**: `project_name` in client settings matches store metadata
3. **On mismatch**: Raise `DataJointError` with descriptive message

```python
# Example error
DataJointError: Object store project name mismatch.
  Client configured: "project_a"
  Store metadata: "project_b"
  Ensure all clients use the same object_storage.project_name setting.
```

### Administrative Responsibility

A 1:1 correspondence is assumed between:
- Database location + `project_name` in client settings
- Object store + `project_name` in store metadata

DataJoint performs basic verification but does **not** enforce this mapping. Administrators are responsible for ensuring correct configuration across all clients.

## Syntax

```python
@schema
class Recording(dj.Manual):
    definition = """
    subject_id : int
    session_id : int
    ---
    raw_data : object          # managed file storage
    processed : object         # another object attribute
    """
```

Note: No `@store` suffix needed - storage is determined by pipeline configuration.

## Database Storage

The `object` type is stored as a `JSON` column in MySQL containing:

**File example:**
```json
{
    "path": "my_schema/Recording/objects/subject_id=123/session_id=45/raw_data_Ax7bQ2kM.dat",
    "size": 12345,
    "hash": null,
    "ext": ".dat",
    "is_dir": false,
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

**File with optional hash:**
```json
{
    "path": "my_schema/Recording/objects/subject_id=123/session_id=45/raw_data_Ax7bQ2kM.dat",
    "size": 12345,
    "hash": "sha256:abcdef1234...",
    "ext": ".dat",
    "is_dir": false,
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

**Folder example:**
```json
{
    "path": "my_schema/Recording/objects/subject_id=123/session_id=45/raw_data_pL9nR4wE",
    "size": 567890,
    "hash": null,
    "ext": null,
    "is_dir": true,
    "timestamp": "2025-01-15T10:30:00Z",
    "item_count": 42
}
```

**Zarr example (large dataset, metadata fields omitted for performance):**
```json
{
    "path": "my_schema/Recording/objects/subject_id=123/session_id=45/neural_data_kM3nP2qR.zarr",
    "size": null,
    "hash": null,
    "ext": ".zarr",
    "is_dir": true,
    "timestamp": "2025-01-15T10:30:00Z"
}
```

### JSON Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Full path/key within storage backend (includes token) |
| `size` | integer/null | No | Total size in bytes (sum for folders), or null if not computed. See [Performance Considerations](#performance-considerations). |
| `hash` | string/null | Yes | Content hash with algorithm prefix, or null (default) |
| `ext` | string/null | Yes | File extension as tooling hint (e.g., `.dat`, `.zarr`) or null. See [Extension Field](#extension-field). |
| `is_dir` | boolean | Yes | True if stored content is a directory/key-prefix (e.g., Zarr store) |
| `timestamp` | string | Yes | ISO 8601 upload timestamp |
| `mime_type` | string | No | MIME type (files only, auto-detected from extension) |
| `item_count` | integer | No | Number of files (folders only), or null if not computed. See [Performance Considerations](#performance-considerations). |

### Extension Field

The `ext` field is a **tooling hint** that preserves the original file extension or provides a conventional suffix for directory-based formats. It is:

- **Not a content-type declaration**: Unlike `mime_type`, it does not attempt to describe the internal content format
- **Useful for tooling**: Enables file browsers, IDEs, and other tools to display appropriate icons or suggest applications
- **Conventional for formats like Zarr**: The `.zarr` extension is recognized by the ecosystem even though a Zarr store contains mixed content (JSON metadata + binary chunks)

For single files, `ext` is extracted from the source filename. For staged inserts (like Zarr), it can be explicitly provided.

### Performance Considerations

For large hierarchical data like Zarr stores, computing certain metadata can be expensive:

- **`size`**: Requires listing all objects and summing their sizes. For stores with millions of chunks, this can take minutes or hours.
- **`item_count`**: Requires listing all objects. Same performance concern as `size`.
- **`hash`**: Requires reading all content. Explicitly not supported for staged inserts.

**These fields are optional** and default to `null` for staged inserts. Users can explicitly request computation when needed, understanding the performance implications.

### Content Hashing

By default, **no content hash is computed** to avoid performance overhead for large objects. Storage backend integrity is trusted.

**Optional hashing** can be requested per-insert:

```python
# Default - no hash (fast)
Recording.insert1({..., "raw_data": "/path/to/large.dat"})

# Request hash computation
Recording.insert1({..., "raw_data": "/path/to/important.dat"}, hash="sha256")
```

Supported hash algorithms: `sha256`, `md5`, `xxhash` (xxh3, faster for large files)

**Staged inserts never compute hashes** - data is written directly to storage without a local copy to hash.

### Folder Manifests

For folders (directories), a **manifest file** is created alongside the folder in the object store to enable integrity verification without computing content hashes:

```
raw_data_pL9nR4wE/
raw_data_pL9nR4wE.manifest.json
```

**Manifest content:**
```json
{
    "files": [
        {"path": "file1.dat", "size": 1234},
        {"path": "subdir/file2.dat", "size": 5678},
        {"path": "subdir/file3.dat", "size": 91011}
    ],
    "total_size": 567890,
    "item_count": 42,
    "created": "2025-01-15T10:30:00Z"
}
```

**Design rationale:**
- Stored in object store (not database) to avoid bloating the JSON for folders with many files
- Placed alongside folder (not inside) to avoid polluting folder contents and interfering with tools like Zarr
- Enables self-contained verification without database access

The manifest enables:
- Quick verification that all expected files exist
- Size validation without reading file contents
- Detection of missing or extra files

### Filename Convention

The stored filename is **always derived from the field name**:
- **Base name**: The attribute/field name (e.g., `raw_data`)
- **Extension**: Adopted from source file (copy insert) or optionally provided (staged insert)
- **Token**: Random suffix for collision avoidance

```
Stored filename = {field}_{token}{ext}

Examples:
  raw_data_Ax7bQ2kM.dat     # file with .dat extension
  raw_data_pL9nR4wE.zarr    # Zarr directory with .zarr extension
  raw_data_kM3nP2qR         # directory without extension
```

This convention ensures:
- Consistent, predictable naming across all objects
- Field name visible in storage for easier debugging
- Extension preserved for MIME type detection and tooling compatibility

## Path Generation

Storage paths are **deterministically constructed** from record metadata, enabling bidirectional lookup between database records and stored files.

### Path Components

1. **Location** - from configuration (`object_storage.location`)
2. **Partition attributes** - promoted PK attributes (if `partition_pattern` configured)
3. **Schema name** - from the table's schema
4. **Table name** - the table class name
5. **Object directory** - `objects/`
6. **Primary key encoding** - remaining PK attributes and values
7. **Suffixed filename** - `{field}_{token}{ext}`

### Path Template

**Without partitioning:**
```
{location}/{schema}/{Table}/objects/{pk_attr1}={pk_val1}/{pk_attr2}={pk_val2}/.../{field}_{token}{ext}
```

**With partitioning:**
```
{location}/{partition_attr}={val}/.../schema/{Table}/objects/{remaining_pk_attrs}/.../{field}_{token}{ext}
```

Note: The `objects/` directory follows the table name, allowing each table folder to also contain tabular data exports (e.g., `data.parquet`) alongside the objects.

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
    raw_data : object
    """
```

Inserting `{"subject_id": 123, "session_id": 45, "raw_data": "/path/to/recording.dat"}` produces:

```
my_project/my_schema/Recording/objects/subject_id=123/session_id=45/raw_data_Ax7bQ2kM.dat
```

Note: The filename is `raw_data` (field name) with `.dat` extension (from source file).

### Example With Partitioning

With `partition_pattern = "{subject_id}"`:

```
my_project/subject_id=123/my_schema/Recording/objects/session_id=45/raw_data_Ax7bQ2kM.dat
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

To prevent filename collisions, each stored object receives a **random token suffix** appended to the field name:

```
field: raw_data, source: recording.dat
stored: raw_data_Ax7bQ2kM.dat

field: image, source: scan.tiff
stored: image_pL9nR4wE.tiff

field: neural_data (staged with .zarr)
stored: neural_data_kM3nP2qR.zarr
```

#### Token Suffix Specification

- **Alphabet**: URL-safe and filename-safe Base64 characters: `A-Z`, `a-z`, `0-9`, `-`, `_`
- **Length**: Configurable via `object_storage.token_length` (default: 8, range: 4-16)
- **Generation**: Cryptographically random using `secrets.token_urlsafe()`

At 8 characters with 64 possible values per character: 64^8 = 281 trillion combinations.

#### Rationale

- Avoids collisions without requiring existence checks
- Field name visible in storage for easier debugging/auditing
- URL-safe for web-based access to cloud storage
- Filesystem-safe across all supported platforms

### No Deduplication

Each insert stores a separate copy of the file, even if identical content was previously stored. This ensures:
- Clear 1:1 relationship between records and files
- Simplified delete behavior
- No reference counting complexity

## Insert Behavior

At insert time, the `object` attribute accepts:

1. **File path** (string or `Path`): Path to an existing file (extension extracted)
2. **Folder path** (string or `Path`): Path to an existing directory
3. **Tuple of (ext, stream)**: File-like object with explicit extension

```python
# From file path - extension (.dat) extracted from source
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/recording.dat"
})
# Stored as: raw_data_Ax7bQ2kM.dat

# From folder path - no extension
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/data_folder/"
})
# Stored as: raw_data_pL9nR4wE/

# From stream with explicit extension
with open("/local/path/data.bin", "rb") as f:
    Recording.insert1({
        "subject_id": 123,
        "session_id": 45,
        "raw_data": (".bin", f)
    })
# Stored as: raw_data_kM3nP2qR.bin
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

### Staged Insert (Direct Write Mode)

For large objects like Zarr arrays, copying from local storage is inefficient. **Staged insert** allows writing directly to the destination.

#### Why a Separate Method?

Staged insert uses a dedicated `staged_insert1` method rather than co-opting `insert1` because:

1. **Explicit over implicit** - Staged inserts have fundamentally different semantics (file creation happens during context, commit on exit). A separate method makes this explicit.
2. **Backward compatibility** - `insert1` returns `None` and doesn't support context manager protocol. Changing this could break existing code.
3. **Clear error handling** - The context manager semantics (success = commit, exception = rollback) are obvious with `staged_insert1`.
4. **Type safety** - The staged context exposes `.store()` for object fields. A dedicated method can return a properly-typed `StagedInsert` object.

**Staged inserts are limited to `insert1`** (one row at a time). Multi-row inserts are not supported for staged operations.

#### Basic Usage

```python
# Stage an insert with direct object storage writes
with Recording.staged_insert1 as staged:
    # Set primary key values
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # Create object storage directly using store()
    # Extension is optional - .zarr is conventional for Zarr arrays
    z = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(10000, 10000), dtype='f4')
    z[:] = compute_large_array()

    # Assign the created object to the record
    staged.rec['raw_data'] = z

# On successful exit: metadata computed, record inserted
# On exception: storage cleaned up, no record inserted
# Stored as: raw_data_Ax7bQ2kM.zarr
```

#### StagedInsert Interface

```python
class StagedInsert:
    """Context manager for staged insert operations."""

    rec: dict[str, Any]  # Record dict for setting attribute values

    def store(self, field: str, ext: str = "") -> fsspec.FSMap:
        """
        Get an FSMap store for direct writes to an object field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".zarr", ".hdf5")

        Returns:
            fsspec.FSMap suitable for Zarr/xarray
        """
        ...

    def open(self, field: str, ext: str = "", mode: str = "wb") -> IO:
        """
        Open a file for direct writes to an object field.

        Args:
            field: Name of the object attribute
            ext: Optional extension (e.g., ".bin", ".dat")
            mode: File mode (default: "wb")

        Returns:
            File-like object for writing
        """
        ...

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return fsspec filesystem for advanced operations."""
        ...
```

#### Staged Insert Flow

```
┌─────────────────────────────────────────────────────────┐
│ 1. Enter context: create StagedInsert with empty rec    │
├─────────────────────────────────────────────────────────┤
│ 2. User sets primary key values in staged.rec           │
├─────────────────────────────────────────────────────────┤
│ 3. User calls store()/open() to get storage handles     │
│    - Path reserved with random token on first call      │
│    - User writes data directly via fsspec               │
├─────────────────────────────────────────────────────────┤
│ 4. User assigns object references to staged.rec         │
├─────────────────────────────────────────────────────────┤
│ 5. On context exit (success):                           │
│    - Compute metadata (size, hash, item_count)          │
│    - Execute database INSERT                            │
├─────────────────────────────────────────────────────────┤
│ 6. On context exit (exception):                         │
│    - Delete any written data                            │
│    - Re-raise exception                                 │
└─────────────────────────────────────────────────────────┘
```

#### Zarr Example

```python
import zarr
import numpy as np

# Create a large Zarr array directly in object storage
with Recording.staged_insert1 as staged:
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # Create Zarr hierarchy directly in object storage
    # .zarr extension is optional but conventional
    root = zarr.open(staged.store('neural_data', '.zarr'), mode='w')
    root.create_dataset('timestamps', data=np.arange(1000000))
    root.create_dataset('waveforms', shape=(1000000, 82), chunks=(10000, 82))

    # Write in chunks (streaming from acquisition)
    for i, chunk in enumerate(data_stream):
        root['waveforms'][i*10000:(i+1)*10000] = chunk

    # Assign to record
    staged.rec['neural_data'] = root

# Record automatically inserted with computed metadata
# Stored as: neural_data_kM3nP2qR.zarr
```

#### Multiple Object Fields

```python
with Recording.staged_insert1 as staged:
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # Write multiple object fields - extension optional
    raw = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(1000, 1000))
    raw[:] = raw_array

    processed = zarr.open(staged.store('processed', '.zarr'), mode='w', shape=(100, 100))
    processed[:] = processed_array

    staged.rec['raw_data'] = raw
    staged.rec['processed'] = processed

# Stored as: raw_data_Ax7bQ2kM.zarr, processed_pL9nR4wE.zarr
```

#### Comparison: Copy vs Staged Insert

| Aspect | Copy Insert | Staged Insert |
|--------|-------------|---------------|
| Data location | Must exist locally first | Written directly to storage |
| Efficiency | Copy overhead | No copy needed |
| Use case | Small files, existing data | Large arrays, streaming data |
| Cleanup on failure | Orphan possible | Cleaned up |
| API | `insert1({..., "field": path})` | `staged_insert1` context manager |
| Multi-row | Supported | Not supported (insert1 only) |

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

On fetch, the `object` type returns a **handle** (`ObjectRef` object) to the stored content. **The file is not copied** - all operations access the storage backend directly.

```python
record = Recording.fetch1()
file_ref = record["raw_data"]

# Access metadata (no I/O)
print(file_ref.path)           # Full storage path
print(file_ref.size)           # File size in bytes
print(file_ref.hash)           # Content hash (if computed) or None
print(file_ref.ext)            # File extension (e.g., ".dat") or None
print(file_ref.is_dir)         # True if stored content is a folder

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

Unlike `attach@store`, the `object` type does **not** automatically download content to a local path. Users access content directly through the `ObjectRef` handle, which streams from the storage backend.

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
    """Object storage configuration for object columns."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_OBJECT_STORAGE_",
        extra="forbid",
        validate_assignment=True,
    )

    project_name: str | None = None  # Must match store metadata
    protocol: Literal["object", "s3", "gcs", "azure"] | None = None
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

- Add `OBJECT` pattern: `object$`
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

- New `ObjectRef` class
- Lazy loading from storage backend
- Metadata access interface

### 7. ObjectRef Class (`objectref.py` - new module)

```python
@dataclass
class ObjectRef:
    """Handle to a file or folder stored in the pipeline's storage backend."""

    path: str
    size: int
    hash: str | None           # content hash (if computed) or None
    ext: str | None            # file extension (e.g., ".dat") or None
    is_dir: bool
    timestamp: datetime
    mime_type: str | None      # files only, derived from ext
    item_count: int | None     # folders only
    _backend: StorageBackend   # internal reference

    # fsspec access (for Zarr, xarray, etc.)
    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Return fsspec filesystem for direct access."""
        ...

    @property
    def store(self) -> fsspec.FSMap:
        """Return FSMap suitable for Zarr/xarray."""
        ...

    @property
    def full_path(self) -> str:
        """Return full URI (e.g., 's3://bucket/path')."""
        ...

    # File operations
    def read(self) -> bytes: ...
    def open(self, subpath: str | None = None, mode: str = "rb") -> IO: ...

    # Folder operations
    def listdir(self, subpath: str = "") -> list[str]: ...
    def walk(self) -> Iterator[tuple[str, list[str], list[str]]]: ...

    # Common operations
    def download(self, destination: Path | str, subpath: str | None = None) -> Path: ...
    def exists(self, subpath: str | None = None) -> bool: ...

    # Integrity verification
    def verify(self) -> bool:
        """
        Verify object integrity.

        For files: checks size matches, and hash if available.
        For folders: validates manifest (all files exist with correct sizes).

        Returns True if valid, raises IntegrityError with details if not.
        """
        ...
```

#### fsspec Integration

The `ObjectRef` provides direct fsspec access for integration with array libraries:

```python
import zarr
import xarray as xr

record = Recording.fetch1()
obj_ref = record["raw_data"]

# Direct Zarr access
z = zarr.open(obj_ref.store, mode='r')
print(z.shape)

# Direct xarray access
ds = xr.open_zarr(obj_ref.store)

# Use fsspec filesystem directly
fs = obj_ref.fs
files = fs.ls(obj_ref.full_path)
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

### Storage Access Architecture

The `object` type separates **data declaration** (the JSON metadata stored in the database) from **storage access** (the library used to read/write objects):

- **Data declaration**: The JSON schema (path, size, hash, etc.) is a pure data structure with no library dependencies
- **Storage access**: Currently uses `fsspec` as the default accessor, but the architecture supports alternative backends

**Why this matters**: While `fsspec` is a mature and widely-used library, alternatives like [`obstore`](https://github.com/developmentseed/obstore) offer performance advantages for certain workloads. By keeping the data model independent of the access library, future versions can support pluggable storage accessors without schema changes.

**Current implementation**: The `ObjectRef` class provides fsspec-based accessors (`fs`, `store` properties). Future versions may add:
- Pluggable accessor interface
- Alternative backends (obstore, custom implementations)
- Backend selection per-operation or per-configuration

## Comparison with Existing Types

| Feature | `attach@store` | `filepath@store` | `object` |
|---------|----------------|------------------|--------|
| Store config | Per-attribute | Per-attribute | Per-pipeline |
| Path control | DataJoint | User-managed | DataJoint |
| DB column | binary(16) UUID | binary(16) UUID | JSON |
| Hidden tables | Yes (external) | Yes (external) | **No** |
| Backend | File/S3 only | File/S3 only | fsspec (any) |
| Partitioning | Hash-based | User path | Configurable |
| Metadata storage | External table | External table | Inline JSON |
| Deduplication | By content | By path | None |

### No Hidden Tables

A key architectural difference: the `object` type does **not** use hidden external tables.

The legacy `attach@store` and `filepath@store` types store a UUID in the table column and maintain a separate hidden `~external_*` table containing:
- File paths/keys
- Checksums
- Size information
- Reference counts

The `object` type eliminates this complexity by storing all metadata **inline** in the JSON column. This provides:
- **Simpler schema** - no hidden tables to manage or migrate
- **Self-contained records** - all information in one place
- **Easier debugging** - metadata visible directly in queries
- **No reference counting** - each record owns its object exclusively

### Legacy Type Deprecation

The existing `attach@store` and `filepath@store` types will be:
- **Maintained** for backward compatibility with existing pipelines
- **Deprecated** in future releases with migration warnings
- **Eventually removed** after a transition period

New pipelines should use the `object` type exclusively.

## Delete Behavior

When a record with a `object` attribute is deleted:

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
- `object` type is additive - new tables only
- Future: Migration utilities to convert existing external storage

## Zarr and Large Hierarchical Data

The `object` type is designed with Zarr and similar hierarchical data formats (HDF5 via kerchunk, TileDB) in mind. This section provides guidance for these use cases.

### Recommended Workflow

For large Zarr stores, use **staged insert** to write directly to object storage:

```python
import zarr
import numpy as np

with Recording.staged_insert1 as staged:
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # Write Zarr directly to object storage
    store = staged.store('neural_data', '.zarr')
    root = zarr.open(store, mode='w')
    root.create_dataset('spikes', shape=(1000000, 384), chunks=(10000, 384), dtype='f4')

    # Stream data without local intermediate copy
    for i, chunk in enumerate(acquisition_stream):
        root['spikes'][i*10000:(i+1)*10000] = chunk

    staged.rec['neural_data'] = root

# Metadata recorded, no expensive size/hash computation
```

### JSON Metadata for Zarr

For Zarr stores, the recommended JSON metadata omits expensive-to-compute fields:

```json
{
    "path": "schema/Recording/objects/subject_id=123/session_id=45/neural_data_kM3nP2qR.zarr",
    "size": null,
    "hash": null,
    "ext": ".zarr",
    "is_dir": true,
    "timestamp": "2025-01-15T10:30:00Z"
}
```

**Field notes for Zarr:**
- **`size`**: Set to `null` - computing total size requires listing all chunks
- **`hash`**: Always `null` for staged inserts - no merkle tree support currently
- **`ext`**: Set to `.zarr` as a conventional tooling hint
- **`is_dir`**: Set to `true` - Zarr stores are key prefixes (logical directories)
- **`item_count`**: Omitted - counting chunks is expensive and rarely useful
- **`mime_type`**: Omitted - Zarr contains mixed content types

### Reading Zarr Data

The `ObjectRef` provides direct access compatible with Zarr and xarray:

```python
record = Recording.fetch1()
obj_ref = record['neural_data']

# Direct Zarr access
z = zarr.open(obj_ref.store, mode='r')
print(z['spikes'].shape)

# xarray integration
ds = xr.open_zarr(obj_ref.store)

# Dask integration (lazy loading)
import dask.array as da
arr = da.from_zarr(obj_ref.store, component='spikes')
```

### Performance Tips

1. **Use chunked writes**: Write data in chunks that match your Zarr chunk size
2. **Avoid metadata computation**: Let `size` and `item_count` default to `null`
3. **Use appropriate chunk sizes**: Balance between too many small files (overhead) and too few large files (memory)
4. **Consider compression**: Configure Zarr compression (blosc, zstd) to reduce storage costs

## Future Extensions

- [ ] Compression options (gzip, lz4, zstd)
- [ ] Encryption at rest
- [ ] Versioning support
- [ ] Streaming upload for large files
- [ ] Checksum verification on fetch
- [ ] Cache layer for frequently accessed files
- [ ] Parallel upload/download for large folders
