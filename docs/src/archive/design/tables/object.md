# Object Type

The `object` type provides managed file and folder storage for DataJoint pipelines. Unlike `attach@store` and `filepath@store` which reference named stores, the `object` type uses a unified storage backend configured at the pipeline level.

## Overview

The `object` type supports both files and folders:

- **Files**: Copied to storage at insert time, accessed via handle on fetch
- **Folders**: Entire directory trees stored as a unit (e.g., Zarr arrays)
- **Staged inserts**: Write directly to storage for large objects

### Key Features

- **Unified storage**: One storage backend per pipeline (local filesystem or cloud)
- **No hidden tables**: Metadata stored inline as JSON (simpler than `attach@store`)
- **fsspec integration**: Direct access for Zarr, xarray, and other array libraries
- **Immutable objects**: Content cannot be modified after insert

## Configuration

Configure object storage in `datajoint.json`:

```json
{
    "object_storage": {
        "project_name": "my_project",
        "protocol": "s3",
        "bucket": "my-bucket",
        "location": "my_project",
        "endpoint": "s3.amazonaws.com"
    }
}
```

For local filesystem storage:

```json
{
    "object_storage": {
        "project_name": "my_project",
        "protocol": "file",
        "location": "/data/my_project"
    }
}
```

### Configuration Options

| Setting | Required | Description |
|---------|----------|-------------|
| `project_name` | Yes | Unique project identifier |
| `protocol` | Yes | Storage backend: `file`, `s3`, `gcs`, `azure` |
| `location` | Yes | Base path or bucket prefix |
| `bucket` | For cloud | Bucket name (S3, GCS, Azure) |
| `endpoint` | For S3 | S3 endpoint URL |
| `partition_pattern` | No | Path pattern with `{attribute}` placeholders |
| `token_length` | No | Random suffix length (default: 8, range: 4-16) |

### Environment Variables

Settings can be overridden via environment variables:

```bash
DJ_OBJECT_STORAGE_PROTOCOL=s3
DJ_OBJECT_STORAGE_BUCKET=my-bucket
DJ_OBJECT_STORAGE_LOCATION=my_project
```

## Table Definition

Define an object attribute in your table:

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

Note: No `@store` suffix needed—storage is determined by pipeline configuration.

## Insert Operations

### Inserting Files

Insert a file by providing its local path:

```python
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/recording.dat"
})
```

The file is copied to object storage and the path is stored as JSON metadata.

### Inserting Folders

Insert an entire directory:

```python
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "/local/path/to/data_folder/"
})
```

### Inserting from Remote URLs

Insert from cloud storage or HTTP sources—content is copied to managed storage:

```python
# From S3
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "s3://source-bucket/path/to/data.dat"
})

# From Google Cloud Storage (e.g., collaborator data)
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "neural_data": "gs://collaborator-bucket/shared/experiment.zarr"
})

# From HTTP/HTTPS
Recording.insert1({
    "subject_id": 123,
    "session_id": 45,
    "raw_data": "https://example.com/public/data.dat"
})
```

Supported protocols: `s3://`, `gs://`, `az://`, `http://`, `https://`

Remote sources may require credentials configured via environment variables or fsspec configuration files.

### Inserting from Streams

Insert from a file-like object with explicit extension:

```python
with open("/local/path/data.bin", "rb") as f:
    Recording.insert1({
        "subject_id": 123,
        "session_id": 45,
        "raw_data": (".bin", f)
    })
```

### Staged Insert (Direct Write)

For large objects like Zarr arrays, use staged insert to write directly to storage without a local copy:

```python
import zarr

with Recording.staged_insert1 as staged:
    # Set primary key values first
    staged.rec['subject_id'] = 123
    staged.rec['session_id'] = 45

    # Create Zarr array directly in object storage
    z = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(10000, 10000))
    z[:] = compute_large_array()

    # Assign to record
    staged.rec['raw_data'] = z

# On successful exit: metadata computed, record inserted
# On exception: storage cleaned up, no record inserted
```

The `staged_insert1` context manager provides:

- `staged.rec`: Dict for setting attribute values
- `staged.store(field, ext)`: Returns `fsspec.FSMap` for Zarr/xarray
- `staged.open(field, ext, mode)`: Returns file handle for writing
- `staged.fs`: Direct fsspec filesystem access

## Fetch Operations

Fetching an object attribute returns an `ObjectRef` handle:

```python
record = Recording.fetch1()
obj = record["raw_data"]

# Access metadata (no I/O)
print(obj.path)      # Storage path
print(obj.size)      # Size in bytes
print(obj.ext)       # File extension (e.g., ".dat")
print(obj.is_dir)    # True if folder
```

### Reading File Content

```python
# Read entire file as bytes
content = obj.read()

# Open as file object
with obj.open() as f:
    data = f.read()
```

### Working with Folders

```python
# List contents
contents = obj.listdir()

# Walk directory tree
for root, dirs, files in obj.walk():
    print(root, files)

# Open specific file in folder
with obj.open("subdir/file.dat") as f:
    data = f.read()
```

### Downloading Files

Download to local filesystem:

```python
# Download entire object
local_path = obj.download("/local/destination/")

# Download specific file from folder
local_path = obj.download("/local/destination/", "subdir/file.dat")
```

### Integration with Zarr and xarray

The `ObjectRef` provides direct fsspec access:

```python
import zarr
import xarray as xr

record = Recording.fetch1()
obj = record["raw_data"]

# Open as Zarr array
z = zarr.open(obj.store, mode='r')
print(z.shape)

# Open with xarray
ds = xr.open_zarr(obj.store)

# Access fsspec filesystem directly
fs = obj.fs
files = fs.ls(obj.full_path)
```

### Verifying Integrity

Verify that stored content matches metadata:

```python
try:
    obj.verify()
    print("Object integrity verified")
except IntegrityError as e:
    print(f"Verification failed: {e}")
```

For files, this checks size (and hash if available). For folders, it validates the manifest.

## Storage Structure

Objects are stored with a deterministic path structure:

```
{location}/{schema}/{Table}/objects/{pk_attrs}/{field}_{token}{ext}
```

Example:
```
my_project/my_schema/Recording/objects/subject_id=123/session_id=45/raw_data_Ax7bQ2kM.dat
```

### Partitioning

Use `partition_pattern` to organize files by attributes:

```json
{
    "object_storage": {
        "partition_pattern": "{subject_id}/{session_id}"
    }
}
```

This promotes specified attributes to the path root for better organization:

```
my_project/subject_id=123/session_id=45/my_schema/Recording/objects/raw_data_Ax7bQ2kM.dat
```

## Database Storage

The `object` type is stored as a JSON column containing metadata:

```json
{
    "path": "my_schema/Recording/objects/subject_id=123/raw_data_Ax7bQ2kM.dat",
    "size": 12345,
    "hash": null,
    "ext": ".dat",
    "is_dir": false,
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

For folders, the metadata includes `item_count` and a manifest file is stored alongside the folder in object storage.

## Comparison with Other Types

| Feature | `attach@store` | `filepath@store` | `object` |
|---------|----------------|------------------|----------|
| Store config | Per-attribute | Per-attribute | Per-pipeline |
| Path control | DataJoint | User-managed | DataJoint |
| Hidden tables | Yes | Yes | **No** |
| Backend | File/S3 only | File/S3 only | fsspec (any) |
| Metadata storage | External table | External table | Inline JSON |
| Folder support | No | No | **Yes** |
| Direct write | No | No | **Yes** |

## Delete Behavior

When a record is deleted:

1. Database record is deleted first (within transaction)
2. Storage file/folder deletion is attempted after commit
3. File deletion failures are logged but don't fail the transaction

Orphaned files (from failed deletes or crashed inserts) can be cleaned up using maintenance utilities.

## Best Practices

1. **Use staged insert for large objects**: Avoid copying multi-gigabyte files through local storage
2. **Set primary keys before calling `store()`**: The storage path depends on primary key values
3. **Use meaningful extensions**: Extensions like `.zarr`, `.hdf5` help identify content type
4. **Verify after critical inserts**: Call `obj.verify()` for important data
5. **Configure partitioning for large datasets**: Improves storage organization and browsing
