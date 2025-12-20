# File Column Type Specification

## Overview

The `file` type is a new DataJoint column data type that provides managed file storage with metadata tracking. Unlike existing attachment types, `file` stores structured metadata as JSON while managing file storage in a configurable location.

## Syntax

```python
@schema
class MyTable(dj.Manual):
    definition = """
    id : int
    ---
    data_file : file@store    # managed file with metadata
    """
```

## Database Storage

The `file` type is stored as a `JSON` column in MySQL. The JSON structure contains:

```json
{
    "path": "relative/path/to/file.ext",
    "size": 12345,
    "hash": "sha256:abcdef1234...",
    "original_name": "original_filename.ext",
    "timestamp": "2025-01-15T10:30:00Z",
    "mime_type": "application/octet-stream"
}
```

### JSON Schema

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `path` | string | Yes | Relative path within the store |
| `size` | integer | Yes | File size in bytes |
| `hash` | string | Yes | Content hash with algorithm prefix |
| `original_name` | string | Yes | Original filename at insert time |
| `timestamp` | string | Yes | ISO 8601 upload timestamp |
| `mime_type` | string | No | MIME type (auto-detected or provided) |

## Insert Behavior

At insert time, the `file` attribute accepts:

1. **File path (string or Path)**: Path to an existing file
2. **Stream object**: File-like object with `read()` method
3. **Tuple of (name, stream)**: Stream with explicit filename

### Insert Flow

```python
# From file path
table.insert1({"id": 1, "data_file": "/path/to/file.dat"})
table.insert1({"id": 2, "data_file": Path("/path/to/file.dat")})

# From stream
with open("/path/to/file.dat", "rb") as f:
    table.insert1({"id": 3, "data_file": f})

# From stream with explicit name
with open("/path/to/file.dat", "rb") as f:
    table.insert1({"id": 4, "data_file": ("custom_name.dat", f)})
```

### Processing Steps

1. Read file content (from path or stream)
2. Compute content hash (SHA-256)
3. Generate storage path using hash-based subfolding
4. Copy file to target location in store
5. Build JSON metadata structure
6. Store JSON in database column

## Fetch Behavior

On fetch, the `file` type returns a `FileRef` object (or configurable to return the path string directly).

```python
# Fetch returns FileRef object
record = table.fetch1()
file_ref = record["data_file"]

# Access metadata
print(file_ref.path)           # Full path to file
print(file_ref.size)           # File size
print(file_ref.hash)           # Content hash
print(file_ref.original_name)  # Original filename

# Read content
content = file_ref.read()      # Returns bytes

# Get as path
path = file_ref.as_path()      # Returns Path object
```

### Fetch Options

```python
# Return path strings instead of FileRef objects
records = table.fetch(download_path="/local/path", format="path")

# Return raw JSON metadata
records = table.fetch(format="metadata")
```

## Store Configuration

The `file` type uses the existing external store infrastructure:

```python
dj.config["stores"] = {
    "raw": {
        "protocol": "file",
        "location": "/data/raw-files",
        "subfolding": (2, 2),  # Hash-based directory structure
    },
    "s3store": {
        "protocol": "s3",
        "endpoint": "s3.amazonaws.com",
        "bucket": "my-bucket",
        "location": "datajoint-files",
        "access_key": "...",
        "secret_key": "...",
    }
}
```

## Comparison with Existing Types

| Feature | `attach` | `filepath` | `file` |
|---------|----------|------------|--------|
| Storage | External store | External store | External store |
| DB Column | binary(16) UUID | binary(16) UUID | JSON |
| Metadata | Limited | Path + hash | Full structured |
| Deduplication | By content | By path | By content |
| Fetch returns | Downloaded path | Staged path | FileRef object |
| Track history | No | Via hash | Yes (in JSON) |

## Implementation Components

### 1. Type Declaration (`declare.py`)

- Add `FILE` pattern: `file@(?P<store>[a-z][\-\w]*)$`
- Add to `SPECIAL_TYPES`
- Substitute to `JSON` type in database

### 2. Insert Processing (`table.py`)

- New `__process_file_attribute()` method
- Handle file path, stream, and (name, stream) inputs
- Copy to store and build metadata JSON

### 3. Fetch Processing (`fetch.py`)

- New `FileRef` class for return values
- Optional download/staging behavior
- Metadata access interface

### 4. Heading Support (`heading.py`)

- Track `is_file` attribute flag
- Store detection from comment

## Error Handling

| Scenario | Behavior |
|----------|----------|
| File not found | Raise `DataJointError` at insert |
| Stream not readable | Raise `DataJointError` at insert |
| Store not configured | Raise `DataJointError` at insert |
| File missing on fetch | Raise `DataJointError` with metadata |
| Hash mismatch on fetch | Warning + option to re-download |

## Migration Considerations

- No migration needed - new type, new tables only
- Existing `attach@store` and `filepath@store` unchanged
- Can coexist in same schema

## Future Extensions

- [ ] Compression options (gzip, lz4)
- [ ] Encryption at rest
- [ ] Versioning support
- [ ] Lazy loading / streaming fetch
- [ ] Checksum verification options
