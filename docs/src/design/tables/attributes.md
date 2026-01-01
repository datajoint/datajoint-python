# Datatypes

DataJoint supports the following datatypes.
To conserve database resources, use the smallest and most restrictive datatype
sufficient for your data.
This also ensures that only valid data are entered into the pipeline.

## Core datatypes (recommended)

Use these portable, scientist-friendly types for cross-database compatibility.

### Integers

-  `int8`: 8-bit signed integer (-128 to 127)
-  `uint8`: 8-bit unsigned integer (0 to 255)
-  `int16`: 16-bit signed integer (-32,768 to 32,767)
-  `uint16`: 16-bit unsigned integer (0 to 65,535)
-  `int32`: 32-bit signed integer
-  `uint32`: 32-bit unsigned integer
-  `int64`: 64-bit signed integer
-  `uint64`: 64-bit unsigned integer
-  `bool`: boolean value (True/False, stored as 0/1)

### Floating-point

-  `float32`: 32-bit single-precision floating-point. Sufficient for many measurements.
-  `float64`: 64-bit double-precision floating-point.
   Avoid using floating-point types in primary keys due to equality comparison issues.
-  `decimal(n,f)`: fixed-point number with *n* total digits and *f* fractional digits.
   Use for exact decimal representation (e.g., currency, coordinates).
   Safe for primary keys due to well-defined precision.

### Strings

-  `char(n)`: fixed-length string of exactly *n* characters.
-  `varchar(n)`: variable-length string up to *n* characters.
-  `text`: unlimited-length text for long-form content (notes, descriptions, abstracts).
-  `enum(...)`: one of several enumerated values, e.g., `enum("low", "medium", "high")`.
   Do not use enums in primary keys due to difficulty changing definitions.

**Encoding policy:** All strings use UTF-8 encoding (`utf8mb4` in MySQL, `UTF8` in PostgreSQL).
Character encoding and collation are database-level configuration, not part of type definitions.
Comparisons are case-sensitive by default.

### Date/Time

-  `date`: date as `'YYYY-MM-DD'`.
-  `datetime`: date and time as `'YYYY-MM-DD HH:MM:SS'`.
   Use `CURRENT_TIMESTAMP` as default for auto-populated timestamps.

**Timezone policy:** All `datetime` values should be stored as **UTC**. Timezone
conversion is a presentation concern handled by the application layer. This ensures
reproducible computations regardless of server location or timezone settings.

### Binary

-  `bytes`: raw binary data (up to 4 GiB). Stores and returns raw bytes without
   serialization. For serialized Python objects (arrays, dicts, etc.), use `<blob>`.

### Other

-  `uuid`: 128-bit universally unique identifier.
-  `json`: JSON document for structured data.

## Native datatypes (advanced)

Native database types are available for advanced use cases but are **not recommended**
for portable pipelines. Using native types will generate a warning.

-  `tinyint`, `smallint`, `int`, `bigint` (with optional `unsigned`)
-  `float`, `double`, `real`
-  `tinyblob`, `blob`, `mediumblob`, `longblob`
-  `tinytext`, `mediumtext`, `longtext` (size variants)
-  `time`, `timestamp`, `year`
-  `mediumint`, `serial`, `int auto_increment`

See the [storage types spec](storage-types-spec.md) for complete mappings.

## Codec types (special datatypes)

Codecs provide `encode()`/`decode()` semantics for complex data that doesn't
fit native database types. They are denoted with angle brackets: `<name>`.

### Storage mode: `@` convention

The `@` character indicates **external storage** (object store vs database):

- **No `@`**: Internal storage (database) - e.g., `<blob>`, `<attach>`
- **`@` present**: External storage (object store) - e.g., `<blob@>`, `<attach@store>`
- **`@` alone**: Use default store - e.g., `<blob@>`
- **`@name`**: Use named store - e.g., `<blob@cold>`

### Built-in codecs

**Serialization types** - for Python objects:

- `<blob>`: DataJoint's native serialization format for Python objects. Supports
  NumPy arrays, dicts, lists, datetime objects, and nested structures. Stores in
  database. Compatible with MATLAB. See [custom types](customtype.md) for details.

- `<blob@>` / `<blob@store>`: Like `<blob>` but stores externally with hash-
  addressed deduplication. Use for large arrays that may be duplicated across rows.

**File storage types** - for managed files:

- `<object@>` / `<object@store>`: Managed file and folder storage with path derived
  from primary key. Supports Zarr, HDF5, and direct writes via fsspec. Returns
  `ObjectRef` for lazy access. External only. See [object storage](object.md).

- `<hash@>` / `<hash@store>`: Hash-addressed storage for raw bytes with
  MD5 deduplication. External only. Use via `<blob@>` or `<attach@>` rather than directly.

**File attachment types** - for file transfer:

- `<attach>`: File attachment stored in database with filename preserved. Similar
  to email attachments. Good for small files (<16MB). See [attachments](attach.md).

- `<attach@>` / `<attach@store>`: Like `<attach>` but stores externally with
  deduplication. Use for large files.

**File reference types** - for external files:

- `<filepath@store>`: Reference to existing file in a configured store. No file
  copying occurs. Returns `ObjectRef` for lazy access. External only. See [filepath](filepath.md).

### User-defined codecs

- `<custom_type>`: Define your own [custom codec](customtype.md) with
  bidirectional conversion between Python objects and database storage. Use for
  graphs, domain-specific objects, or custom data structures.

## Core type aliases

DataJoint provides convenient type aliases that map to standard database types.
These aliases use familiar naming conventions from NumPy and other numerical computing
libraries, making table definitions more readable and portable across database backends.

| Alias | MySQL | PostgreSQL | Description |
|-------|-------|------------|-------------|
| `bool` | `TINYINT` | `BOOLEAN` | Boolean value (0 or 1) |
| `int8` | `TINYINT` | `SMALLINT` | 8-bit signed integer (-128 to 127) |
| `uint8` | `TINYINT UNSIGNED` | `SMALLINT` | 8-bit unsigned integer (0 to 255) |
| `int16` | `SMALLINT` | `SMALLINT` | 16-bit signed integer |
| `uint16` | `SMALLINT UNSIGNED` | `INTEGER` | 16-bit unsigned integer |
| `int32` | `INT` | `INTEGER` | 32-bit signed integer |
| `uint32` | `INT UNSIGNED` | `BIGINT` | 32-bit unsigned integer |
| `int64` | `BIGINT` | `BIGINT` | 64-bit signed integer |
| `uint64` | `BIGINT UNSIGNED` | `NUMERIC(20)` | 64-bit unsigned integer |
| `float32` | `FLOAT` | `REAL` | 32-bit single-precision float |
| `float64` | `DOUBLE` | `DOUBLE PRECISION` | 64-bit double-precision float |
| `bytes` | `LONGBLOB` | `BYTEA` | Raw binary data |

Example usage:

```python
@schema
class Measurement(dj.Manual):
    definition = """
    measurement_id : int32
    ---
    temperature : float32       # single-precision temperature reading
    precise_value : float64     # double-precision measurement
    sample_count : uint32       # unsigned 32-bit counter
    sensor_flags : uint8        # 8-bit status flags
    is_valid : bool             # boolean flag
    raw_data : bytes            # raw binary data
    processed : <blob>          # serialized Python object
    large_array : <blob@>       # external storage with deduplication
    """
```

## Datatypes not (yet) supported

-  `binary(n)` / `varbinary(n)` - use `bytes` instead
-  `bit(n)` - use `int` types with bitwise operations
-  `set(...)` - use `json` for multiple selections

For additional information about these datatypes, see
http://dev.mysql.com/doc/refman/5.6/en/data-types.html
