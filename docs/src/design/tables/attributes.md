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
   serialization. For serialized Python objects (arrays, dicts, etc.), use `<djblob>`.

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

## Special DataJoint-only datatypes

These types abstract certain kinds of non-database data to facilitate use
together with DataJoint.

- `<djblob>`: DataJoint's native serialization format for Python objects. Supports
NumPy arrays, dicts, lists, datetime objects, and nested structures. Compatible with
MATLAB. See [custom types](customtype.md) for details.

- `object`: managed [file and folder storage](object.md) with support for direct writes
(Zarr, HDF5) and fsspec integration. Recommended for new pipelines.

- `attach`: a [file attachment](attach.md) similar to email attachments facillitating
sending/receiving an opaque data file to/from a DataJoint pipeline.

- `filepath@store`: a [filepath](filepath.md) used to link non-DataJoint managed files
into a DataJoint pipeline.

- `<custom_type>`: a [custom attribute type](customtype.md) that defines bidirectional
conversion between Python objects and database storage formats. Use this to store
complex data types like graphs, domain-specific objects, or custom data structures.

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
    measurement_id : int
    ---
    temperature : float32       # single-precision temperature reading
    precise_value : float64     # double-precision measurement
    sample_count : uint32       # unsigned 32-bit counter
    sensor_flags : uint8        # 8-bit status flags
    is_valid : bool             # boolean flag
    raw_data : bytes            # raw binary data
    """
```

## Datatypes not (yet) supported

-  `binary(n)` / `varbinary(n)` - use `bytes` instead
-  `bit(n)` - use `int` types with bitwise operations
-  `set(...)` - use `json` for multiple selections

For additional information about these datatypes, see
http://dev.mysql.com/doc/refman/5.6/en/data-types.html
