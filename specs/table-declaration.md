# DataJoint Table Declaration Specification

Version: 1.0
Status: Draft
Last Updated: 2025-01-04

## Overview

This document specifies the table declaration mechanism in DataJoint Python. Table declarations define the schema structure using a domain-specific language (DSL) embedded in Python class definitions.

## 1. Table Class Structure

### 1.1 Basic Declaration Pattern

```python
@schema
class TableName(dj.Manual):
    definition = """
    # table comment
    primary_attr : int32
    ---
    secondary_attr : float64
    """
```

### 1.2 Table Tiers

| Tier | Base Class | Table Prefix | Purpose |
|------|------------|--------------|---------|
| Manual | `dj.Manual` | (none) | User-entered data |
| Lookup | `dj.Lookup` | `#` | Reference/enumeration data |
| Imported | `dj.Imported` | `_` | Data from external sources |
| Computed | `dj.Computed` | `__` | Derived from other tables |
| Part | `dj.Part` | `master__` | Detail records of master table |

### 1.3 Class Naming Rules

- **Format**: Strict CamelCase (e.g., `MyTable`, `ProcessedData`)
- **Pattern**: `^[A-Z][A-Za-z0-9]*$`
- **Conversion**: CamelCase to snake_case for SQL table name
- **Examples**:
  - `SessionTrial` -> `session_trial`
  - `ProcessedEMG` -> `processed_emg`

### 1.4 Table Name Constraints

- **Maximum length**: 64 characters (MySQL limit)
- **Final name**: prefix + snake_case(class_name)
- **Validation**: Checked at declaration time

---

## 2. Definition String Grammar

### 2.1 Overall Structure

```
[table_comment]
primary_key_section
---
secondary_section
```

### 2.2 Table Comment (Optional)

```
# Free-form description of the table purpose
```

- Must be first non-empty line if present
- Starts with `#`
- Cannot start with `#:`
- Stored in MySQL table COMMENT

### 2.3 Primary Key Separator

```
---
```

or equivalently:

```
___
```

- Three dashes or three underscores
- Separates primary key attributes (above) from secondary attributes (below)
- Required if table has secondary attributes

### 2.4 Line Types

Each non-empty, non-comment line is one of:

1. **Attribute definition**
2. **Foreign key reference**
3. **Index declaration**

---

## 3. Attribute Definition

### 3.1 Syntax

```
attribute_name [= default_value] : type [# comment]
```

### 3.2 Components

| Component | Required | Description |
|-----------|----------|-------------|
| `attribute_name` | Yes | Identifier for the column |
| `default_value` | No | Default value (before colon) |
| `type` | Yes | Data type specification |
| `comment` | No | Documentation (after `#`) |

### 3.3 Attribute Name Rules

- **Pattern**: `^[a-z][a-z0-9_]*$`
- **Start**: Lowercase letter
- **Contains**: Lowercase letters, digits, underscores
- **Convention**: snake_case

### 3.4 Examples

```python
definition = """
# Experimental session with subject and timing info
session_id : int32                          # auto-assigned
---
subject_name : varchar(100)                 # subject identifier
trial_number = 1 : int32                    # default to 1
score = null : float32                      # nullable
timestamp = CURRENT_TIMESTAMP : datetime    # auto-timestamp
notes = '' : varchar(4000)                  # empty default
"""
```

---

## 4. Type System

### 4.1 Core Types

Scientist-friendly type names with guaranteed semantics:

| Type | SQL Mapping | Size | Description |
|------|-------------|------|-------------|
| `int8` | `tinyint` | 1 byte | 8-bit signed integer |
| `uint8` | `tinyint unsigned` | 1 byte | 8-bit unsigned integer |
| `int16` | `smallint` | 2 bytes | 16-bit signed integer |
| `uint16` | `smallint unsigned` | 2 bytes | 16-bit unsigned integer |
| `int32` | `int` | 4 bytes | 32-bit signed integer |
| `uint32` | `int unsigned` | 4 bytes | 32-bit unsigned integer |
| `int64` | `bigint` | 8 bytes | 64-bit signed integer |
| `uint64` | `bigint unsigned` | 8 bytes | 64-bit unsigned integer |
| `float32` | `float` | 4 bytes | 32-bit IEEE 754 float |
| `float64` | `double` | 8 bytes | 64-bit IEEE 754 float |
| `bool` | `tinyint` | 1 byte | Boolean (0 or 1) |
| `uuid` | `binary(16)` | 16 bytes | UUID stored as binary |
| `bytes` | `longblob` | Variable | Binary data (up to 4GB) |

### 4.2 String Types

| Type | SQL Mapping | Description |
|------|-------------|-------------|
| `char(N)` | `char(N)` | Fixed-length string |
| `varchar(N)` | `varchar(N)` | Variable-length string (max N) |
| `text` | `text` | Unlimited text |
| `enum('a','b',...)` | `enum(...)` | Enumerated values |

### 4.3 Temporal Types

| Type | SQL Mapping | Description |
|------|-------------|-------------|
| `date` | `date` | Date (YYYY-MM-DD) |
| `datetime` | `datetime` | Date and time |
| `datetime(N)` | `datetime(N)` | With fractional seconds (0-6) |

### 4.4 Other Types

| Type | SQL Mapping | Description |
|------|-------------|-------------|
| `json` | `json` | JSON document |
| `decimal(P,S)` | `decimal(P,S)` | Fixed-point decimal |

### 4.5 Native SQL Types (Passthrough)

These SQL types are accepted but generate a warning recommending core types:

- Integer variants: `tinyint`, `smallint`, `mediumint`, `bigint`, `integer`, `serial`
- Float variants: `float`, `double`, `real` (with size specifiers)
- Text variants: `tinytext`, `mediumtext`, `longtext`
- Blob variants: `tinyblob`, `smallblob`, `mediumblob`, `longblob`
- Temporal: `time`, `timestamp`, `year`
- Numeric: `numeric(P,S)`

### 4.6 Codec Types

Format: `<codec_name>` or `<codec_name@store>`

| Codec | Internal dtype | External dtype | Purpose |
|-------|---------------|----------------|---------|
| `<blob>` | `bytes` | `<hash>` | Serialized Python objects |
| `<hash>` | N/A (external only) | `json` | Hash-addressed deduped storage |
| `<attach>` | `bytes` | `<hash>` | File attachments with filename |
| `<filepath>` | N/A (external only) | `json` | Reference to managed file |
| `<object>` | N/A (external only) | `json` | Object storage (Zarr, HDF5) |

External storage syntax:
- `<blob@>` - default store
- `<blob@store_name>` - named store

### 4.7 Type Reconstruction

Core types and codecs are stored in the SQL COMMENT field for reconstruction:

```sql
COMMENT ':float32:user comment here'
COMMENT ':<blob@store>:user comment'
```

---

## 5. Default Values

### 5.1 Syntax

```
attribute_name = default_value : type
```

### 5.2 Literal Types

| Value | Meaning | SQL |
|-------|---------|-----|
| `null` | Nullable attribute | `DEFAULT NULL` |
| `CURRENT_TIMESTAMP` | Server timestamp | `DEFAULT CURRENT_TIMESTAMP` |
| `"string"` or `'string'` | String literal | `DEFAULT "string"` |
| `123` | Numeric literal | `DEFAULT 123` |
| `true`/`false` | Boolean | `DEFAULT 1`/`DEFAULT 0` |

### 5.3 Constant Literals

These values are used without quotes in SQL:
- `NULL`
- `CURRENT_TIMESTAMP`

### 5.4 Nullable Attributes

```
score = null : float32
```

- The special default `null` (case-insensitive) makes the attribute nullable
- Nullable attributes can be omitted from INSERT
- Primary key attributes CANNOT be nullable

### 5.5 Blob/JSON Default Restrictions

Blob and JSON attributes can only have `null` as default:

```python
# Valid
data = null : <blob>

# Invalid - raises DataJointError
data = '' : <blob>
```

---

## 6. Foreign Key References

### 6.1 Syntax

```
-> [options] ReferencedTable
```

### 6.2 Options

| Option | Effect |
|--------|--------|
| `nullable` | All inherited attributes become nullable |
| `unique` | Creates UNIQUE INDEX on FK attributes |

Options are comma-separated in brackets:
```
-> [nullable, unique] ParentTable
```

### 6.3 Attribute Inheritance

Foreign keys automatically inherit all primary key attributes from the referenced table:

```python
# Parent
class Subject(dj.Manual):
    definition = """
    subject_id : int32
    ---
    name : varchar(100)
    """

# Child - inherits subject_id
class Session(dj.Manual):
    definition = """
    -> Subject
    session_id : int32
    ---
    session_date : date
    """
```

### 6.4 Position Rules

| Position | Effect |
|----------|--------|
| Before `---` | FK attributes become part of primary key |
| After `---` | FK attributes are secondary (dependent) |

### 6.5 Nullable Foreign Keys

```
-> [nullable] OptionalParent
```

- Only allowed after `---` (secondary)
- Primary key FKs cannot be nullable
- Creates optional relationship

### 6.6 Unique Foreign Keys

```
-> [unique] ParentTable
```

- Creates UNIQUE INDEX on inherited attributes
- Enforces one-to-one relationship from child perspective

### 6.7 Projections in Foreign Keys

```
-> Parent.proj(alias='original_name')
```

- Reference same table multiple times with different attribute names
- Useful for self-referential or multi-reference patterns

### 6.8 Referential Actions

All foreign keys use:
- `ON UPDATE CASCADE` - Parent key changes propagate
- `ON DELETE RESTRICT` - Cannot delete parent with children

### 6.9 Lineage Tracking

Foreign key relationships are recorded in the `~lineage` table:

```python
{
    'child_attr': ('parent_schema.parent_table', 'parent_attr')
}
```

Used for semantic attribute matching in queries.

---

## 7. Index Declarations

### 7.1 Syntax

```
index(attr1, attr2, ...)
unique index(attr1, attr2, ...)
```

### 7.2 Examples

```python
definition = """
# User contact information
user_id : int32
---
first_name : varchar(50)
last_name : varchar(50)
email : varchar(100)
index(last_name, first_name)
unique index(email)
"""
```

### 7.3 Computed Expressions

Indexes can include SQL expressions:

```
index(last_name, (YEAR(birth_date)))
```

### 7.4 Limitations

- Cannot be altered after table creation (via `table.alter()`)
- Must reference existing attributes

---

## 8. Part Tables

### 8.1 Declaration

```python
@schema
class Master(dj.Manual):
    definition = """
    master_id : int32
    """

    class Detail(dj.Part):
        definition = """
        -> master
        detail_id : int32
        ---
        value : float32
        """
```

### 8.2 Naming

- SQL name: `master_table__part_name`
- Example: `experiment__trial`

### 8.3 Master Reference

Within Part definition, use:
- `-> master` (lowercase keyword)
- `-> MasterClassName` (class name)

### 8.4 Constraints

- Parts must reference their master
- Cannot delete Part records directly (use master)
- Cannot drop Part table directly (use master)
- Part inherits master's primary key

---

## 9. Auto-Populated Tables

### 9.1 Classes

- `dj.Imported` - Data from external sources
- `dj.Computed` - Derived from other DataJoint tables

### 9.2 Primary Key Constraint

All primary key attributes must come from foreign key references.

**Valid:**
```python
class Analysis(dj.Computed):
    definition = """
    -> Session
    -> Parameter
    ---
    result : float64
    """
```

**Invalid** (by default):
```python
class Analysis(dj.Computed):
    definition = """
    -> Session
    analysis_id : int32    # ERROR: non-FK primary key
    ---
    result : float64
    """
```

**Override:**
```python
dj.config['jobs.allow_new_pk_fields_in_computed_tables'] = True
```

### 9.3 Job Metadata

When `config['jobs.add_job_metadata'] = True`, auto-populated tables receive:

| Column | Type | Description |
|--------|------|-------------|
| `_job_start_time` | `datetime(3)` | Job start timestamp |
| `_job_duration` | `float64` | Duration in seconds |
| `_job_version` | `varchar(64)` | Code version |

---

## 10. Validation

### 10.1 Parse-Time Checks

| Check | Error |
|-------|-------|
| Unknown type | `DataJointError: Unsupported attribute type` |
| Invalid attribute name | `DataJointError: Declaration error` |
| Comment starts with `:` | `DataJointError: comment must not start with colon` |
| Non-null blob default | `DataJointError: default value for blob can only be NULL` |

### 10.2 Declaration-Time Checks

| Check | Error |
|-------|-------|
| Table name > 64 chars | `DataJointError: Table name exceeds max length` |
| No primary key | `DataJointError: Table must have a primary key` |
| Nullable primary key attr | `DataJointError: Primary key attributes cannot be nullable` |
| Invalid CamelCase | `DataJointError: Invalid table name` |
| FK resolution failure | `DataJointError: Foreign key reference could not be resolved` |

### 10.3 Insert-Time Validation

The `table.validate()` method checks:
- Required fields present
- NULL constraints satisfied
- Primary key completeness
- Codec validation (if defined)
- UUID format
- JSON serializability

---

## 11. SQL Generation

### 11.1 CREATE TABLE Template

```sql
CREATE TABLE `schema`.`table_name` (
    `attr1` TYPE1 NOT NULL COMMENT "...",
    `attr2` TYPE2 DEFAULT NULL COMMENT "...",
    PRIMARY KEY (`pk1`, `pk2`),
    FOREIGN KEY (`fk_attr`) REFERENCES `parent` (`pk`)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    INDEX (`idx_attr`),
    UNIQUE INDEX (`uniq_attr`)
) ENGINE=InnoDB COMMENT="table comment"
```

### 11.2 Type Comment Encoding

Core types and codecs are preserved in comments:

```sql
`value` float NOT NULL COMMENT ":float32:measurement value"
`data` longblob DEFAULT NULL COMMENT ":<blob>:serialized data"
`archive` json DEFAULT NULL COMMENT ":<blob@cold>:external storage"
```

---

## 12. Implementation Files

| File | Purpose |
|------|---------|
| `declare.py` | Definition parsing, SQL generation |
| `heading.py` | Attribute metadata, type reconstruction |
| `table.py` | Base Table class, declaration interface |
| `user_tables.py` | Tier classes (Manual, Computed, etc.) |
| `schemas.py` | Schema binding, table decoration |
| `codecs.py` | Codec registry and resolution |
| `lineage.py` | Attribute lineage tracking |

---

## 13. Future Considerations

Potential improvements identified for the declaration system:

1. **Better error messages** with suggestions and context
2. **Import-time validation** via `__init_subclass__`
3. **Parser alternatives** (regex-based for simpler grammar)
4. **SQL dialect abstraction** for multi-database support
5. **Extended constraints** (CHECK, custom validation)
6. **Migration support** for schema evolution
7. **Definition caching** for performance
8. **IDE tooling** support via structured intermediate representation
