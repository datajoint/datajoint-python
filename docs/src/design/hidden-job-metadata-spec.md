# Hidden Job Metadata in Computed Tables

## Overview

Job execution metadata (start time, duration, code version) should be persisted in computed tables themselves, not just in ephemeral job entries. This is accomplished using hidden attributes.

## Motivation

The current job table (`~~table_name`) tracks execution metadata, but:
1. Job entries are deleted after completion (unless `keep_completed=True`)
2. Users often need to know when and with what code version each row was computed
3. This metadata should be transparent - not cluttering the user-facing schema

Hidden attributes (prefixed with `_`) provide the solution: stored in the database but filtered from user-facing APIs.

## Hidden Job Metadata Attributes

| Attribute | Type | Description |
|-----------|------|-------------|
| `_job_start_time` | datetime(3) | When computation began |
| `_job_duration` | float32 | Computation duration in seconds |
| `_job_version` | varchar(64) | Code version (e.g., git commit hash) |

**Design notes:**
- `_job_duration` (elapsed time) rather than `_job_completed_time` because duration is more informative for performance analysis
- `varchar(64)` for version is sufficient for git hashes (40 chars for SHA-1, 7-8 for short hash)
- `datetime(3)` provides millisecond precision

## Configuration

### Settings Structure

Job metadata is controlled via `config.jobs` settings:

```python
class JobsSettings(BaseSettings):
    """Job queue configuration for AutoPopulate 2.0."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_JOBS_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    # Existing settings
    auto_refresh: bool = Field(default=True, ...)
    keep_completed: bool = Field(default=False, ...)
    stale_timeout: int = Field(default=3600, ...)
    default_priority: int = Field(default=5, ...)
    version_method: Literal["git", "none"] | None = Field(default=None, ...)
    allow_new_pk_fields_in_computed_tables: bool = Field(default=False, ...)

    # New setting for hidden job metadata
    add_job_metadata: bool = Field(
        default=False,
        description="Add hidden job metadata attributes (_job_start_time, _job_duration, _job_version) "
        "to Computed and Imported tables during declaration. Tables created without this setting "
        "will not receive metadata updates during populate."
    )
```

### Access Patterns

```python
import datajoint as dj

# Read setting
dj.config.jobs.add_job_metadata  # False (default)

# Enable programmatically
dj.config.jobs.add_job_metadata = True

# Enable via environment variable
# DJ_JOBS_ADD_JOB_METADATA=true

# Enable in config file (dj_config.yaml)
# jobs:
#   add_job_metadata: true

# Temporary override
with dj.config.override(jobs={"add_job_metadata": True}):
    schema(MyComputedTable)  # Declared with metadata columns
```

### Setting Interactions

| Setting | Effect on Job Metadata |
|---------|----------------------|
| `add_job_metadata=True` | New Computed/Imported tables get hidden metadata columns |
| `add_job_metadata=False` | Tables declared without metadata columns (default) |
| `version_method="git"` | `_job_version` populated with git short hash |
| `version_method="none"` | `_job_version` left empty |
| `version_method=None` | `_job_version` left empty (same as "none") |

### Behavior at Declaration vs Populate

| `add_job_metadata` at declare | `add_job_metadata` at populate | Result |
|------------------------------|-------------------------------|--------|
| True | True | Metadata columns created and populated |
| True | False | Metadata columns exist but not populated |
| False | True | No metadata columns, populate skips silently |
| False | False | No metadata columns, normal behavior |

### Retrofitting Existing Tables

Tables created before enabling `add_job_metadata` do not have the hidden metadata columns.
To add metadata columns to existing tables, use the migration utility (not automatic):

```python
from datajoint.migrate import add_job_metadata_columns

# Add hidden metadata columns to specific table
add_job_metadata_columns(MyComputedTable)

# Add to all Computed/Imported tables in a schema
add_job_metadata_columns(schema)
```

This utility:
- ALTERs the table to add the three hidden columns
- Does NOT populate existing rows (metadata remains NULL)
- Future `populate()` calls will populate metadata for new rows

## Behavior

### Declaration-time

When `config.jobs.add_job_metadata=True` and a Computed/Imported table is declared:
- Hidden metadata columns are added to the table definition
- Only master tables receive metadata columns; Part tables never get them

### Population-time

After `make()` completes successfully:
1. Check if the table has hidden metadata columns
2. If yes: UPDATE the just-inserted rows with start_time, duration, version
3. If no: Silently skip (no error, no ALTER)

This applies to both:
- **Direct mode** (`reserve_jobs=False`): Single-process populate
- **Distributed mode** (`reserve_jobs=True`): Multi-worker with job table coordination

## Excluding Hidden Attributes from Binary Operators

### Problem Statement

If two tables have hidden attributes with the same name (e.g., both have `_job_start_time`), SQL's NATURAL JOIN would incorrectly match on them:

```sql
-- NATURAL JOIN matches ALL common attributes including hidden
SELECT * FROM table_a NATURAL JOIN table_b
-- Would incorrectly match on _job_start_time!
```

### Solution: Replace NATURAL JOIN with USING Clause

Hidden attributes must be excluded from all binary operator considerations. The result of a join does not preserve hidden attributes from its operands.

**Current implementation:**
```python
def from_clause(self):
    clause = next(support)
    for s, left in zip(support, self._left):
        clause += " NATURAL{left} JOIN {clause}".format(...)
```

**Proposed implementation:**
```python
def from_clause(self):
    clause = next(support)
    for s, (left, using_attrs) in zip(support, self._joins):
        if using_attrs:
            using = "USING ({})".format(", ".join(f"`{a}`" for a in using_attrs))
            clause += " {left}JOIN {s} {using}".format(
                left="LEFT " if left else "",
                s=s,
                using=using
            )
        else:
            # Cross join (no common non-hidden attributes)
            clause += " CROSS JOIN " + s if not left else " LEFT JOIN " + s + " ON TRUE"
    return clause
```

### Changes Required

#### 1. `QueryExpression._left` → `QueryExpression._joins`

Replace `_left: List[bool]` with `_joins: List[Tuple[bool, List[str]]]`

Each join stores:
- `left`: Whether it's a left join
- `using_attrs`: Non-hidden common attributes to join on

```python
# Before
result._left = self._left + [left] + other._left

# After
join_attributes = [n for n in self.heading.names if n in other.heading.names]
result._joins = self._joins + [(left, join_attributes)] + other._joins
```

#### 2. `heading.names` (existing behavior)

Already filters out hidden attributes:
```python
@property
def names(self):
    return [k for k in self.attributes]  # attributes excludes is_hidden=True
```

This ensures join attribute computation automatically excludes hidden attributes.

### Behavior Summary

| Scenario | Hidden Attributes | Result |
|----------|-------------------|--------|
| `A * B` (join) | Same hidden attr in both | NOT matched - excluded from USING |
| `A & B` (semijoin) | Same hidden attr in both | NOT matched |
| `A - B` (antijoin) | Same hidden attr in both | NOT matched |
| `A.proj()` | Hidden attrs in A | NOT projected (unless explicitly named) |
| `A.fetch()` | Hidden attrs in A | NOT returned by default |

## Implementation Details

### 1. Declaration (declare.py)

```python
def declare(full_table_name, definition, context):
    # ... existing code ...

    # Add hidden job metadata for auto-populated tables
    if config.jobs.add_job_metadata and table_tier in (TableTier.COMPUTED, TableTier.IMPORTED):
        # Only for master tables, not parts
        if not is_part_table:
            job_metadata_sql = [
                "`_job_start_time` datetime(3) DEFAULT NULL",
                "`_job_duration` float DEFAULT NULL",
                "`_job_version` varchar(64) DEFAULT ''",
            ]
            attribute_sql.extend(job_metadata_sql)
```

### 2. Population (autopopulate.py)

```python
def _populate1(self, key, callback, use_jobs, jobs):
    start_time = datetime.now()
    version = _get_job_version()

    # ... call make() ...

    duration = time.time() - start_time.timestamp()

    # Update job metadata if table has the hidden attributes
    if self._has_job_metadata_attrs():
        self._update_job_metadata(
            key,
            start_time=start_time,
            duration=duration,
            version=version
        )

def _has_job_metadata_attrs(self):
    """Check if table has hidden job metadata columns."""
    hidden_attrs = self.heading._attributes  # includes hidden
    return '_job_start_time' in hidden_attrs

def _update_job_metadata(self, key, start_time, duration, version):
    """Update hidden job metadata for the given key."""
    # UPDATE using primary key
    pk_condition = make_condition(self, key, set())
    self.connection.query(
        f"UPDATE {self.full_table_name} SET "
        f"`_job_start_time`=%s, `_job_duration`=%s, `_job_version`=%s "
        f"WHERE {pk_condition}",
        args=(start_time, duration, version[:64])
    )
```

### 3. Job table (jobs.py)

Update version field length:
```python
version=""      : varchar(64)
```

### 4. Version helper

```python
def _get_job_version() -> str:
    """Get version string, truncated to 64 chars."""
    from .settings import config

    method = config.jobs.version_method
    if method is None or method == "none":
        return ""
    elif method == "git":
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.stdout.strip()[:64] if result.returncode == 0 else ""
        except Exception:
            return ""
    return ""
```

## Example Usage

```python
# Enable job metadata for new tables
dj.config.jobs.add_job_metadata = True

@schema
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    result : float
    """

    def make(self, key):
        # User code - unaware of hidden attributes
        self.insert1({**key, 'result': compute(key)})

# Job metadata automatically added and populated:
# _job_start_time, _job_duration, _job_version

# User-facing API unaffected:
ProcessedData().heading.names  # ['raw_data_id', 'result']
ProcessedData().fetch()  # Returns only visible attributes

# Access hidden attributes explicitly if needed:
ProcessedData().fetch('_job_start_time', '_job_duration', '_job_version')
```

## Summary of Design Decisions

| Decision | Resolution |
|----------|------------|
| Configuration | `config.jobs.add_job_metadata` (default False) |
| Environment variable | `DJ_JOBS_ADD_JOB_METADATA` |
| Existing tables | No automatic ALTER - silently skip metadata if columns absent |
| Retrofitting | Manual via `datajoint.migrate.add_job_metadata_columns()` utility |
| Populate modes | Record metadata in both direct and distributed modes |
| Part tables | No metadata columns - only master tables |
| Version length | varchar(64) in both jobs table and computed tables |
| Binary operators | Hidden attributes excluded via USING clause instead of NATURAL JOIN |
| Failed makes | N/A - transaction rolls back, no rows to update |
