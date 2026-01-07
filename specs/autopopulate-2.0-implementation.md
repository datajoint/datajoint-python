# AutoPopulate 2.0 Implementation Plan

This document outlines the implementation steps for AutoPopulate 2.0 based on the specification in `docs/src/compute/autopopulate2.0-spec.md`.

## Overview

The implementation involves changes to these files:
- `src/datajoint/jobs.py` - New `JobsTable` class (per-table jobs)
- `src/datajoint/autopopulate.py` - Updated `AutoPopulate` mixin
- `src/datajoint/user_tables.py` - FK-only PK constraint for Computed/Imported
- `src/datajoint/schemas.py` - Updated `schema.jobs` property
- `src/datajoint/settings.py` - New configuration options

## Table Naming Convention

Jobs tables use the `~~` prefix (double tilde):

| Table Type | Example Class | MySQL Table Name |
|------------|---------------|------------------|
| Manual | `Subject` | `subject` |
| Lookup | `#Method` | `#method` |
| Imported | `_Recording` | `_recording` |
| Computed | `__Analysis` | `__analysis` |
| Hidden | `~jobs` | `~jobs` |
| **Jobs (new)** | N/A | `~~analysis` |

The `~~` prefix:
- Distinguishes from single-tilde hidden tables (`~jobs`, `~lineage`)
- Shorter than suffix-based naming
- Excluded from `list_tables()` (tables starting with `~`)

## Execution Modes

AutoPopulate 2.0 supports two execution modes, both equally valid:

### Direct Mode (`reserve_jobs=False`, default)

Best for:
- Early development and debugging
- Single-worker execution
- Simple pipelines without distributed computing
- Interactive exploration

Behavior:
- Computes `(key_source & restrictions) - self` directly
- No jobs table involvement
- No coordination overhead

### Distributed Mode (`reserve_jobs=True`)

Best for:
- Multi-worker parallel processing
- Production pipelines with monitoring
- Job prioritization and scheduling
- Error tracking and retry workflows

Behavior:
- Uses per-table jobs table for coordination
- Supports priority, scheduling, status tracking
- Enables dashboard monitoring

## Phase 1: JobsTable Class

### 1.1 Create JobsTable Class

**File**: `src/datajoint/jobs.py`

```python
class JobsTable(Table):
    """Hidden table managing job queue for an auto-populated table."""

    _prefix = "~~"

    def __init__(self, target_table):
        """
        Initialize jobs table for an auto-populated table.

        Args:
            target_table: The Computed/Imported table instance
        """
        self._target_class = target_table.__class__
        self._connection = target_table.connection
        self.database = target_table.database
        self._definition = self._generate_definition(target_table)

    @property
    def table_name(self):
        """Jobs table name: ~~base_name"""
        target_name = self._target_class.table_name
        base_name = target_name.lstrip('_')
        return f"~~{base_name}"
```

### 1.2 Core Methods

```python
def refresh(
    self,
    *restrictions,
    delay: float = 0,
    priority: int = None,
    stale_timeout: float = None,
    orphan_timeout: float = None
) -> dict:
    """
    Refresh jobs queue: add new, remove stale, handle orphans.

    Args:
        restrictions: Filter key_source when adding new jobs
        delay: Seconds until new jobs become available
        priority: Priority for new jobs (lower = more urgent)
        stale_timeout: Remove jobs older than this if key not in key_source
        orphan_timeout: Reset reserved jobs older than this to pending

    Returns:
        {'added': int, 'removed': int, 'orphaned': int, 're_pended': int}
    """

def reserve(self, key: dict) -> bool:
    """
    Reserve a pending job for processing.

    Returns True if reservation successful, False if job not available.
    """

def complete(self, key: dict, duration: float = None) -> None:
    """Mark job as completed (success or delete based on config)."""

def error(self, key: dict, error_message: str, error_stack: str = None) -> None:
    """Mark job as failed with error details."""

def ignore(self, key: dict) -> None:
    """Mark job to be skipped during populate."""

def progress(self) -> dict:
    """Return job status breakdown."""
```

### 1.3 Status Properties

```python
@property
def pending(self) -> QueryExpression:
    return self & 'status="pending"'

@property
def reserved(self) -> QueryExpression:
    return self & 'status="reserved"'

@property
def errors(self) -> QueryExpression:
    return self & 'status="error"'

@property
def ignored(self) -> QueryExpression:
    return self & 'status="ignore"'

@property
def completed(self) -> QueryExpression:
    return self & 'status="success"'
```

### 1.4 Definition Generation

```python
def _generate_definition(self, target_table):
    """Build jobs table definition from target's FK-derived primary key."""
    fk_attrs = self._get_fk_derived_pk_attrs(target_table)
    pk_lines = "\n    ".join(f"{name} : {dtype}" for name, dtype in fk_attrs)

    return f"""
    # Job queue for {target_table.full_table_name}
    {pk_lines}
    ---
    status          : enum('pending', 'reserved', 'success', 'error', 'ignore')
    priority        : uint8       # Set by refresh(), default from config
    created_time=CURRENT_TIMESTAMP : timestamp
    scheduled_time=CURRENT_TIMESTAMP : timestamp
    reserved_time=null  : timestamp
    completed_time=null : timestamp
    duration=null   : float64
    error_message="" : varchar(2047)
    error_stack=null : <blob>
    user=""         : varchar(255)
    host=""         : varchar(255)
    pid=0           : uint32
    connection_id=0 : uint64
    version=""      : varchar(255)
    """
```

## Phase 2: FK-Only Primary Key Constraint

### 2.1 Validation for New Tables

**File**: `src/datajoint/user_tables.py`

New auto-populated tables must have FK-only primary keys:

```python
@classmethod
def _validate_pk_constraint(cls):
    """Enforce FK-only PK for new auto-populated tables."""
    if cls.is_declared:
        return  # Skip validation for existing tables

    heading = cls.heading
    non_fk_pk = [
        name for name in heading.primary_key
        if not heading[name].is_foreign_key
    ]
    if non_fk_pk:
        raise DataJointError(
            f"Auto-populated table {cls.__name__} has non-FK primary key "
            f"attributes: {non_fk_pk}. Move these to secondary attributes "
            f"or reference a lookup table."
        )
```

### 2.2 Legacy Table Support

Existing tables with non-FK PK attributes continue to work:
- Jobs table uses only FK-derived attributes
- Warning logged about degraded granularity
- One job may cover multiple target rows

## Phase 3: AutoPopulate Mixin Updates

### 3.1 Add `jobs` Property

**File**: `src/datajoint/autopopulate.py`

```python
class AutoPopulate:
    _jobs_table = None

    @property
    def jobs(self):
        """Access the jobs table for this auto-populated table."""
        if self._jobs_table is None:
            self._jobs_table = JobsTable(self)
            if not self._jobs_table.is_declared:
                self._jobs_table.declare()
        return self._jobs_table
```

### 3.2 Update `populate()` Signature

```python
def populate(
    self,
    *restrictions,
    suppress_errors: bool = False,
    return_exception_objects: bool = False,
    reserve_jobs: bool = False,
    max_calls: int = None,
    display_progress: bool = False,
    processes: int = 1,
    make_kwargs: dict = None,
    priority: int = None,
    refresh: bool = None,
) -> dict:
```

### 3.3 Execution Path Selection

```python
def populate(self, *restrictions, reserve_jobs=False, **kwargs):
    if self.connection.in_transaction:
        raise DataJointError("Populate cannot be called during a transaction.")

    if reserve_jobs:
        return self._populate_distributed(*restrictions, **kwargs)
    else:
        return self._populate_direct(*restrictions, **kwargs)
```

### 3.4 Direct Mode Implementation

```python
def _populate_direct(self, *restrictions, max_calls=None, suppress_errors=False, ...):
    """
    Populate without jobs table coordination.

    Computes keys directly from key_source, suitable for single-worker
    execution, development, and debugging.
    """
    keys = (self.key_source & AndList(restrictions)) - self
    keys = keys.fetch('KEY', limit=max_calls)

    success_count = 0
    error_list = []

    for key in tqdm(keys, disable=not display_progress):
        result = self._populate1(key, jobs=None, suppress_errors=suppress_errors, ...)
        # ... handle result
```

### 3.5 Distributed Mode Implementation

```python
def _populate_distributed(self, *restrictions, refresh=None, priority=None, max_calls=None, ...):
    """
    Populate with jobs table coordination.

    Uses jobs table for multi-worker coordination, priority scheduling,
    and status tracking.
    """
    # Refresh if configured
    if refresh is None:
        refresh = config['jobs.auto_refresh']
    if refresh:
        self.jobs.refresh(*restrictions, priority=priority)

    # Fetch pending jobs
    pending = (
        self.jobs.pending & 'scheduled_time <= NOW()'
    ).fetch('KEY', order_by='priority ASC, scheduled_time ASC', limit=max_calls)

    success_count = 0
    error_list = []

    for key in tqdm(pending, disable=not display_progress):
        if not self.jobs.reserve(key):
            continue  # Already reserved by another worker

        start_time = time.time()
        try:
            self._call_make(key, ...)
            duration = time.time() - start_time
            self.jobs.complete(key, duration=duration)
            success_count += 1
        except Exception as e:
            self.connection.cancel_transaction()
            self.jobs.error(key, str(e), traceback.format_exc())
            if not suppress_errors:
                raise
            error_list.append((key, e))

    return {'success_count': success_count, 'error_list': error_list}
```

## Phase 4: Schema Updates

### 4.1 Update `schema.jobs` Property

**File**: `src/datajoint/schemas.py`

```python
@property
def jobs(self):
    """
    Return list of JobsTable objects for all auto-populated tables.

    Returns:
        List[JobsTable]: Jobs tables for Computed/Imported tables in schema
    """
    from .jobs import JobsTable

    jobs_tables = []
    for table_name in self.list_tables():
        table_class = self(table_name)
        if hasattr(table_class, 'jobs'):
            jobs_tables.append(table_class.jobs)
    return jobs_tables
```

### 4.2 Exclude `~~` from `list_tables()`

Already handled - tables starting with `~` are excluded.

## Phase 5: Configuration

### 5.1 Add Config Options

**File**: `src/datajoint/settings.py`

```python
DEFAULTS = {
    'jobs.auto_refresh': True,
    'jobs.keep_completed': False,
    'jobs.stale_timeout': 3600,
    'jobs.default_priority': 5,
    'jobs.version': None,
}
```

### 5.2 Version Helper

```python
def get_job_version() -> str:
    """Get version string based on config."""
    version = config['jobs.version']
    if version == 'git':
        try:
            result = subprocess.run(
                ['git', 'rev-parse', '--short', 'HEAD'],
                capture_output=True, text=True, timeout=5
            )
            return result.stdout.strip() if result.returncode == 0 else ''
        except Exception:
            return ''
    return version or ''
```

## Phase 6: Table Lifecycle

### 6.1 Drop Jobs Table with Target

When an auto-populated table is dropped, its jobs table is also dropped:

```python
def drop(self):
    if hasattr(self, '_jobs_table') and self._jobs_table is not None:
        if self._jobs_table.is_declared:
            self._jobs_table.drop_quick()
    # ... existing drop logic
```

## Phase 7: Update Spec

Update `docs/src/compute/autopopulate2.0-spec.md`:
- Change `~table__jobs` references to `~~table`
- Update table naming section

## Implementation Order

1. **Phase 5**: Configuration (foundation)
2. **Phase 1**: JobsTable class
3. **Phase 2**: FK-only PK constraint
4. **Phase 3**: AutoPopulate updates
5. **Phase 4**: Schema.jobs property
6. **Phase 6**: Table lifecycle
7. **Phase 7**: Spec update
8. **Testing**: Throughout

## Testing Strategy

### Unit Tests
- `test_jobs_table_naming` - `~~` prefix
- `test_jobs_definition_generation` - FK-derived PK
- `test_refresh_operations` - add/remove/orphan/repend
- `test_reserve_complete_error_flow` - job lifecycle
- `test_progress_counts` - status aggregation

### Integration Tests
- `test_populate_direct_mode` - without jobs table
- `test_populate_distributed_mode` - with jobs table
- `test_multiprocess_populate` - concurrent workers
- `test_legacy_table_support` - non-FK PK tables
- `test_schema_jobs_property` - list of jobs tables

## Migration Notes

- Legacy `~jobs` table is NOT auto-deleted
- New `~~` tables created on first access to `.jobs`
- Both can coexist during transition
- Manual cleanup of legacy `~jobs` when ready
