# Autopopulate 2.0 Specification

## Overview

This specification redesigns the DataJoint job handling system to provide better visibility, control, and scalability for distributed computing workflows. The new system replaces the schema-level `~jobs` table with per-table job tables that offer richer status tracking, proper referential integrity, and dashboard-friendly monitoring.

## Problem Statement

### Current Jobs Table Limitations

The existing `~jobs` table has significant limitations:

1. **Limited status tracking**: Only supports `reserved`, `error`, and `ignore` statuses
2. **Functions as an error log**: Cannot efficiently track pending or completed jobs
3. **Poor dashboard visibility**: No way to monitor pipeline progress without querying multiple tables
4. **Key hashing obscures data**: Primary keys are stored as hashes, making debugging difficult
5. **No referential integrity**: Jobs table is independent of computed tables; orphaned jobs can accumulate

### Key Source Limitations

1. **Frequent manual modifications**: Subset operations require modifying `key_source` property
2. **Local visibility only**: Custom key sources are not accessible database-wide
3. **Performance bottleneck**: Multiple workers querying `key_source` simultaneously creates contention
4. **Codebase dependency**: Requires full pipeline codebase to determine pending work

## Proposed Solution

### Core Design Principles

1. **Foreign-key-only primary keys**: Auto-populated tables cannot introduce new primary key attributes; their primary key must comprise only foreign key references
2. **Per-table jobs**: Each computed table gets its own hidden jobs table
3. **Native primary keys**: Jobs table uses the same primary key structure as its parent table (no hashes)
4. **Referential integrity**: Jobs are foreign-key linked to parent tables with cascading deletes
5. **Rich status tracking**: Extended status values for full lifecycle visibility
6. **Automatic refresh**: `populate()` automatically refreshes the jobs queue

### Primary Key Constraint

**Auto-populated tables (`dj.Imported` and `dj.Computed`) must have primary keys composed entirely of foreign key references.**

This constraint ensures:
- **1:1 key_source mapping**: Each entry in `key_source` corresponds to exactly one potential job
- **Deterministic job identity**: A job's identity is fully determined by its parent records
- **Simplified jobs table**: The jobs table can directly reference the same parents as the computed table

```python
# VALID: Primary key is entirely foreign keys
@schema
class FilteredImage(dj.Computed):
    definition = """
    -> Image
    ---
    filtered_image : <djblob>
    """

# VALID: Multiple foreign keys in primary key
@schema
class Comparison(dj.Computed):
    definition = """
    -> Image.proj(image_a='image_id')
    -> Image.proj(image_b='image_id')
    ---
    similarity : float
    """

# INVALID: Additional primary key attribute not allowed
@schema
class Analysis(dj.Computed):
    definition = """
    -> Recording
    analysis_method : varchar(32)   # NOT ALLOWED - adds to primary key
    ---
    result : float
    """
```

**Migration note**: Existing tables that violate this constraint will continue to work but cannot use the new jobs system. A deprecation warning will be issued.

## Architecture

### Jobs Table Structure

Each `dj.Imported` or `dj.Computed` table `MyTable` will have an associated hidden jobs table `~my_table__jobs` with the following structure:

```
# Job queue for MyTable
-> ParentTable1
-> ParentTable2
...                           # Same primary key structure as MyTable
---
status          : enum('pending', 'reserved', 'success', 'error', 'ignore')
priority        : int         # Higher priority = processed first (default: 0)
scheduled_time  : datetime    # Process on or after this time (default: now)
reserved_time   : datetime    # When job was reserved (null if not reserved)
completed_time  : datetime    # When job completed (null if not completed)
duration        : float       # Execution duration in seconds (null if not completed)
error_message   : varchar(2047)  # Truncated error message
error_stack     : mediumblob  # Full error traceback
user            : varchar(255)   # Database user who reserved/completed job
host            : varchar(255)   # Hostname of worker
pid             : int unsigned   # Process ID of worker
connection_id   : bigint unsigned  # MySQL connection ID
version         : varchar(255)   # Code version (git hash, package version, etc.)
```

### Access Pattern

Jobs are accessed as a property of the computed table:

```python
# Current pattern (schema-level)
schema.jobs

# New pattern (per-table)
MyTable.jobs

# Examples
FilteredImage.jobs                    # Access jobs table
FilteredImage.jobs & 'status="error"' # Query errors
FilteredImage.jobs.refresh()          # Refresh job queue
```

### Status Values

| Status | Description |
|--------|-------------|
| `pending` | Job is queued and ready to be processed |
| `reserved` | Job is currently being processed by a worker |
| `success` | Job completed successfully |
| `error` | Job failed with an error |
| `ignore` | Job should be skipped (manually set) |

### Status Transitions

```
                    ┌─────────────────────────────────────┐
                    │                                     │
                    ▼                                     │
┌─────────┐    ┌──────────┐    ┌───────────┐    ┌────────┴──┐
│ (none)  │───▶│ pending  │───▶│ reserved  │───▶│  success  │
└─────────┘    └──────────┘    └───────────┘    └───────────┘
     │              │               │
     │              │               │
     │              ▼               ▼
     │         ┌──────────┐    ┌───────────┐
     └────────▶│  ignore  │    │   error   │───┐
               └──────────┘    └───────────┘   │
                    ▲               │          │
                    │               ▼          │
                    │          ┌──────────┐    │
                    └──────────│ pending  │◀───┘
                               └──────────┘
                               (after reset)
```

## API Design

### JobsTable Class

```python
class JobsTable(Table):
    """Hidden table managing job queue for a computed table."""

    @property
    def definition(self) -> str:
        """Dynamically generated based on parent table's primary key."""
        ...

    def refresh(self, *restrictions) -> int:
        """
        Refresh the jobs queue by scanning for missing entries.

        Computes: (key_source & restrictions) - target - jobs
        Inserts new entries with status='pending'.

        Returns:
            Number of new jobs added to queue.
        """
        ...

    def reserve(self, key: dict) -> bool:
        """
        Attempt to reserve a job for processing.

        Uses SELECT FOR UPDATE to prevent race conditions.
        Only reserves jobs with status='pending' and scheduled_time <= now.

        Returns:
            True if reservation successful, False if already taken.
        """
        ...

    def complete(self, key: dict, duration: float = None) -> None:
        """
        Mark a job as successfully completed.

        Updates status to 'success', records duration and completion time.
        """
        ...

    def error(self, key: dict, error_message: str, error_stack: str = None) -> None:
        """
        Mark a job as failed with error details.

        Updates status to 'error', records error message and stack trace.
        """
        ...

    def ignore(self, key: dict) -> None:
        """
        Mark a job to be ignored (skipped during populate).
        """
        ...

    def reset(self, *restrictions, include_errors: bool = True) -> int:
        """
        Reset jobs to pending status.

        Args:
            restrictions: Conditions to filter which jobs to reset
            include_errors: If True, also reset error jobs (default: True)

        Returns:
            Number of jobs reset.
        """
        ...

    def clear_completed(self, *restrictions, before: datetime = None) -> int:
        """
        Remove completed jobs from the queue.

        Args:
            restrictions: Conditions to filter which jobs to clear
            before: Only clear jobs completed before this time

        Returns:
            Number of jobs cleared.
        """
        ...

    @property
    def pending(self) -> QueryExpression:
        """Return query for pending jobs."""
        return self & 'status="pending"'

    @property
    def reserved(self) -> QueryExpression:
        """Return query for reserved jobs."""
        return self & 'status="reserved"'

    @property
    def errors(self) -> QueryExpression:
        """Return query for error jobs."""
        return self & 'status="error"'

    @property
    def completed(self) -> QueryExpression:
        """Return query for completed jobs."""
        return self & 'status="success"'
```

### AutoPopulate Integration

The `populate()` method is updated to use the new jobs table:

```python
def populate(
    self,
    *restrictions,
    suppress_errors: bool = False,
    return_exception_objects: bool = False,
    reserve_jobs: bool = False,
    order: str = "original",
    limit: int = None,
    max_calls: int = None,
    display_progress: bool = False,
    processes: int = 1,
    make_kwargs: dict = None,
    # New parameters
    priority: int = None,          # Only process jobs with this priority or higher
    refresh: bool = True,          # Refresh jobs queue before populating
) -> dict:
    """
    Populate the table by calling make() for each missing entry.

    New behavior with reserve_jobs=True:
        1. If refresh=True, calls self.jobs.refresh(*restrictions)
        2. Fetches jobs from self.jobs where status='pending' and scheduled_time <= now
        3. Reserves and processes jobs using the jobs table
        4. Records success/error status in jobs table
    """
    ...
```

### Progress and Monitoring

```python
# Current progress reporting
remaining, total = MyTable.progress()

# Enhanced progress with jobs table
MyTable.jobs.progress()  # Returns detailed status breakdown

# Example output:
# {
#     'pending': 150,
#     'reserved': 3,
#     'success': 847,
#     'error': 12,
#     'ignore': 5,
#     'total': 1017
# }
```

### Priority and Scheduling

```python
# Set priority for specific jobs (higher = processed first)
MyTable.jobs.set_priority(restriction, priority=10)

# Schedule jobs for future processing
from datetime import datetime, timedelta
future_time = datetime.now() + timedelta(hours=2)
MyTable.jobs.schedule(restriction, scheduled_time=future_time)

# Insert with priority during refresh
MyTable.jobs.refresh(priority=5)  # All new jobs get priority=5
```

## Implementation Details

### Table Naming Convention

Jobs tables follow the existing hidden table naming pattern:
- Table `FilteredImage` (stored as `__filtered_image`)
- Jobs table: `~filtered_image__jobs` (stored as `_filtered_image__jobs`)

### Referential Integrity

The jobs table references the same parent tables as the computed table:

```python
# If FilteredImage has definition:
@schema
class FilteredImage(dj.Computed):
    definition = """
    -> Image
    ---
    filtered_image : <djblob>
    """

# The jobs table will have:
# -> Image  (same foreign key reference)
# This ensures cascading deletes work correctly
```

### Cascading Behavior

When a parent record is deleted:
1. The corresponding computed table record is deleted (existing behavior)
2. The corresponding jobs table record is also deleted (new behavior)

This prevents orphaned job records.

### Migration from Current System

The schema-level `~jobs` table will be:
1. **Maintained** for backward compatibility during transition
2. **Deprecated** with warnings when `reserve_jobs=True` is used
3. **Migration utility** provided to convert existing jobs to new format

```python
# Migration utility
schema.migrate_jobs()  # Migrates ~jobs entries to per-table jobs tables
```

### Race Condition Handling

Job reservation uses database-level locking to prevent race conditions:

```sql
-- Reserve a job atomically
START TRANSACTION;
SELECT * FROM `_my_table__jobs`
WHERE status = 'pending'
  AND scheduled_time <= NOW()
ORDER BY priority DESC, scheduled_time ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- If row found, update it
UPDATE `_my_table__jobs`
SET status = 'reserved',
    reserved_time = NOW(),
    user = CURRENT_USER(),
    host = @@hostname,
    pid = CONNECTION_ID()
WHERE <primary_key_match>;

COMMIT;
```

### Stale Job Detection

Reserved jobs that have been running too long may indicate crashed workers:

```python
# Find potentially stale jobs (reserved > 1 hour ago)
stale = MyTable.jobs & 'status="reserved"' & 'reserved_time < NOW() - INTERVAL 1 HOUR'

# Reset stale jobs to pending
MyTable.jobs.reset(stale)
```

## Configuration Options

New configuration settings for job management:

```python
# In datajoint config
dj.config['jobs.auto_refresh'] = True      # Auto-refresh on populate (default: True)
dj.config['jobs.keep_completed'] = False   # Keep success records (default: False)
dj.config['jobs.stale_timeout'] = 3600     # Seconds before reserved job is stale (default: 3600)
dj.config['jobs.default_priority'] = 0     # Default priority for new jobs (default: 0)
```

## Usage Examples

### Basic Distributed Computing

```python
# Worker 1
FilteredImage.populate(reserve_jobs=True)

# Worker 2 (can run simultaneously)
FilteredImage.populate(reserve_jobs=True)

# Monitor progress
print(FilteredImage.jobs.progress())
```

### Priority-Based Processing

```python
# Mark urgent jobs as high priority
urgent_subjects = Subject & 'priority="urgent"'
FilteredImage.jobs.set_priority(urgent_subjects, priority=100)

# Workers will process high-priority jobs first
FilteredImage.populate(reserve_jobs=True)
```

### Scheduled Processing

```python
# Schedule jobs for overnight processing
from datetime import datetime, timedelta

tonight = datetime.now().replace(hour=22, minute=0, second=0)
FilteredImage.jobs.schedule('subject_id > 100', scheduled_time=tonight)

# Only jobs scheduled for now or earlier will be processed
FilteredImage.populate(reserve_jobs=True)
```

### Error Recovery

```python
# View errors
errors = FilteredImage.jobs.errors.fetch(as_dict=True)
for err in errors:
    print(f"Key: {err['subject_id']}, Error: {err['error_message']}")

# Reset specific errors after fixing the issue
FilteredImage.jobs.reset('subject_id=42')

# Reset all errors
FilteredImage.jobs.reset(include_errors=True)
```

### Dashboard Queries

```python
# Get pipeline-wide status
def pipeline_status(schema):
    status = {}
    for table in schema.list_tables():
        tbl = getattr(schema, table)
        if hasattr(tbl, 'jobs'):
            status[table] = tbl.jobs.progress()
    return status

# Example output:
# {
#     'FilteredImage': {'pending': 150, 'reserved': 3, 'success': 847, 'error': 12},
#     'Analysis': {'pending': 500, 'reserved': 0, 'success': 0, 'error': 0},
# }
```

## Backward Compatibility

### Deprecation Path

1. **Phase 1 (Current Release)**:
   - New jobs tables created alongside existing `~jobs`
   - `reserve_jobs=True` uses new system by default
   - `reserve_jobs='legacy'` uses old system
   - Deprecation warning when using legacy system

2. **Phase 2 (Next Release)**:
   - Legacy `~jobs` table no longer updated
   - `reserve_jobs='legacy'` removed
   - Migration utility provided

3. **Phase 3 (Future Release)**:
   - Legacy `~jobs` table dropped on schema upgrade

### API Compatibility

The `schema.jobs` property will continue to work but return a unified view:

```python
# Returns all jobs across all tables in the schema
schema.jobs  # Deprecated, shows warning

# Equivalent to:
# SELECT * FROM _table1__jobs UNION SELECT * FROM _table2__jobs ...
```

## Future Extensions

- [ ] Web-based dashboard for job monitoring
- [ ] Webhook notifications for job completion/failure
- [ ] Job dependencies (job B waits for job A)
- [ ] Resource tagging (GPU required, high memory, etc.)
- [ ] Retry policies (max retries, exponential backoff)
- [ ] Job grouping/batching for efficiency
- [ ] Integration with external schedulers (Slurm, PBS, etc.)

## Rationale

### Why Not External Orchestration?

The team considered integrating external tools like Airflow or Flyte but rejected this approach because:

1. **Deployment complexity**: External orchestrators require significant infrastructure
2. **Maintenance burden**: Additional systems to maintain and monitor
3. **Accessibility**: Not all DataJoint users have access to orchestration platforms
4. **Tight integration**: DataJoint's transaction model requires close coordination

The built-in jobs system provides 80% of the value with minimal additional complexity.

### Why Per-Table Jobs?

Per-table jobs tables provide:

1. **Better isolation**: Jobs for one table don't affect others
2. **Simpler queries**: No need to filter by table_name
3. **Native keys**: Primary keys are readable, not hashed
4. **Referential integrity**: Automatic cleanup via foreign keys
5. **Scalability**: Each table's jobs can be indexed independently

### Why Remove Key Hashing?

The current system hashes primary keys to support arbitrary key types. The new system uses native keys because:

1. **Readability**: Debugging is much easier with readable keys
2. **Query efficiency**: Native keys can use table indexes
3. **Foreign keys**: Hash-based keys cannot participate in foreign key relationships
4. **Simplicity**: No need for hash computation and comparison

### Why Require Foreign-Key-Only Primary Keys?

Restricting auto-populated tables to foreign-key-only primary keys provides:

1. **1:1 job correspondence**: Each `key_source` entry maps to exactly one job, eliminating ambiguity about what constitutes a "job"
2. **Proper referential integrity**: The jobs table can reference the same parent tables, enabling cascading deletes
3. **Eliminates key_source complexity**: No need for custom `key_source` definitions to enumerate non-foreign-key combinations
4. **Clearer data model**: The computation graph is fully determined by table dependencies
5. **Simpler populate logic**: No need to handle partial key matching or key enumeration

**What if I need multiple outputs per parent?**

Use a part table pattern instead:

```python
# Instead of adding analysis_method to primary key:
@schema
class Analysis(dj.Computed):
    definition = """
    -> Recording
    ---
    timestamp : datetime
    """

    class Method(dj.Part):
        definition = """
        -> master
        analysis_method : varchar(32)
        ---
        result : float
        """

    def make(self, key):
        self.insert1(key)
        for method in ['pca', 'ica', 'nmf']:
            result = run_analysis(key, method)
            self.Method.insert1({**key, 'analysis_method': method, 'result': result})
```

This pattern maintains the 1:1 job mapping while supporting multiple outputs per computation.
