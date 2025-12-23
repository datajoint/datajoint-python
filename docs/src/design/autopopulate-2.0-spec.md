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

### Terminology

- **Stale job**: A pending job whose upstream records have been deleted. The job references keys that no longer exist in `key_source`. Stale jobs are automatically cleaned up by `refresh()`.
- **Orphaned job**: A reserved job from a crashed or terminated process. The worker that reserved the job is no longer running, but the job remains in `reserved` status. Orphaned jobs must be cleared manually (see below).

### Core Design Principles

1. **Per-table jobs**: Each computed table gets its own hidden jobs table
2. **FK-derived primary keys**: Jobs table primary key includes only attributes derived from foreign keys in the target table's primary key (not additional primary key attributes)
3. **No FK constraints on jobs**: Jobs tables omit foreign key constraints for performance; stale jobs are cleaned by `refresh()`
4. **Rich status tracking**: Extended status values for full lifecycle visibility
5. **Automatic refresh**: `populate()` automatically refreshes the jobs queue (adding new jobs, removing stale ones)

## Architecture

### Jobs Table Structure

Each `dj.Imported` or `dj.Computed` table `MyTable` will have an associated hidden jobs table `~my_table__jobs` with the following structure:

```
# Job queue for MyTable
subject_id : int
session_id : int
...                           # Only FK-derived primary key attributes (NO foreign key constraints)
---
status          : enum('pending', 'reserved', 'success', 'error', 'ignore')
priority        : int         # Lower = more urgent (0 = highest priority, default: 5)
created_time    : datetime    # When job was added to queue
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

**Important**: The jobs table primary key includes only those attributes that come through foreign keys in the target table's primary key. Additional primary key attributes (if any) are excluded. This means:
- If a target table has primary key `(-> Subject, -> Session, method)`, the jobs table has primary key `(subject_id, session_id)` only
- Multiple target rows may map to a single job entry when additional PK attributes exist
- Jobs tables have **no foreign key constraints** for performance (stale jobs handled by `refresh()`)

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
| `success` | Job completed successfully (optional, depends on settings) |
| `error` | Job failed with an error |
| `ignore` | Job should be skipped (manually set, not part of automatic transitions) |

### Status Transitions

```mermaid
stateDiagram-v2
    state "(none)" as none1
    state "(none)" as none2
    none1 --> pending : refresh()
    none1 --> ignore : ignore()
    pending --> reserved : reserve()
    reserved --> none2 : complete()
    reserved --> success : complete()*
    reserved --> error : error()
    success --> pending : refresh()*
    error --> none2 : delete()
    success --> none2 : delete()
    ignore --> none2 : delete()
```

- `complete()` deletes the job entry (default when `jobs.keep_completed=False`)
- `complete()*` keeps the job as `success` (when `jobs.keep_completed=True`)
- `refresh()*` re-pends a `success` job if its key is in `key_source` but not in target

**Transition methods:**
- `refresh()` — Adds new jobs as `pending`; also re-pends `success` jobs if key is in `key_source` but not in target
- `ignore()` — Marks a key as `ignore` (can be called on keys not yet in jobs table)
- `reserve()` — Marks a pending job as `reserved` before calling `make()`
- `complete()` — Marks reserved job as `success`, or deletes it (based on `jobs.keep_completed` setting)
- `error()` — Marks reserved job as `error` with message and stack trace
- `delete()` — Inherited from `delete_quick()`; use `(jobs & condition).delete()` pattern

**Manual status control:**
- `ignore` is set manually via `jobs.ignore(key)` and is not part of automatic transitions
- Jobs with `status='ignore'` are skipped by `populate()` and `refresh()`
- To reset an ignored job, delete it and call `refresh()`: `jobs.ignored.delete(); jobs.refresh()`

## API Design

### JobsTable Class

```python
class JobsTable(Table):
    """Hidden table managing job queue for a computed table."""

    @property
    def definition(self) -> str:
        """Dynamically generated based on parent table's primary key."""
        ...

    def refresh(
        self,
        *restrictions,
        scheduled_time: datetime = None,
        priority: int = 5,
        stale_timeout: float = None
    ) -> dict:
        """
        Refresh the jobs queue: add new jobs and remove stale ones.

        Operations performed:
        1. Add new jobs: (key_source & restrictions) - target - jobs → insert as 'pending'
        2. Remove stale jobs: pending jobs older than stale_timeout whose keys
           are no longer in key_source (upstream records were deleted)

        Args:
            restrictions: Conditions to filter key_source
            scheduled_time: When new jobs should become available for processing.
                           Default: now (jobs are immediately available).
                           Use future times to schedule jobs for later processing.
            priority: Priority for new jobs (lower = more urgent). Default: 5
            stale_timeout: Seconds after which pending jobs are checked for staleness.
                          Jobs older than this are removed if their key is no longer
                          in key_source. Default from config: jobs.stale_timeout (3600s)

        Returns:
            {'added': int, 'removed': int} - counts of jobs added and stale jobs removed
        """
        ...

    def reserve(self, key: dict) -> bool:
        """
        Attempt to reserve a job for processing.

        Updates status to 'reserved' if currently 'pending' and scheduled_time <= now.
        No locking is used; rare conflicts are resolved by the make() transaction.

        Returns:
            True if reservation successful, False if job not found or not pending.
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

        To reset an ignored job, delete it and call refresh().
        """
        ...

    # delete() is inherited from delete_quick() - no confirmation required
    # Usage: (jobs & condition).delete() or jobs.errors.delete()

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
    def ignored(self) -> QueryExpression:
        """Return query for ignored jobs."""
        return self & 'status="ignore"'

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
    priority: int = None,          # Only process jobs at this priority or more urgent (lower values)
    refresh: bool = True,          # Refresh jobs queue if no pending jobs available
) -> dict:
    """
    Populate the table by calling make() for each missing entry.

    New behavior with reserve_jobs=True:
        1. Fetch all non-stale pending jobs (ordered by priority ASC, scheduled_time ASC)
        2. For each pending job:
           a. Mark job as 'reserved' (per-key, before make)
           b. Call make(key)
           c. On success: mark job as 'success' or delete (based on keep_completed)
           d. On error: mark job as 'error' with message/stack
        3. If refresh=True and no pending jobs were found, call self.jobs.refresh()
           and repeat from step 1
        4. Continue until no more pending jobs or max_calls reached
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

Priority and scheduling are handled via `refresh()` parameters. Lower priority values are more urgent (0 = highest priority).

```python
from datetime import datetime, timedelta

# Add urgent jobs (priority=0 is most urgent)
MyTable.jobs.refresh(priority=0)

# Add normal jobs (default priority=5)
MyTable.jobs.refresh()

# Add low-priority background jobs
MyTable.jobs.refresh(priority=10)

# Schedule jobs for future processing (2 hours from now)
future_time = datetime.now() + timedelta(hours=2)
MyTable.jobs.refresh(scheduled_time=future_time)

# Combine: urgent jobs scheduled for tonight
tonight = datetime.now().replace(hour=22, minute=0, second=0)
MyTable.jobs.refresh(priority=0, scheduled_time=tonight)

# Add urgent jobs for specific subjects
MyTable.jobs.refresh(Subject & 'priority="urgent"', priority=0)
```

## Implementation Details

### Table Naming Convention

Jobs tables follow the existing hidden table naming pattern:
- Table `FilteredImage` (stored as `__filtered_image`)
- Jobs table: `~filtered_image__jobs` (stored as `_filtered_image__jobs`)

### Primary Key Derivation

The jobs table primary key includes only those attributes derived from foreign keys in the target table's primary key:

```python
# Example 1: FK-only primary key (simple case)
@schema
class FilteredImage(dj.Computed):
    definition = """
    -> Image
    ---
    filtered_image : <djblob>
    """
# Jobs table primary key: (image_id) — same as target

# Example 2: Target with additional PK attribute
@schema
class Analysis(dj.Computed):
    definition = """
    -> Recording
    analysis_method : varchar(32)   # Additional PK attribute
    ---
    result : float
    """
# Jobs table primary key: (recording_id) — excludes 'analysis_method'
# One job entry covers all analysis_method values for a given recording
```

The jobs table has **no foreign key constraints** for performance reasons.

### Stale Job Handling

Stale jobs are pending jobs whose upstream records have been deleted. Since there are no FK constraints on jobs tables, these jobs remain until cleaned up by `refresh()`:

```python
# refresh() handles stale jobs automatically
result = FilteredImage.jobs.refresh()
# Returns: {'added': 10, 'removed': 3}  # 3 stale jobs cleaned up

# Stale detection logic:
# 1. Find pending jobs where created_time < (now - stale_timeout)
# 2. Check if their keys still exist in key_source
# 3. Remove pending jobs whose keys no longer exist
```

**Why not use foreign key cascading deletes?**
- FK constraints add overhead on every insert/update/delete operation
- Jobs tables are high-traffic (frequent reservations and status updates)
- Stale jobs are harmless until refresh—they simply won't match key_source
- The `refresh()` approach is more efficient for batch cleanup

### Table Drop and Alter Behavior

When an auto-populated table is **dropped**, its associated jobs table is automatically dropped:

```python
# Dropping FilteredImage also drops ~filtered_image__jobs
FilteredImage.drop()
```

When an auto-populated table is **altered** (e.g., primary key changes), the jobs table is dropped and can be recreated via `refresh()`:

```python
# Alter that changes primary key structure
# Jobs table is dropped since its structure no longer matches
FilteredImage.alter()

# Recreate jobs table with new structure
FilteredImage.jobs.refresh()
```

### Lazy Table Creation

Jobs tables are created automatically on first use:

```python
# First call to populate with reserve_jobs=True creates the jobs table
FilteredImage.populate(reserve_jobs=True)
# Creates ~filtered_image__jobs if it doesn't exist, then populates

# Alternatively, explicitly create/refresh the jobs table
FilteredImage.jobs.refresh()
```

The jobs table is created with a primary key derived from the target table's foreign key attributes.

### Conflict Resolution

Conflict resolution relies on the transaction surrounding each `make()` call. This applies regardless of whether `reserve_jobs=True` or `reserve_jobs=False`:

- With `reserve_jobs=False`: Workers query `key_source` directly and may attempt the same key
- With `reserve_jobs=True`: Job reservation reduces conflicts but doesn't eliminate them entirely

When two workers attempt to populate the same key:
1. Both call `make()` for the same key
2. First worker's `make()` transaction commits, inserting the result
3. Second worker's `make()` transaction fails with duplicate key error
4. Second worker catches the error and moves to the next job

**Why this is acceptable**:
- The `make()` transaction guarantees data integrity
- Duplicate key error is a clean, expected signal
- With `reserve_jobs=True`, conflicts are rare (requires near-simultaneous reservation)
- Wasted computation is minimal compared to locking complexity

### Job Reservation vs Pre-Partitioning

The job reservation mechanism (`reserve_jobs=True`) allows workers to dynamically claim jobs from a shared queue. However, some orchestration systems may prefer to **pre-partition** jobs before distributing them to workers:

```python
# Pre-partitioning example: orchestrator divides work explicitly
all_pending = FilteredImage.jobs.pending.fetch("KEY")

# Split jobs among workers (e.g., by worker index)
n_workers = 4
for worker_id in range(n_workers):
    worker_jobs = all_pending[worker_id::n_workers]  # Round-robin assignment
    # Send worker_jobs to worker via orchestration system (Slurm, K8s, etc.)

# Worker receives its assigned keys and processes them directly
for key in assigned_keys:
    FilteredImage.populate(key, reserve_jobs=False)
```

**When to use each approach**:

| Approach | Use Case |
|----------|----------|
| **Dynamic reservation** (`reserve_jobs=True`) | Simple setups, variable job durations, workers that start/stop dynamically |
| **Pre-partitioning** | Batch schedulers (Slurm, PBS), predictable job counts, avoiding reservation overhead |

Both approaches benefit from the same transaction-based conflict resolution as a safety net.

### Orphaned Job Handling

Orphaned jobs are reserved jobs from crashed or terminated processes. The API does not provide an algorithmic method for detecting or clearing orphaned jobs because this is dependent on the orchestration system (e.g., Slurm job IDs, Kubernetes pod status, process heartbeats).

Users must manually clear orphaned jobs using the `delete()` method:

```python
# Delete all reserved jobs (use with caution - may kill active jobs!)
MyTable.jobs.reserved.delete()

# Delete reserved jobs from a specific host that crashed
(MyTable.jobs.reserved & 'host="crashed-node"').delete()

# Delete reserved jobs older than 1 hour (likely orphaned)
(MyTable.jobs.reserved & 'reserved_time < NOW() - INTERVAL 1 HOUR').delete()

# Delete and re-add as pending
MyTable.jobs.reserved.delete()
MyTable.jobs.refresh()
```

**Note**: Deleting a reserved job does not terminate the running worker—it simply removes the reservation record. If the worker is still running, it will complete its `make()` call. If the job is then refreshed as pending and picked up by another worker, duplicated work may occur. Coordinate with your orchestration system to identify truly orphaned jobs before clearing them.

## Configuration Options

New configuration settings for job management:

```python
# In datajoint config
dj.config['jobs.auto_refresh'] = True      # Auto-refresh on populate (default: True)
dj.config['jobs.keep_completed'] = False   # Keep success records (default: False)
dj.config['jobs.stale_timeout'] = 3600     # Seconds before pending job is considered stale (default: 3600)
dj.config['jobs.default_priority'] = 5     # Default priority for new jobs (lower = more urgent)
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
# Add urgent jobs (priority=0 is most urgent)
urgent_subjects = Subject & 'priority="urgent"'
FilteredImage.jobs.refresh(urgent_subjects, priority=0)

# Workers will process lowest-priority-value jobs first
FilteredImage.populate(reserve_jobs=True)
```

### Scheduled Processing

```python
# Schedule jobs for overnight processing
from datetime import datetime, timedelta

tonight = datetime.now().replace(hour=22, minute=0, second=0)
FilteredImage.jobs.refresh('subject_id > 100', scheduled_time=tonight)

# Only jobs scheduled for now or earlier will be processed
FilteredImage.populate(reserve_jobs=True)
```

### Error Recovery

```python
# View errors
errors = FilteredImage.jobs.errors.fetch(as_dict=True)
for err in errors:
    print(f"Key: {err['subject_id']}, Error: {err['error_message']}")

# Delete specific error jobs after fixing the issue
(FilteredImage.jobs & 'subject_id=42').delete()

# Delete all error jobs
FilteredImage.jobs.errors.delete()

# Re-add deleted jobs as pending (if keys still in key_source)
FilteredImage.jobs.refresh()
```

### Dashboard Queries

```python
# Get pipeline-wide status using schema.jobs
def pipeline_status(schema):
    return {
        jt.target.table_name: jt.progress()
        for jt in schema.jobs
    }

# Example output:
# {
#     'FilteredImage': {'pending': 150, 'reserved': 3, 'success': 847, 'error': 12},
#     'Analysis': {'pending': 500, 'reserved': 0, 'success': 0, 'error': 0},
# }

# Refresh all jobs tables in the schema
for jobs_table in schema.jobs:
    jobs_table.refresh()

# Get all errors across the pipeline
all_errors = []
for jt in schema.jobs:
    errors = jt.errors.fetch(as_dict=True)
    for err in errors:
        err['_table'] = jt.target.table_name
        all_errors.append(err)
```

## Backward Compatibility

### Migration

This is a major release. The legacy schema-level `~jobs` table is replaced by per-table jobs tables:

- **Legacy `~jobs` table**: No longer used; can be dropped manually if present
- **New jobs tables**: Created automatically on first `populate(reserve_jobs=True)` call
- **No parallel support**: Teams should migrate cleanly to the new system

### API Compatibility

The `schema.jobs` property returns a list of all jobs table objects for auto-populated tables in the schema:

```python
# Returns list of JobsTable objects
schema.jobs
# [FilteredImage.jobs, Analysis.jobs, ...]

# Iterate over all jobs tables
for jobs_table in schema.jobs:
    print(f"{jobs_table.target.table_name}: {jobs_table.progress()}")

# Query all errors across the schema
all_errors = [job for jt in schema.jobs for job in jt.errors.fetch(as_dict=True)]

# Refresh all jobs tables
for jobs_table in schema.jobs:
    jobs_table.refresh()
```

This replaces the legacy single `~jobs` table with direct access to per-table jobs.

## Hazard Analysis

This section identifies potential hazards and their mitigations.

### Race Conditions

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Simultaneous reservation** | Two workers reserve the same pending job at nearly the same time | Acceptable: duplicate `make()` calls are resolved by transaction—second worker gets duplicate key error |
| **Reserve during refresh** | Worker reserves a job while another process is running `refresh()` | No conflict: `refresh()` adds new jobs and removes stale ones; reservation updates existing rows |
| **Concurrent refresh calls** | Multiple processes call `refresh()` simultaneously | Acceptable: may result in duplicate insert attempts, but primary key constraint prevents duplicates |
| **Complete vs delete race** | One process completes a job while another deletes it | Acceptable: one operation succeeds, other becomes no-op (row not found) |

### State Transitions

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Invalid state transition** | Code attempts illegal transition (e.g., pending → success) | Implementation enforces valid transitions; invalid attempts raise error |
| **Stuck in reserved** | Worker crashes while job is reserved (orphaned job) | Manual intervention required: `jobs.reserved.delete()` (see Orphaned Job Handling) |
| **Success re-pended unexpectedly** | `refresh()` re-pends a success job when user expected it to stay | Only occurs if `keep_completed=True` AND key exists in `key_source` but not in target; document clearly |
| **Ignore not respected** | Ignored jobs get processed anyway | Implementation must skip `status='ignore'` in `populate()` job fetching |

### Data Integrity

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Stale job processed** | Job references deleted upstream data | `make()` will fail or produce invalid results; `refresh()` cleans stale jobs before processing |
| **Jobs table out of sync** | Jobs table doesn't match `key_source` | `refresh()` synchronizes; call periodically or rely on `populate(refresh=True)` |
| **Partial make failure** | `make()` partially succeeds then fails | DataJoint transaction rollback ensures atomicity; job marked as error |
| **Error message truncation** | Error details exceed `varchar(2047)` | Full stack stored in `error_stack` (mediumblob); `error_message` is summary only |

### Performance

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Large jobs table** | Jobs table grows very large with `keep_completed=True` | Default is `keep_completed=False`; provide guidance on periodic cleanup |
| **Slow refresh on large key_source** | `refresh()` queries entire `key_source` | Can restrict refresh to subsets: `jobs.refresh(Subject & 'lab="smith"')` |
| **Many jobs tables per schema** | Schema with many computed tables has many jobs tables | Jobs tables are lightweight; only created on first use |

### Operational

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Accidental job deletion** | User runs `jobs.delete()` without restriction | `delete()` inherits from `delete_quick()` (no confirmation); users must apply restrictions carefully |
| **Clearing active jobs** | User clears reserved jobs while workers are still running | May cause duplicated work if job is refreshed and picked up again; coordinate with orchestrator |
| **Priority confusion** | User expects higher number = higher priority | Document clearly: lower values are more urgent (0 = highest priority) |

### Migration

| Hazard | Description | Mitigation |
|--------|-------------|------------|
| **Legacy ~jobs table conflict** | Old `~jobs` table exists alongside new per-table jobs | Systems are independent; legacy table can be dropped manually |
| **Mixed version workers** | Some workers use old system, some use new | Major release; do not support mixed operation—require full migration |
| **Lost error history** | Migrating loses error records from legacy table | Document migration procedure; users can export legacy errors before migration |

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
4. **High performance**: No FK constraints means minimal overhead on job operations
5. **Scalability**: Each table's jobs can be indexed independently

### Why Remove Key Hashing?

The current system hashes primary keys to support arbitrary key types. The new system uses native keys because:

1. **Readability**: Debugging is much easier with readable keys
2. **Query efficiency**: Native keys can use table indexes
3. **Foreign keys**: Hash-based keys cannot participate in foreign key relationships
4. **Simplicity**: No need for hash computation and comparison

### Why FK-Derived Primary Keys Only?

The jobs table primary key includes only attributes derived from foreign keys in the target table's primary key. This design:

1. **Aligns with key_source**: The `key_source` query naturally produces keys matching the FK-derived attributes
2. **Simplifies job identity**: A job's identity is determined by its upstream dependencies
3. **Handles additional PK attributes**: When targets have additional PK attributes (e.g., `method`), one job covers all values for that attribute
