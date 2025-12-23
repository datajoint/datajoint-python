# Job Management

DataJoint provides a job reservation system for coordinating distributed `populate()`
operations across multiple workers. Each auto-populated table (`dj.Imported` or
`dj.Computed`) has an associated hidden jobs table that tracks processing status.

## Overview

The jobs system enables:

- **Distributed computing**: Multiple workers can process the same table without conflicts
- **Progress tracking**: Monitor pending, reserved, completed, and failed jobs
- **Error management**: Track and retry failed computations
- **Priority scheduling**: Process urgent jobs first

## Accessing the Jobs Table

Every auto-populated table has a `.jobs` attribute:

```python
@schema
class ProcessedData(dj.Computed):
    definition = """
    -> RawData
    ---
    result : float
    """

    def make(self, key):
        # computation logic
        self.insert1(dict(key, result=compute(key)))

# Access the jobs table
ProcessedData.jobs
```

## Job States

Jobs can be in one of five states:

| Status | Description |
|--------|-------------|
| `pending` | Queued and ready for processing |
| `reserved` | Currently being processed by a worker |
| `success` | Completed successfully |
| `error` | Failed with an error |
| `ignore` | Manually marked to skip |

## Refreshing the Job Queue

The `refresh()` method updates the jobs queue by adding new jobs and removing stale ones:

```python
# Add jobs for all missing keys
ProcessedData.jobs.refresh()

# Add jobs for specific restrictions
ProcessedData.jobs.refresh("subject_id > 10")

# Set priority (lower = more urgent, default: 5)
ProcessedData.jobs.refresh(priority=1)

# Delay job availability by 60 seconds
ProcessedData.jobs.refresh(delay=60)
```

**Returns**: `{'added': int, 'removed': int}` - counts of jobs added and stale jobs removed.

### Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `restrictions` | None | Filter conditions for key_source |
| `delay` | 0 | Seconds until jobs become available |
| `priority` | 5 | Job priority (lower = more urgent) |
| `stale_timeout` | 3600 | Seconds before checking pending jobs for staleness |

## Querying Job Status

### Filter by Status

```python
# Pending jobs
ProcessedData.jobs.pending

# Reserved (in-progress) jobs
ProcessedData.jobs.reserved

# Completed jobs
ProcessedData.jobs.completed

# Failed jobs
ProcessedData.jobs.errors

# Ignored jobs
ProcessedData.jobs.ignored
```

### Progress Summary

```python
ProcessedData.jobs.progress()
# Returns: {'pending': 50, 'reserved': 2, 'success': 100, 'error': 3, 'ignore': 1, 'total': 156}
```

### Fetch Pending Jobs

```python
# Get up to 10 highest-priority pending jobs
keys = ProcessedData.jobs.fetch_pending(limit=10)

# Get pending jobs at priority 3 or higher (lower number)
keys = ProcessedData.jobs.fetch_pending(priority=3)
```

## Managing Jobs

### Mark Keys to Ignore

Skip specific keys during populate:

```python
ProcessedData.jobs.ignore({"subject_id": 5, "session_id": 3})
```

### Clear Jobs

```python
# Delete all jobs
ProcessedData.jobs.delete()

# Delete specific jobs
(ProcessedData.jobs & "status='error'").delete()

# Drop the entire jobs table
ProcessedData.jobs.drop()
```

### View Error Details

```python
# View error messages
ProcessedData.jobs.errors.fetch("KEY", "error_message")

# Get full error traceback
error_job = (ProcessedData.jobs.errors & key).fetch1()
print(error_job["error_stack"])
```

## Configuration

Configure job behavior in `datajoint.json`:

```json
{
    "jobs": {
        "default_priority": 5,
        "stale_timeout": 3600,
        "keep_completed": false
    }
}
```

| Setting | Default | Description |
|---------|---------|-------------|
| `jobs.default_priority` | 5 | Default priority for new jobs |
| `jobs.stale_timeout` | 3600 | Seconds before pending jobs are checked for staleness |
| `jobs.keep_completed` | false | Keep job records after successful completion |

## Jobs Table Schema

The jobs table stores:

| Attribute | Type | Description |
|-----------|------|-------------|
| *primary key* | (varies) | FK-derived primary key from target table |
| `status` | enum | pending, reserved, success, error, ignore |
| `priority` | int | Lower = more urgent |
| `created_time` | datetime | When job was added |
| `scheduled_time` | datetime | Process on or after this time |
| `reserved_time` | datetime | When job was reserved |
| `completed_time` | datetime | When job completed |
| `duration` | float | Execution duration in seconds |
| `error_message` | varchar | Error message if failed |
| `error_stack` | blob | Full error traceback |
| `user` | varchar | Database user |
| `host` | varchar | Worker hostname |
| `pid` | int | Worker process ID |
| `connection_id` | bigint | MySQL connection ID |

## Distributed Processing Example

Run multiple workers to process a table in parallel:

```python
# Worker script (run on multiple machines)
import datajoint as dj

schema = dj.Schema('my_pipeline')

@schema
class Analysis(dj.Computed):
    definition = """
    -> Experiment
    ---
    result : float
    """

    def make(self, key):
        data = (Experiment & key).fetch1('data')
        self.insert1(dict(key, result=analyze(data)))

# Each worker runs:
Analysis.populate(reserve_jobs=True)
```

With `reserve_jobs=True`, workers coordinate through the jobs table to avoid
processing the same key twice.
