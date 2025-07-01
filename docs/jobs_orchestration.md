# DataJoint Jobs Orchestration mechanism

This document describes the behavior and mechanism of DataJoint's jobs reservation and execution system.

## Jobs Table Structure

The jobs table (`~jobs`) is a system table that tracks the state and execution of jobs in the DataJoint pipeline. It has the following key fields:

- `table_name`: The full table name being populated
- `key_hash`: A hash of the job's primary key
- `status`: Current job status, one of:
  - `scheduled`: Job is queued for execution
  - `reserved`: Job is currently being processed
  - `error`: Job failed with an error
  - `ignore`: Job is marked to be ignored
  - `success`: Job completed successfully
- `key`: JSON structure containing the job's primary key (query-able)
- `error_message`: Error message if job failed
- `error_stack`: Stack trace if job failed
- `user`: Database user who created the job
- `host`: System hostname where job was created
- `pid`: Process ID of the job
- `connection_id`: Database connection ID
- `timestamp`: When the job status was last changed
- `run_duration`: How long the job took to execute (in seconds)
- `run_metadata`: JSON structure containing metadata about the run (e.g. code version, environment info, system state)

## Job Scheduling Process

The `schedule_jobs` method implements an optimization strategy to prevent excessive scheduling:

1. **Rate Limiting**:
   - Uses `min_scheduling_interval` (configurable via `dj.config["min_scheduling_interval"]`)
   - Default interval is 5 seconds
   - Can be overridden per call

2. **Scheduling Logic**:
   - Checks for recent scheduling events within the interval
   - Skips scheduling if recent events exist
   - Otherwise, finds keys that need computation by:
     1. Querying the `key_source` to get all possible keys
     2. Excluding keys that already exist in the target table
     3. Excluding keys that are already in the jobs table with incompatible status
        (i.e., `scheduled`, `reserved`, or `success`)
   - Schedules each valid key as a new job
   - Records scheduling events for rate limiting

3. **Job States**:
   - New jobs start as `scheduled`
   - Jobs can be rescheduled if in `error` or `ignore` state (with `force=True`)
   - Prevents rescheduling if job is `scheduled`, `reserved`, or `success`

## Populate Process Flow

The `populate()` method orchestrates the job execution process:

1. **Initialization**:
   - Optionally schedules new jobs (controlled by `schedule_jobs` parameter)

2. **Job Selection**:
   - If `reserve_jobs=True`:
     - Fetches `scheduled` jobs from the jobs table
     - Applies any restrictions to the job set
     - Attempts to reserve each job before processing
     - Skips jobs that cannot be reserved (already taken by another process)
   - If `reserve_jobs=False`:
     - Uses traditional direct computation approach

3. **Execution**:
   - Processes jobs in specified order (`original`, `reverse`, or `random`)
   - Supports single or multi-process execution
   - For reserved jobs:
     - Updates job status to `reserved` during processing
     - Records execution metrics (duration, version)
     - On successful completion: remove job from the jobs table
     - On error: update job status to `error`
   - Records errors and execution metrics

4. **Cleanup**:
   - Optionally clean up orphaned/outdated jobs

## Job Cleanup Process

The `cleanup_jobs` method maintains database consistency by removing orphaned jobs:

1. **Orphaned Success Jobs**:
   - Identifies jobs marked as `success` but not present in the target table
   - These typically occur when target table entries are deleted

2. **Orphaned Incomplete Jobs**:
   - Identifies jobs in `scheduled`/`error`/`ignore` state that are no longer in the `key_source`
   - These typically occur when upstream table entries are deleted

3. **Cleanup Characteristics**:
   - Potentially time-consuming operation
   - Should not need to run frequently
   - Helps maintain database consistency

## Jobs Table Maintenance

The "freshness" and consistency of the jobs table depends on regular maintenance through two key operations:

1. **Scheduling Updates** (`schedule_jobs`):
   - Adds new jobs to the table
   - Should be run frequently enough to keep up with new data
   - Rate-limited by `min_scheduling_interval` to prevent overload
   - Example: Run every few minutes in a cron job for active pipelines
   - Event-driven approach: `inserts` in upstream tables auto trigger this step

2. **Cleanup** (`cleanup_jobs`):
   - Removes orphaned or outdated jobs
   - Should be run periodically to maintain consistency
   - More resource-intensive than scheduling
   - Example: Run daily during low-activity periods
   - Event-driven approach: `deletes` in upstream or target tables auto trigger this step

The balance between these operations affects:
- How quickly new jobs are discovered and scheduled
- How long orphaned jobs remain in the table
- Database size and query performance
- Overall system responsiveness

Recommended maintenance schedule:
```python
# Example: Run scheduling frequently
dj.config["min_scheduling_interval"] = 300  # 5 minutes

# Example: Run cleanup daily
# (implement as a cron job or scheduled task)
def daily_cleanup():
    for table in your_pipeline_tables:
        table.cleanup_jobs()
``` 