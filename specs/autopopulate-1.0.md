# AutoPopulate 1.0 Specification

This document describes the legacy AutoPopulate system in DataJoint Python, documenting how automated computation pipelines work. This specification serves as a reference for the system being replaced by AutoPopulate 2.0.

## Overview

AutoPopulate is a mixin class that adds the `populate()` method to a Table class. Auto-populated tables inherit from both `Table` and `AutoPopulate`, define the `key_source` property, and implement the `make` callback method.

**Source Files:**
- `src/datajoint/autopopulate.py` - Main AutoPopulate mixin
- `src/datajoint/jobs.py` - Job reservation table
- `src/datajoint/schemas.py` - Schema class with jobs property

## Key Characteristics (1.0 vs 2.0)

| Aspect | AutoPopulate 1.0 | AutoPopulate 2.0 |
|--------|------------------|------------------|
| **Jobs table scope** | Schema-level (`~jobs`) | Per-table (`~table__jobs`) |
| **Primary key** | `(table_name, key_hash)` | FK-derived attributes only |
| **Key storage** | MD5 hash + pickled blob | Native column values |
| **Status values** | `reserved`, `error`, `ignore` | `pending`, `reserved`, `success`, `error`, `ignore` |
| **Pending tracking** | None (computed on-the-fly) | Explicit `pending` status |
| **Priority** | None | Integer priority (lower = more urgent) |
| **Scheduling** | None | `scheduled_time` for delayed execution |
| **Duration tracking** | None | `duration` in seconds |
| **Code version** | None | `version` field |
| **`schema.jobs`** | Single `JobTable` | List of per-table `JobsTable` objects |
| **Job refresh** | None | `refresh()` syncs with `key_source` |

## 1. Key Source Generation

### Default Behavior

The `key_source` property returns a `QueryExpression` yielding primary key values to be passed to `make()`.

**Default implementation** (`autopopulate.py:59-83`):
1. Fetch all primary parent tables via `self.target.parents(primary=True, as_objects=True, foreign_key_info=True)`
2. Handle aliased attributes by projecting with renamed columns
3. Join all parent tables using the `*` operator (natural join)

```python
@property
def key_source(self):
    def _rename_attributes(table, props):
        return (
            table.proj(**{attr: ref for attr, ref in props["attr_map"].items() if attr != ref})
            if props["aliased"]
            else table.proj()
        )

    if self._key_source is None:
        parents = self.target.parents(primary=True, as_objects=True, foreign_key_info=True)
        if not parents:
            raise DataJointError(
                "A table must have dependencies from its primary key for auto-populate to work"
            )
        self._key_source = _rename_attributes(*parents[0])
        for q in parents[1:]:
            self._key_source *= _rename_attributes(*q)
    return self._key_source
```

### Custom Key Source

Subclasses may override `key_source` to change the scope or granularity of `make()` calls.

### Jobs To Do Computation

The `_jobs_to_do()` method (`autopopulate.py:171-197`):
1. Validates `key_source` is a `QueryExpression`
2. Verifies target table has all primary key attributes from `key_source`
3. Applies restrictions via `AndList`
4. Projects to primary key attributes only

```python
def _jobs_to_do(self, restrictions):
    todo = self.key_source
    # ... validation ...
    return (todo & AndList(restrictions)).proj()
```

The actual keys to populate are computed as:
```python
keys = (self._jobs_to_do(restrictions) - self.target).fetch("KEY", limit=limit)
```

This subtracts already-populated keys from the todo list.

## 2. Job Table Creation and Management

### Schema-Level Job Tables

Each schema has its own job reservation table named `~jobs`. The job table is created lazily when first accessed.

**Schema.jobs property** (`schemas.py:367-377`):
```python
@property
def jobs(self):
    """
    schema.jobs provides a view of the job reservation table for the schema
    """
    self._assert_exists()
    if self._jobs is None:
        self._jobs = JobTable(self.connection, self.database)
    return self._jobs
```

### JobTable Initialization

**JobTable.__init__** (`jobs.py:18-40`):
```python
def __init__(self, conn, database):
    self.database = database
    self._connection = conn
    self._heading = Heading(table_info=dict(
        conn=conn, database=database, table_name=self.table_name, context=None
    ))
    self._support = [self.full_table_name]

    self._definition = """    # job reservation table for `{database}`
        table_name  :varchar(255)  # className of the table
        key_hash  :char(32)  # key hash
        ---
        status  :enum('reserved','error','ignore')
        key=null  :<blob>  # structure containing the key
        error_message=""  :varchar({error_message_length})
        error_stack=null  :<blob>  # error stack if failed
        user="" :varchar(255)
        host=""  :varchar(255)
        pid=0  :int unsigned
        connection_id = 0  : bigint unsigned
        timestamp=CURRENT_TIMESTAMP  :timestamp
        """.format(database=database, error_message_length=ERROR_MESSAGE_LENGTH)
    if not self.is_declared:
        self.declare()
    self._user = self.connection.get_user()
```

The `~jobs` table is automatically declared (created) if it doesn't exist when the `JobTable` is instantiated.

### Schema Registration

When a schema is activated, it registers itself with the connection (`schemas.py:136`):
```python
self.connection.register(self)
```

**Connection.register** (`connection.py:222-224`):
```python
def register(self, schema):
    self.schemas[schema.database] = schema
    self.dependencies.clear()
```

This allows `populate()` to access the jobs table via:
```python
jobs = self.connection.schemas[self.target.database].jobs
```

### Job Table Name

The job table uses a special name prefixed with `~` (`jobs.py:47-48`):
```python
@property
def table_name(self):
    return "~jobs"
```

Tables prefixed with `~` are system tables excluded from `schema.list_tables()`.

## 3. Job Reservation System

### Job Table Structure

The `~jobs` table (`jobs.py:24-37`) stores job reservations:

| Attribute | Type | Description |
|-----------|------|-------------|
| `table_name` | varchar(255) | Full table name (`database.table_name`) |
| `key_hash` | char(32) | MD5 hash of primary key dict |
| `status` | enum | `'reserved'`, `'error'`, or `'ignore'` |
| `key` | blob | Pickled key dict |
| `error_message` | varchar(2047) | Truncated error message |
| `error_stack` | blob | Full stack trace |
| `user` | varchar(255) | Database user |
| `host` | varchar(255) | System hostname |
| `pid` | int unsigned | Process ID |
| `connection_id` | bigint unsigned | MySQL connection ID |
| `timestamp` | timestamp | Automatic timestamp |

### Reservation Flow

**Reserve** (`jobs.py:58-81`):
```python
def reserve(self, table_name, key):
    job = dict(
        table_name=table_name,
        key_hash=key_hash(key),
        status="reserved",
        host=platform.node(),
        pid=os.getpid(),
        connection_id=self.connection.connection_id,
        key=key,
        user=self._user,
    )
    try:
        self.insert1(job, ignore_extra_fields=True)
    except DuplicateError:
        return False
    return True
```

Atomicity is guaranteed by MySQL's unique constraint on `(table_name, key_hash)`.

**Complete** (`jobs.py:113-121`):
```python
def complete(self, table_name, key):
    job_key = dict(table_name=table_name, key_hash=key_hash(key))
    (self & job_key).delete_quick()
```

**Error** (`jobs.py:123-150`):
```python
def error(self, table_name, key, error_message, error_stack=None):
    if len(error_message) > ERROR_MESSAGE_LENGTH:
        error_message = error_message[:ERROR_MESSAGE_LENGTH - len(TRUNCATION_APPENDIX)] + TRUNCATION_APPENDIX
    self.insert1(
        dict(
            table_name=table_name,
            key_hash=key_hash(key),
            status="error",
            # ... metadata ...
            error_message=error_message,
            error_stack=error_stack,
        ),
        replace=True,
    )
```

**Ignore** (`jobs.py:83-111`):
```python
def ignore(self, table_name, key):
    job = dict(
        table_name=table_name,
        key_hash=key_hash(key),
        status="ignore",
        # ... metadata ...
    )
    try:
        self.insert1(job, ignore_extra_fields=True)
    except DuplicateError:
        return False
    return True
```

### Job Filtering in Populate

Before populating, keys with existing job entries are excluded (`autopopulate.py:257-261`):
```python
if reserve_jobs:
    exclude_key_hashes = (
        jobs & {"table_name": self.target.table_name} & 'status in ("error", "ignore", "reserved")'
    ).fetch("key_hash")
    keys = [key for key in keys if key_hash(key) not in exclude_key_hashes]
```

### Job Table Maintenance

The `JobTable` class provides simplified `delete()` and `drop()` methods (`jobs.py:50-56`):
```python
def delete(self):
    """bypass interactive prompts and dependencies"""
    self.delete_quick()

def drop(self):
    """bypass interactive prompts and dependencies"""
    self.drop_quick()
```

These bypass normal safety prompts since the jobs table is a system table.

## 4. Make Method Invocation

### Make Method Contract

The `make(key)` method must perform three steps:
1. **Fetch**: Retrieve data from parent tables, restricted by key
2. **Compute**: Calculate secondary attributes from fetched data
3. **Insert**: Insert new tuple(s) into the target table

### Two Implementation Patterns

#### Pattern A: Regular Method

All three steps execute within a single database transaction.

**Execution flow** (`autopopulate.py:340-355`):
```python
if not is_generator:
    self.connection.start_transaction()
    # ... key existence check ...
    make(dict(key), **(make_kwargs or {}))
```

#### Pattern B: Generator (Tripartite) Method

Separates computation from transaction to allow long-running computation outside the transaction window.

**Required methods**:
- `make_fetch(key)` - All database queries
- `make_compute(key, *fetched_data)` - All computation
- `make_insert(key, *computed_result)` - All inserts

**Default generator implementation** (`autopopulate.py:140-152`):
```python
def make(self, key):
    fetched_data = self.make_fetch(key)
    computed_result = yield fetched_data

    if computed_result is None:
        computed_result = self.make_compute(key, *fetched_data)
        yield computed_result

    self.make_insert(key, *computed_result)
    yield
```

**Execution flow** (`autopopulate.py:356-370`):
```python
# Phase 1: Fetch and compute OUTSIDE transaction
gen = make(dict(key), **(make_kwargs or {}))
fetched_data = next(gen)
fetch_hash = deepdiff.DeepHash(fetched_data, ignore_iterable_order=False)[fetched_data]
computed_result = next(gen)

# Phase 2: Verify and insert INSIDE transaction
self.connection.start_transaction()
gen = make(dict(key), **(make_kwargs or {}))  # restart
fetched_data = next(gen)
if fetch_hash != deepdiff.DeepHash(fetched_data, ignore_iterable_order=False)[fetched_data]:
    raise DataJointError("Referential integrity failed! The `make_fetch` data has changed")
gen.send(computed_result)  # insert
```

The deep hash comparison ensures data integrity by detecting concurrent modifications.

### Legacy Support

The legacy `_make_tuples` method name is supported (`autopopulate.py:333`):
```python
make = self._make_tuples if hasattr(self, "_make_tuples") else self.make
```

### Insert Protection

Direct inserts into auto-populated tables are blocked outside `make()` (`autopopulate.py:351, 402`):
```python
self.__class__._allow_insert = True
try:
    # ... make() execution ...
finally:
    self.__class__._allow_insert = False
```

The `Table.insert()` method checks this flag and raises `DataJointError` if insert is attempted outside the populate context (unless `allow_direct_insert=True`).

## 5. Transaction Management

### Transaction Lifecycle

**Start** (`connection.py:322-327`):
```python
def start_transaction(self):
    if self.in_transaction:
        raise DataJointError("Nested transactions are not supported.")
    self.query("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    self._in_transaction = True
```

Uses MySQL's `WITH CONSISTENT SNAPSHOT` for repeatable read isolation.

**Commit** (`connection.py:337-343`):
```python
def commit_transaction(self):
    self.query("COMMIT")
    self._in_transaction = False
```

**Cancel/Rollback** (`connection.py:329-335`):
```python
def cancel_transaction(self):
    self.query("ROLLBACK")
    self._in_transaction = False
```

### Transaction Rules

1. **No nested transactions** - `populate()` cannot be called during an existing transaction (`autopopulate.py:237-238`)
2. **Regular make**: Transaction spans entire `make()` execution
3. **Generator make**: Transaction spans only the final fetch verification and insert phase

## 6. Error Management

### Error Handling Flow

(`autopopulate.py:372-402`):

```python
try:
    # ... make() execution ...
except (KeyboardInterrupt, SystemExit, Exception) as error:
    try:
        self.connection.cancel_transaction()
    except LostConnectionError:
        pass  # Connection lost during rollback

    error_message = "{exception}{msg}".format(
        exception=error.__class__.__name__,
        msg=": " + str(error) if str(error) else "",
    )

    if jobs is not None:
        jobs.error(
            self.target.table_name,
            self._job_key(key),
            error_message=error_message,
            error_stack=traceback.format_exc(),
        )

    if not suppress_errors or isinstance(error, SystemExit):
        raise
    else:
        logger.error(error)
        return key, error if return_exception_objects else error_message
else:
    self.connection.commit_transaction()
    if jobs is not None:
        jobs.complete(self.target.table_name, self._job_key(key))
    return True
```

### Error Suppression

When `suppress_errors=True`:
- Errors are logged to the jobs table
- Errors are collected and returned instead of raised
- `SystemExit` is never suppressed (for graceful SIGTERM handling)

### SIGTERM Handling

When `reserve_jobs=True`, a SIGTERM handler is installed (`autopopulate.py:245-251`):
```python
def handler(signum, frame):
    logger.info("Populate terminated by SIGTERM")
    raise SystemExit("SIGTERM received")

old_handler = signal.signal(signal.SIGTERM, handler)
```

This allows graceful termination of long-running populate jobs.

## 7. Populate Method Interface

### Full Signature

```python
def populate(
    self,
    *restrictions,
    keys=None,
    suppress_errors=False,
    return_exception_objects=False,
    reserve_jobs=False,
    order="original",
    limit=None,
    max_calls=None,
    display_progress=False,
    processes=1,
    make_kwargs=None,
):
```

### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `*restrictions` | various | - | Restrictions AND-ed to filter `key_source` |
| `keys` | list[dict] | None | Explicit keys to populate (bypasses `key_source`) |
| `suppress_errors` | bool | False | Collect errors instead of raising |
| `return_exception_objects` | bool | False | Return exception objects vs. strings |
| `reserve_jobs` | bool | False | Enable job reservation for distributed processing |
| `order` | str | "original" | Key order: "original", "reverse", "random" |
| `limit` | int | None | Max keys to fetch from `key_source` |
| `max_calls` | int | None | Max `make()` calls to execute |
| `display_progress` | bool | False | Show progress bar |
| `processes` | int | 1 | Number of worker processes |
| `make_kwargs` | dict | None | Non-computation kwargs passed to `make()` |

### Return Value

```python
{
    "success_count": int,  # Number of successful make() calls
    "error_list": list,    # List of (key, error) tuples if suppress_errors=True
}
```

## 8. Multiprocessing Support

### Process Initialization

(`autopopulate.py:27-36`):
```python
def _initialize_populate(table, jobs, populate_kwargs):
    process = mp.current_process()
    process.table = table
    process.jobs = jobs
    process.populate_kwargs = populate_kwargs
    table.connection.connect()  # reconnect
```

### Connection Handling

Before forking (`autopopulate.py:296-297`):
```python
self.connection.close()  # Disconnect parent
del self.connection._conn.ctx  # SSLContext not pickleable
```

After workers complete (`autopopulate.py:311`):
```python
self.connection.connect()  # Reconnect parent
```

### Worker Execution

```python
def _call_populate1(key):
    process = mp.current_process()
    return process.table._populate1(key, process.jobs, **process.populate_kwargs)
```

Uses `Pool.imap()` with `chunksize=1` for ordered execution with progress tracking.

## 9. Return Values from _populate1

| Value | Meaning |
|-------|---------|
| `True` | Successfully completed `make()` and inserted data |
| `False` | Key already exists in target OR job reservation failed |
| `(key, error)` | Error occurred (when `suppress_errors=True`) |

## 10. Key Observations

### Strengths

1. **Atomic job reservation** via MySQL unique constraints
2. **Generator pattern** allows long computation outside transactions
3. **Deep hash verification** ensures data consistency
4. **Graceful shutdown** via SIGTERM handling
5. **Error persistence** in jobs table for debugging
6. **Per-schema job tables** allow independent job management

### Limitations (Addressed in 2.0)

The following limitations are documented in GitHub issue [#1258](https://github.com/datajoint/datajoint-python/issues/1258) and related issues.

#### Job Table Design Issues

1. **Limited status tracking**: Only `reserved`, `error`, and `ignore` statuses. No explicit tracking of pending jobs or successful completions.

2. **Functions as error log**: Cannot track pending or completed jobs efficiently. Finding pending jobs requires computing `key_source - target - jobs` each time.

3. **Poor dashboard visibility**: No way to monitor pipeline progress without querying multiple tables and computing set differences. See [#873](https://github.com/datajoint/datajoint-python/issues/873).

4. **Key hashing obscures data**: Primary keys stored as 32-character MD5 hashes. Actual keys stored as pickled blobs requiring deserialization to inspect.

5. **No referential integrity**: Jobs table is independent of computed tables. Orphaned jobs accumulate when upstream data is deleted.

6. **Schema-level scope**: All computed tables share one jobs table. Filtering by `table_name` required for all operations.

#### Key Source Issues

1. **Frequent manual modifications**: Subset operations require modifying `key_source` in Python code. No database-level persistence.

2. **Local visibility only**: Custom key sources not accessible database-wide. See discussion in [#1258](https://github.com/datajoint/datajoint-python/issues/1258).

3. **Performance bottleneck**: Multiple workers querying `key_source` simultaneously strains database. See [#749](https://github.com/datajoint/datajoint-python/issues/749).

4. **Codebase dependency**: Requires full pipeline codebase to determine pending work. Cannot query job status from SQL alone.

#### Missing Features

1. **No priority system**: Jobs processed in fetch order only (original, reverse, random).

2. **No scheduling**: Cannot delay job execution to a future time.

3. **No duration tracking**: No record of how long jobs take to complete.

4. **No version tracking**: No record of which code version processed a job.

5. **Simple retry logic**: Failed jobs stay in `error` status until manually cleared.

6. **No stale job cleanup**: Jobs referencing deleted upstream data remain indefinitely.

7. **No orphaned job handling**: Reserved jobs from crashed workers remain forever. See [#665](https://github.com/datajoint/datajoint-python/issues/665).

#### Populate Parameter Confusion

The `limit` vs `max_calls` parameters have confusing behavior. See [#1203](https://github.com/datajoint/datajoint-python/issues/1203):
- `limit`: Applied before excluding reserved/error jobs (can result in no work even when jobs available)
- `max_calls`: Applied after excluding reserved/error jobs (usually what users expect)

## 11. Related GitHub Issues

| Issue | Title | Status |
|-------|-------|--------|
| [#1258](https://github.com/datajoint/datajoint-python/issues/1258) | FEAT: Autopopulate 2.0 | Open |
| [#1203](https://github.com/datajoint/datajoint-python/issues/1203) | Unexpected behaviour of `limit` in populate() | Open |
| [#749](https://github.com/datajoint/datajoint-python/issues/749) | Strain on MySQL with expensive key-source | Closed |
| [#873](https://github.com/datajoint/datajoint-python/issues/873) | Provide way to list specific jobs | Closed |
| [#665](https://github.com/datajoint/datajoint-python/issues/665) | Cluster support - machine failures | Closed |
