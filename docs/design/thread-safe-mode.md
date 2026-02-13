# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Add `thread_safe` mode that makes global config read-only and provides connection-scoped mutable settings via `conn.config`.

## API

### Enable Thread-Safe Mode

Set via environment variable or config file (read-only after initialization):

```bash
export DJ_THREAD_SAFE=true
```

```json
// datajoint.json
{"thread_safe": true}
```

### Create Connections

```python
conn = dj.Connection(
    host="localhost",
    user="user",
    password="password",
)

# Modify settings per-connection
conn.config.safemode = False
conn.config.display.limit = 25

schema = dj.Schema("my_schema", connection=conn)
```

### conn.config

Every connection has a `config` attribute that:
- Copies from global `dj.config` at connection time
- Is always mutable (even in thread-safe mode)
- Provides connection-scoped settings

```python
conn.config.safemode         # Read setting
conn.config.safemode = False # Write setting (always allowed)
conn.config.stores = {...}   # Configure stores for this connection
```

## Behavior

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` read | Works | Works |
| `dj.config` write | Works | Raises `ThreadSafetyError` |
| `dj.conn()` | Works | Raises `ThreadSafetyError` |
| `dj.Schema("name")` | Works | Raises `ThreadSafetyError` |
| `dj.Connection(...)` | Works | Works |
| `conn.config` read/write | Works | Works |
| `Schema(..., connection=conn)` | Works | Works |

## Read-Only Settings

- `thread_safe`: Always read-only (set via env var or config file only)
- All of `dj.config`: Read-only when `thread_safe=True`

## Implementation

### 1. Add thread_safe setting
- Add `thread_safe: bool = False` field to `Config` with `DJ_THREAD_SAFE` env alias
- Make `thread_safe` always read-only after initialization
- When `thread_safe=True`, make `dj.config` writes raise `ThreadSafetyError`

### 2. Add guards for global state
- `dj.conn()`: Raise `ThreadSafetyError` when `thread_safe=True`
- `Schema.__init__`: Raise `ThreadSafetyError` when `connection=None` and `thread_safe=True`

### 3. Add conn.config
- `Connection.__init__`: Create `self.config` as copy of global `dj.config`
- `conn.config` is always mutable

### 4. Refactor internal code to use conn.config

All runtime operations must use `self.connection.config` instead of global `config`:

**table.py:**
- `Table.delete()`: Use `self.connection.config.safemode`
- `Table.drop()`: Use `self.connection.config.safemode`

**schemas.py:**
- `Schema.drop()`: Use `self.connection.config.safemode`
- `Schema.__init__`: Use `self.connection.config.database.create_tables`

**preview.py:**
- Use `connection.config.display.limit`
- Use `connection.config.display.width`
- Use `connection.config.display.show_tuple_count`
- Note: Preview functions need connection passed in or accessed via table

**diagram.py:**
- Use `schema.connection.config.display.diagram_direction`

**jobs.py:**
- Use `self.connection.config.jobs.*` for all jobs settings
- `version_method`, `default_priority`, `stale_timeout`, `keep_completed`

**autopopulate.py:**
- Use `self.connection.config.jobs.allow_new_pk_fields_in_computed_tables`
- Use `self.connection.config.jobs.auto_refresh`

**declare.py:**
- Use `connection.config.jobs.add_job_metadata`

**connection.py:**
- Use `self.config.database.reconnect` for reconnect behavior
- Use `self.config.query_cache` for query caching

**hash_registry.py, staged_insert.py, builtin_codecs/\*:**
- Use `connection.config.get_store_spec()` for store configuration
- Use `connection.config.download_path` for downloads

### 5. Add ThreadSafetyError exception

```python
class ThreadSafetyError(DataJointError):
    """Raised when modifying global state in thread-safe mode."""
```

## Error Messages

- Config write: `"Global config is read-only in thread-safe mode. Use conn.config for connection-scoped settings."`
- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use Connection() with explicit parameters."`
- Schema without connection: `"Schema requires explicit connection in thread-safe mode."`
