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
conn.config.display_limit = 25

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

1. Add `thread_safe: bool = False` field to `Config` with `DJ_THREAD_SAFE` env alias
2. Make `thread_safe` always read-only after initialization
3. When `thread_safe=True`, make `dj.config` writes raise `ThreadSafetyError`
4. Add guard to `dj.conn()`
5. Add guard to `Schema.__init__` when `connection=None`
6. Add `conn.config` to `Connection` that copies from global `dj.config`
7. Add `ThreadSafetyError` exception

## Exceptions

```python
class ThreadSafetyError(DataJointError):
    """Raised when modifying global state in thread-safe mode."""
```

Error messages:
- Config write: `"Global config is read-only in thread-safe mode. Use conn.config for connection-scoped settings."`
- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use Connection() with explicit parameters."`
- Schema without connection: `"Schema requires explicit connection in thread-safe mode."`
