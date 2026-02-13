# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Add `thread_safe` mode that blocks global state access and requires explicit connection configuration.

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

### Connection.from_config()

Creates a connection with explicit configuration. Works in both `thread_safe=True` and `thread_safe=False` modes.

```python
conn = dj.Connection.from_config(
    host="localhost",
    user="user",
    password="password",
    safemode=False,
    display_limit=25,
)
schema = dj.Schema("my_schema", connection=conn)
```

**Parameters:**
- `host` (required): Database hostname
- `user` (required): Database username
- `password` (required): Database password
- `port`: Database port (default: 3306)
- Any other setting from `dj.config` (e.g., `safemode`, `display_limit`, `stores`)

**Defaults:** Settings not explicitly provided use hardcoded defaults (same as `dj.config` defaults). Global `dj.config` is never accessed.

**Connection-scoped settings:** Stored on `conn.config` and accessed as `conn.config.safemode`, `conn.config.display_limit`, etc.

```python
conn = dj.Connection.from_config(host="localhost", user="u", password="p")
conn.config.safemode      # True (default)
conn.config.display_limit # 12 (default)

conn.config.safemode = False  # Modify for this connection only
```

## Behavior

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config.X` | Works | Raises `ThreadSafetyError` |
| `dj.conn()` | Works | Raises `ThreadSafetyError` |
| `dj.Schema("name")` | Works | Raises `ThreadSafetyError` |
| `Connection.from_config()` | Works | Works |
| `Schema(..., connection=conn)` | Works | Works |

## Read-Only Settings

Only `thread_safe` is read-only after initialization. It can only be set via:
- Environment variable `DJ_THREAD_SAFE`
- Config file `datajoint.json`

## Implementation

1. Add `thread_safe: bool = False` field to `Config` with `DJ_THREAD_SAFE` env alias
2. Make `thread_safe` read-only after `Config` initialization
3. Add guards to `Config.__getattr__`, `Config.__setattr__`, `Config.__getitem__`, `Config.__setitem__`
4. Add guard to `dj.conn()`
5. Add guard to `Schema.__init__` when `connection=None`
6. Add `Connection.from_config()` class method that:
   - Accepts all connection params and settings as kwargs
   - Uses hardcoded defaults (never accesses global config)
   - Creates `conn.config` object to store connection-scoped settings
7. Add `ThreadSafetyError` exception

## Exceptions

```python
class ThreadSafetyError(DataJointError):
    """Raised when accessing global state in thread-safe mode."""
```

Error messages:
- Config access: `"Global config is inaccessible in thread-safe mode. Use Connection.from_config() with explicit configuration."`
- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use Connection.from_config() with explicit configuration."`
- Schema without connection: `"Schema requires explicit connection in thread-safe mode. Use Schema(..., connection=conn)."`
