# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Add `thread_safe` mode that makes global config read-only and requires explicit connections with mutable connection-scoped settings.

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

Creates a connection with explicit configuration. Works in both modes.

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
- Any other setting (e.g., `safemode`, `display_limit`, `stores`)

**Config creation:** Copies global `dj.config`, then applies kwargs. Creates `conn.config` which is always mutable.

```python
conn = dj.Connection.from_config(host="localhost", user="u", password="p")
conn.config.safemode = False      # Always OK: conn.config is mutable
conn.config.display_limit = 25    # Always OK
```

## Behavior

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` read | Works | Works (read-only) |
| `dj.config` write | Works | Raises `ThreadSafetyError` |
| `dj.conn()` | Works | Raises `ThreadSafetyError` |
| `dj.Schema("name")` | Works | Raises `ThreadSafetyError` |
| `Connection.from_config()` | Works | Works |
| `conn.config` read/write | Works | Works |
| `Schema(..., connection=conn)` | Works | Works |

## Read-Only Settings

- `thread_safe`: Always read-only after initialization (set via env var or config file only)
- All of `dj.config`: Read-only when `thread_safe=True`

## Implementation

1. Add `thread_safe: bool = False` field to `Config` with `DJ_THREAD_SAFE` env alias
2. Make `thread_safe` always read-only after initialization
3. When `thread_safe=True`, make all `dj.config` writes raise `ThreadSafetyError`
4. Add guard to `dj.conn()`
5. Add guard to `Schema.__init__` when `connection=None`
6. Add `Connection.from_config()` class method that:
   - Copies global `dj.config`
   - Applies kwargs overrides
   - Creates mutable `conn.config`
7. Add `ThreadSafetyError` exception

## Exceptions

```python
class ThreadSafetyError(DataJointError):
    """Raised when modifying global state in thread-safe mode."""
```

Error messages:
- Config write: `"Global config is read-only in thread-safe mode. Use conn.config for connection-scoped settings."`
- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use Connection.from_config()."`
- Schema without connection: `"Schema requires explicit connection in thread-safe mode."`
