# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Introduce **context** objects that encapsulate config and connection. The `dj` module provides the singleton (legacy) context. New isolated contexts are created with `dj.new()`.

## API

### Legacy API (singleton context)

```python
import datajoint as dj

dj.config.safemode = False
dj.conn(host="localhost", user="u", password="p")
schema = dj.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

### New API (isolated context)

```python
import datajoint as dj

ctx = dj.new(
    host="localhost",
    user="user",
    password="password",
)
ctx.config.safemode = False
schema = ctx.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

### Context structure

Each context exposes only:
- `ctx.config` - Config instance (copy of `dj.config` at creation)
- `ctx.connection` - Connection (created at context construction)
- `ctx.Schema()` - Schema factory using context's connection

```python
ctx = dj.new(host="localhost", user="u", password="p")
ctx.config            # Config instance
ctx.connection        # Connection instance
ctx.Schema("name")    # Creates schema using ctx.connection
```

### Thread-safe mode

```bash
export DJ_THREAD_SAFE=true
```

When `thread_safe=True`:
- `dj.conn()` raises `ThreadSafetyError`
- `dj.Schema()` raises `ThreadSafetyError`
- `dj.config` only allows access to `thread_safe` (all other access raises `ThreadSafetyError`)
- `dj.new()` works - isolated contexts are always allowed

```python
# thread_safe=True

dj.config.thread_safe       # OK - allowed
dj.config.safemode          # ThreadSafetyError
dj.config.safemode = False  # ThreadSafetyError
dj.conn()                   # ThreadSafetyError
dj.Schema("name")           # ThreadSafetyError

ctx = dj.new(host="h", user="u", password="p")  # OK
ctx.config.safemode = False  # OK
ctx.Schema("name")           # OK
```

## Behavior Summary

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config.thread_safe` | Works | Works |
| `dj.config.*` (other) | Works | `ThreadSafetyError` |
| `dj.conn()` | Works | `ThreadSafetyError` |
| `dj.Schema()` | Works | `ThreadSafetyError` |
| `dj.new()` | Works | Works |
| `ctx.config.*` | Works | Works |
| `ctx.connection` | Works | Works |
| `ctx.Schema()` | Works | Works |

## Usage Example

```python
import datajoint as dj

# Create isolated context
ctx = dj.new(
    host="localhost",
    user="user",
    password="password",
)

# Configure
ctx.config.safemode = False
ctx.config.stores = {"raw": {"protocol": "file", "location": "/data"}}

# Create schema
schema = ctx.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = """
    mouse_id: int
    """

@schema
class Session(dj.Manual):
    definition = """
    -> Mouse
    session_date: date
    """

# Use tables
Mouse().insert1({"mouse_id": 1})
Mouse().delete()  # Uses ctx.config.safemode
```

## Implementation

### 1. Create Context class

```python
class Context:
    def __init__(self, host, user, password, port=3306, ...):
        self.config = copy(dj.config)  # Independent config copy
        self.connection = Connection(host, user, password, port, ...)
        self.connection._config = self.config  # Link config to connection

    def Schema(self, name, **kwargs):
        return Schema(name, connection=self.connection, **kwargs)
```

### 2. Add dj.new()

```python
def new(host, user, password, **kwargs) -> Context:
    """Create a new isolated context with its own config and connection."""
    return Context(host, user, password, **kwargs)
```

### 3. Add thread_safe guards

In `dj.config`:
- Allow read/write of `thread_safe` always
- When `thread_safe=True`, block all other attribute access

```python
def __getattr__(self, name):
    if name == "thread_safe":
        return self._thread_safe
    if self._thread_safe:
        raise ThreadSafetyError("Global config is inaccessible in thread-safe mode.")
    # ... normal access
```

### 4. Refactor internal code

All internal code uses `self.connection._config` instead of global `config`:
- Tables access config via `self.connection._config`
- This works uniformly for both singleton and isolated contexts

## Error Messages

- `dj.config.*`: `"Global config is inaccessible in thread-safe mode. Use ctx = dj.new(...) for isolated config."`
- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use ctx = dj.new(...) to create an isolated context."`
- `dj.Schema()`: `"dj.Schema() is disabled in thread-safe mode. Use ctx = dj.new(...) to create an isolated context."`
