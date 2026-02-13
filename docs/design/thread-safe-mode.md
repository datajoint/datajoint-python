# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Introduce **context** objects that encapsulate config and connection. The `dj` module itself is the singleton (legacy) context. New isolated contexts are created with `dj.new()`.

## API

### Legacy API (singleton context)

The `dj` module acts as the default singleton context:

```python
import datajoint as dj

dj.config.safemode = False
dj.conn(host="localhost", user="u", password="p")
schema = dj.Schema("my_schema")  # Uses dj's connection

@schema
class Mouse(dj.Manual):
    definition = "..."
```

### New API (isolated context)

Create isolated contexts with `dj.new()`:

```python
import datajoint as dj

ctx = dj.new()  # New context with its own config copy
ctx.config.safemode = False
ctx.connect(host="localhost", user="u", password="p")
schema = ctx.Schema("my_schema")  # Uses ctx's connection

@schema
class Mouse(ctx.Manual):
    definition = "..."
```

### Context structure

Each context has:
- **One config** - copy of settings at creation time
- **One connection** - established via `ctx.connect()`
- **Schema factory** - `ctx.Schema()` auto-uses context's connection
- **Table base classes** - `ctx.Manual`, `ctx.Lookup`, `ctx.Imported`, `ctx.Computed`, `ctx.Part`

```python
ctx = dj.new()
ctx.config          # Config instance (copy of dj.config at creation)
ctx.connect(...)    # Establish connection
ctx.Schema(...)     # Create schema using ctx's connection
ctx.Manual          # Base class for manual tables
ctx.Lookup          # Base class for lookup tables
ctx.Imported        # Base class for imported tables
ctx.Computed        # Base class for computed tables
ctx.Part            # Base class for part tables
```

### Thread-safe mode

```bash
export DJ_THREAD_SAFE=true
```

When `thread_safe=True`:
- `dj.conn()` raises `ThreadSafetyError`
- `dj.Schema()` raises `ThreadSafetyError`
- `dj.config` is read-only
- `dj.new()` works - isolated contexts are always allowed

```python
# thread_safe=True

dj.Schema("name")           # ThreadSafetyError
dj.conn()                   # ThreadSafetyError
dj.config.safemode = False  # ThreadSafetyError

ctx = dj.new()              # OK - isolated context
ctx.config.safemode = False # OK - context's own config
ctx.connect(...)            # OK
ctx.Schema("name")          # OK
```

## Behavior Summary

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` read | Works | Works |
| `dj.config` write | Works | `ThreadSafetyError` |
| `dj.conn()` | Works | `ThreadSafetyError` |
| `dj.Schema()` | Works | `ThreadSafetyError` |
| `dj.new()` | Works | Works |
| `ctx.config` read/write | Works | Works |
| `ctx.connect()` | Works | Works |
| `ctx.Schema()` | Works | Works |

## Context Lifecycle

```python
# Create context
ctx = dj.new()

# Configure
ctx.config.database.host = "localhost"
ctx.config.safemode = False
ctx.config.stores = {...}

# Connect
ctx.connect(
    host="localhost",  # Or use ctx.config.database.host
    user="user",
    password="password",
)

# Use
schema = ctx.Schema("my_schema")

@schema
class Mouse(ctx.Manual):
    definition = """
    mouse_id: int
    """

Mouse().insert1({"mouse_id": 1})

# Cleanup (optional - closes connection)
ctx.close()
```

## Legacy Compatibility

The singleton `dj` context works exactly as before:

```python
# These are equivalent:
dj.conn()           # Singleton connection
dj.config           # Singleton config
dj.Schema("name")   # Uses singleton connection

# Internally, dj module delegates to singleton context
```

## Implementation

### 1. Create Context class

```python
class Context:
    def __init__(self, config: Config):
        self.config = config
        self._connection = None

    def connect(self, host, user, password, ...):
        self._connection = Connection(...)
        self._connection.config = self.config

    def conn(self):
        return self._connection

    def Schema(self, name, ...):
        return Schema(name, connection=self._connection, ...)

    # Table base classes that reference this context
    @property
    def Manual(self): ...
    @property
    def Lookup(self): ...
    # etc.
```

### 2. Add dj.new()

```python
def new() -> Context:
    """Create a new isolated context with its own config and connection."""
    config_copy = copy(config)  # Copy current global config
    return Context(config_copy)
```

### 3. Make dj module act as singleton context

```python
# In datajoint/__init__.py
_singleton_context = Context(config)

def conn(...):
    if config.thread_safe:
        raise ThreadSafetyError(...)
    return _singleton_context.conn(...)

def Schema(...):
    if config.thread_safe:
        raise ThreadSafetyError(...)
    return _singleton_context.Schema(...)
```

### 4. Add thread_safe guards

- `dj.conn()`: Raise `ThreadSafetyError` when `thread_safe=True`
- `dj.Schema()`: Raise `ThreadSafetyError` when `thread_safe=True`
- `dj.config` writes: Raise `ThreadSafetyError` when `thread_safe=True`

### 5. Refactor internal code

All internal code uses `self.connection.config` instead of global `config`:
- Tables access config via `self.connection.config`
- Connection has reference to its context's config

## Error Messages

- `dj.conn()`: `"dj.conn() is disabled in thread-safe mode. Use ctx = dj.new() to create an isolated context."`
- `dj.Schema()`: `"dj.Schema() is disabled in thread-safe mode. Use ctx = dj.new() to create an isolated context."`
- `dj.config` write: `"Global config is read-only in thread-safe mode. Use ctx = dj.new() for isolated config."`
