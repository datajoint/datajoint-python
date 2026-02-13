# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Introduce **instance** objects that encapsulate config and connection. The `dj` module provides access to a lazily-loaded singleton instance. New isolated instances are created with `dj.instance()`.

## API

### Legacy API (singleton instance)

```python
import datajoint as dj

dj.config.safemode = False
dj.conn(host="localhost", user="u", password="p")
schema = dj.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

Internally, `dj.config`, `dj.conn()`, and `dj.Schema()` delegate to a lazily-loaded singleton instance.

### New API (isolated instance)

```python
import datajoint as dj

inst = dj.instance(
    host="localhost",
    user="user",
    password="password",
)
inst.config.safemode = False
schema = inst.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

### Instance structure

Each instance has:
- `inst.config` - Config (created fresh at instance creation)
- `inst.connection` - Connection (created at instance creation)
- `inst.Schema()` - Schema factory using instance's connection

```python
inst = dj.instance(host="localhost", user="u", password="p")
inst.config            # Config instance
inst.connection        # Connection instance
inst.Schema("name")    # Creates schema using inst.connection
```

### Thread-safe mode

```bash
export DJ_THREAD_SAFE=true
```

`thread_safe` is read from environment/config file at module import time.

When `thread_safe=True`, accessing the singleton raises `ThreadSafetyError`:
- `dj.config` raises `ThreadSafetyError`
- `dj.conn()` raises `ThreadSafetyError`
- `dj.Schema()` raises `ThreadSafetyError`
- `dj.instance()` works - isolated instances are always allowed

```python
# thread_safe=True

dj.config               # ThreadSafetyError
dj.conn()               # ThreadSafetyError
dj.Schema("name")       # ThreadSafetyError

inst = dj.instance(host="h", user="u", password="p")  # OK
inst.config.safemode = False  # OK
inst.Schema("name")           # OK
```

## Behavior Summary

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` | Singleton config | `ThreadSafetyError` |
| `dj.conn()` | Singleton connection | `ThreadSafetyError` |
| `dj.Schema()` | Uses singleton | `ThreadSafetyError` |
| `dj.instance()` | Works | Works |
| `inst.config` | Works | Works |
| `inst.connection` | Works | Works |
| `inst.Schema()` | Works | Works |

## Singleton Lazy Loading

The singleton instance is created lazily on first access to `dj.config`, `dj.conn()`, or `dj.Schema()`:

```python
# First access triggers singleton creation
dj.config.safemode      # Creates singleton, returns singleton.config.safemode
dj.conn()               # Returns singleton.connection (connects if needed)
dj.Schema("name")       # Returns singleton.Schema("name")
```

## Usage Example

```python
import datajoint as dj

# Create isolated instance
inst = dj.instance(
    host="localhost",
    user="user",
    password="password",
)

# Configure
inst.config.safemode = False
inst.config.stores = {"raw": {"protocol": "file", "location": "/data"}}

# Create schema
schema = inst.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = """
    mouse_id: int
    """

# Use tables
Mouse().insert1({"mouse_id": 1})
Mouse().delete()  # Uses inst.config.safemode
```

## Implementation

### 1. Create Instance class

```python
class Instance:
    def __init__(self, host, user, password, port=3306, **kwargs):
        self.config = Config()  # Fresh config with defaults
        # Apply any config overrides from kwargs
        self.connection = Connection(host, user, password, port, ...)
        self.connection._config = self.config

    def Schema(self, name, **kwargs):
        return Schema(name, connection=self.connection, **kwargs)
```

### 2. Add dj.instance()

```python
def instance(host, user, password, **kwargs) -> Instance:
    """Create a new isolated instance with its own config and connection."""
    return Instance(host, user, password, **kwargs)
```

### 3. Singleton with lazy loading

```python
# Module level
_thread_safe = _load_thread_safe_from_env_or_config()
_singleton = None

def _get_singleton():
    if _thread_safe:
        raise ThreadSafetyError(
            "Global DataJoint state is disabled in thread-safe mode. "
            "Use dj.instance() to create an isolated instance."
        )
    global _singleton
    if _singleton is None:
        _singleton = Instance(
            host=_load_from_config("database.host"),
            user=_load_from_config("database.user"),
            password=_load_from_config("database.password"),
            ...
        )
    return _singleton

# Public API
@property
def config():
    return _get_singleton().config

def conn():
    return _get_singleton().connection

def Schema(name, **kwargs):
    return _get_singleton().Schema(name, **kwargs)
```

### 4. Refactor internal code

All internal code uses `self.connection._config` instead of global `config`:
- Tables access config via `self.connection._config`
- This works uniformly for both singleton and isolated instances

## Error Messages

- Singleton access: `"Global DataJoint state is disabled in thread-safe mode. Use dj.instance() to create an isolated instance."`
