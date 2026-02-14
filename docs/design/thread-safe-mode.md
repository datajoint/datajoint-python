# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Introduce **Instance** objects that encapsulate config and connection. The `dj` module provides access to a lazily-loaded singleton instance. New isolated instances are created with `dj.Instance()`.

## API

### Legacy API (singleton instance)

```python
import datajoint as dj

dj.config.safemode = False
dj.conn()  # Triggers singleton creation, returns connection
schema = dj.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

Internally, `dj.config`, `dj.conn()`, and `dj.Schema()` are aliases to the singleton instance:
- `dj.config` → `dj._singleton_instance.config`
- `dj.conn()` → `dj._singleton_instance.connection`
- `dj.Schema()` → `dj._singleton_instance.Schema()`

The singleton is created lazily on first access to any of these.

### New API (isolated instance)

```python
import datajoint as dj

inst = dj.Instance(
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
- `inst.FreeTable()` - FreeTable factory using instance's connection

```python
inst = dj.Instance(host="localhost", user="u", password="p")
inst.config            # Config instance
inst.connection        # Connection instance
inst.Schema("name")    # Creates schema using inst.connection
inst.FreeTable("db.tbl")  # Access table using inst.connection
```

### Table base classes vs instance methods

**Base classes** (`dj.Manual`, `dj.Lookup`, etc.) - Used with `@schema` decorator:
```python
@schema
class Mouse(dj.Manual):  # dj.Manual - schema links to connection
    definition = "..."
```

**Instance methods** (`inst.Schema()`, `inst.FreeTable()`) - Need connection directly:
```python
schema = inst.Schema("my_schema")     # Uses inst.connection
table = inst.FreeTable("db.table")    # Uses inst.connection
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
- `dj.Instance()` works - isolated instances are always allowed

```python
# thread_safe=True

dj.config               # ThreadSafetyError
dj.conn()               # ThreadSafetyError
dj.Schema("name")       # ThreadSafetyError

inst = dj.Instance(host="h", user="u", password="p")  # OK
inst.config.safemode = False  # OK
inst.Schema("name")           # OK
```

## Behavior Summary

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` | `_singleton.config` | `ThreadSafetyError` |
| `dj.conn()` | `_singleton.connection` | `ThreadSafetyError` |
| `dj.Schema()` | `_singleton.Schema()` | `ThreadSafetyError` |
| `dj.Instance()` | Works | Works |
| `inst.config` | Works | Works |
| `inst.connection` | Works | Works |
| `inst.Schema()` | Works | Works |

## Singleton Lazy Loading

The singleton instance is created lazily on first access:

```python
dj.config          # Creates singleton, returns _singleton.config
dj.conn()          # Creates singleton, returns _singleton.connection
dj.Schema("name")  # Creates singleton, returns _singleton.Schema("name")
```

All three trigger creation of the same singleton instance.

## Usage Example

```python
import datajoint as dj

# Create isolated instance
inst = dj.Instance(
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

    def FreeTable(self, full_table_name):
        return FreeTable(self.connection, full_table_name)
```

### 2. Singleton with lazy loading

```python
# Module level
_thread_safe = _load_thread_safe_from_env_or_config()
_singleton_instance = None

def _get_singleton():
    if _thread_safe:
        raise ThreadSafetyError(
            "Global DataJoint state is disabled in thread-safe mode. "
            "Use dj.Instance() to create an isolated instance."
        )
    global _singleton_instance
    if _singleton_instance is None:
        _singleton_instance = Instance(
            host=_load_from_env_or_config("database.host"),
            user=_load_from_env_or_config("database.user"),
            password=_load_from_env_or_config("database.password"),
            ...
        )
    return _singleton_instance
```

### 3. Legacy API as aliases

```python
# dj.config -> singleton.config
class _ConfigProxy:
    def __getattr__(self, name):
        return getattr(_get_singleton().config, name)
    def __setattr__(self, name, value):
        setattr(_get_singleton().config, name, value)

config = _ConfigProxy()

# dj.conn() -> singleton.connection
def conn():
    return _get_singleton().connection

# dj.Schema() -> singleton.Schema()
def Schema(name, **kwargs):
    return _get_singleton().Schema(name, **kwargs)

# dj.FreeTable() -> singleton.FreeTable()
def FreeTable(full_table_name):
    return _get_singleton().FreeTable(full_table_name)
```

### 4. Refactor internal code

All internal code uses `self.connection._config` instead of global `config`:
- Tables access config via `self.connection._config`
- This works uniformly for both singleton and isolated instances

## Error Messages

- Singleton access: `"Global DataJoint state is disabled in thread-safe mode. Use dj.Instance() to create an isolated instance."`
