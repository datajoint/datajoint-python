# Thread-Safe Mode Specification

## Problem

DataJoint uses global state (`dj.config`, `dj.conn()`) that is not thread-safe. Multi-tenant applications (web servers, async workers) need isolated connections per request/task.

## Solution

Introduce **Instance** objects that encapsulate config and connection. The `dj` module provides a global config that can be modified before connecting, and a lazily-loaded singleton connection. New isolated instances are created with `dj.Instance()`.

## API

### Legacy API (global config + singleton connection)

```python
import datajoint as dj

# Configure credentials (no connection yet)
dj.config.database.user = "user"
dj.config.database.password = "password"

# First call to conn() or Schema() creates the singleton connection
dj.conn()  # Creates connection using dj.config credentials
schema = dj.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = "..."
```

Alternatively, pass credentials directly to `conn()`:
```python
dj.conn(host="localhost", user="user", password="password")
```

Internally:
- `dj.config` → delegates to `_global_config` (with thread-safety check)
- `dj.conn()` → returns `_singleton_connection` (created lazily)
- `dj.Schema()` → uses `_singleton_connection`
- `dj.FreeTable()` → uses `_singleton_connection`

### New API (isolated instance)

```python
import datajoint as dj

inst = dj.Instance(
    host="localhost",
    user="user",
    password="password",
)
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

`thread_safe` is checked dynamically on each access to global state.

When `thread_safe=True`, accessing global state raises `ThreadSafetyError`:
- `dj.config` raises `ThreadSafetyError`
- `dj.conn()` raises `ThreadSafetyError`
- `dj.Schema()` raises `ThreadSafetyError` (without explicit connection)
- `dj.FreeTable()` raises `ThreadSafetyError` (without explicit connection)
- `dj.Instance()` works - isolated instances are always allowed

```python
# thread_safe=True

dj.config               # ThreadSafetyError
dj.conn()               # ThreadSafetyError
dj.Schema("name")       # ThreadSafetyError

inst = dj.Instance(host="h", user="u", password="p")  # OK
inst.Schema("name")           # OK
```

## Behavior Summary

| Operation | `thread_safe=False` | `thread_safe=True` |
|-----------|--------------------|--------------------|
| `dj.config` | `_global_config` | `ThreadSafetyError` |
| `dj.conn()` | `_singleton_connection` | `ThreadSafetyError` |
| `dj.Schema()` | Uses singleton | `ThreadSafetyError` |
| `dj.FreeTable()` | Uses singleton | `ThreadSafetyError` |
| `dj.Instance()` | Works | Works |
| `inst.config` | Works | Works |
| `inst.connection` | Works | Works |
| `inst.Schema()` | Works | Works |

## Lazy Loading

The global config is created at module import time. The singleton connection is created lazily on first access:

```python
dj.config.database.user = "user"  # Modifies global config (no connection yet)
dj.config.database.password = "pw"
dj.conn()          # Creates singleton connection using global config
dj.Schema("name")  # Uses existing singleton connection
```

## Usage Example

```python
import datajoint as dj

# Create isolated instance
inst = dj.Instance(
    host="localhost",
    user="user",
    password="password",
)

# Create schema
schema = inst.Schema("my_schema")

@schema
class Mouse(dj.Manual):
    definition = """
    mouse_id: int
    """

# Use tables
Mouse().insert1({"mouse_id": 1})
Mouse().fetch()
```

## Implementation

### 1. Create Instance class

```python
class Instance:
    def __init__(self, host, user, password, port=3306, **kwargs):
        self.config = _create_config()  # Fresh config with defaults
        # Apply any config overrides from kwargs
        self.connection = Connection(host, user, password, port, ...)
        self.connection._config = self.config

    def Schema(self, name, **kwargs):
        return Schema(name, connection=self.connection, **kwargs)

    def FreeTable(self, full_table_name):
        return FreeTable(self.connection, full_table_name)
```

### 2. Global config and singleton connection

```python
# Module level
_global_config = _create_config()  # Created at import time
_singleton_connection = None       # Created lazily

def _check_thread_safe():
    if _load_thread_safe():
        raise ThreadSafetyError(
            "Global DataJoint state is disabled in thread-safe mode. "
            "Use dj.Instance() to create an isolated instance."
        )

def _get_singleton_connection():
    _check_thread_safe()
    global _singleton_connection
    if _singleton_connection is None:
        _singleton_connection = Connection(
            host=_global_config.database.host,
            user=_global_config.database.user,
            password=_global_config.database.password,
            ...
        )
        _singleton_connection._config = _global_config
    return _singleton_connection
```

### 3. Legacy API with thread-safety checks

```python
# dj.config -> global config with thread-safety check
class _ConfigProxy:
    def __getattr__(self, name):
        _check_thread_safe()
        return getattr(_global_config, name)
    def __setattr__(self, name, value):
        _check_thread_safe()
        setattr(_global_config, name, value)

config = _ConfigProxy()

# dj.conn() -> singleton connection
def conn():
    return _get_singleton_connection()

# dj.Schema() -> uses singleton connection
def Schema(name, connection=None, **kwargs):
    if connection is None:
        _check_thread_safe()
        connection = _get_singleton_connection()
    return _Schema(name, connection=connection, **kwargs)

# dj.FreeTable() -> uses singleton connection
def FreeTable(conn_or_name, full_table_name=None):
    if full_table_name is None:
        # Called as FreeTable("db.table")
        _check_thread_safe()
        return _FreeTable(_get_singleton_connection(), conn_or_name)
    else:
        # Called as FreeTable(conn, "db.table")
        return _FreeTable(conn_or_name, full_table_name)
```

### 4. Refactor internal code

All internal code uses `self.connection._config` instead of global `config`:
- Connection stores reference to its config as `self._config`
- Tables access config via `self.connection._config`
- This works uniformly for both singleton and isolated instances

## Global State Audit

All module-level mutable state was reviewed for thread-safety implications.

### Guarded (blocked in thread-safe mode)

| State | Location | Mechanism |
|-------|----------|-----------|
| `config` singleton | `settings.py:979` | `_ConfigProxy` raises `ThreadSafetyError`; use `inst.config` instead |
| `conn()` singleton | `connection.py:108` | `_check_thread_safe()` guard; use `inst.connection` instead |

These are the two globals that carry connection-scoped state (credentials, database settings) and are the primary source of cross-tenant interference.

### Safe by design (no guard needed)

| State | Location | Rationale |
|-------|----------|-----------|
| `_codec_registry` | `codecs.py:47` | Effectively immutable after import. Registration runs in `__init_subclass__` under Python's import lock. Runtime mutation (`_load_entry_points`) is idempotent under the GIL. Codecs are part of the type system, not connection-scoped. |
| `_entry_points_loaded` | `codecs.py:48` | Bool flag for idempotent lazy loading; worst case under concurrent access is redundant work, not corruption. |

### Low risk (no guard needed)

| State | Location | Rationale |
|-------|----------|-----------|
| Logging side effects | `logging.py:8,17,40-45,56` | Standard Python logging configuration. Monkey-patches `Logger` and replaces `sys.excepthook` at import time. Not DataJoint-specific mutable state. |
| `use_32bit_dims` | `blob.py:65` | Runtime flag affecting deserialization. Rarely changed; not connection-scoped. |
| `compression` dict | `blob.py:61` | Decompressor function registry. Populated at import time, effectively read-only thereafter. |
| `_lazy_modules` | `__init__.py:92` | Import caching via `globals()` mutation. Protected by Python's import lock. |
| `ADAPTERS` dict | `adapters/__init__.py:16` | Backend registry. Populated at import time, read-only in practice. |

### Design principle

Only state that is **connection-scoped** (credentials, database settings, connection objects) needs thread-safe guards. State that is **code-scoped** (type registries, import caches, logging configuration) is shared across all threads by design and does not vary between tenants.

## Error Messages

- Singleton access: `"Global DataJoint state is disabled in thread-safe mode. Use dj.Instance() to create an isolated instance."`
