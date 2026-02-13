# Thread-Safe Mode Specification

**Status:** Draft
**Version:** 0.1
**Target Release:** DataJoint 2.2
**Authors:** Dimitri Yatsenko, Claude

## Overview

Thread-safe mode enables DataJoint to operate in multi-tenant environments (web applications, serverless functions, multi-threaded services) where multiple users or requests share the same Python process. When enabled, global mutable state is disabled, and all connection-specific configuration becomes scoped to individual `Connection` objects.

## Motivation

Traditional DataJoint usage relies on global state:
- `dj.config` — singleton configuration object
- `dj.conn()` — singleton database connection

This model works well for single-user scripts and notebooks but creates problems in:
- **Web applications** — concurrent requests from different users/tenants
- **Serverless functions** — shared runtime across invocations
- **Multi-threaded workers** — parallel processing with different credentials
- **Agentic workflows** — AI agents managing multiple database contexts

## Design Principles

1. **One-way lock** — Once enabled, thread-safe mode cannot be disabled
2. **Explicit over implicit** — All connection parameters must be explicitly provided
3. **No hidden global state** — Connection behavior is fully determined by its configuration
4. **Backward compatible** — Existing code works unchanged when `thread_safe=False`

---

## Configuration Categories

### Global Config (`dj.config`)

In thread-safe mode, `dj.config` access is blocked except for `thread_safe` itself:

| Setting | `thread_safe=False` | `thread_safe=True` |
|---------|---------------------|-------------------|
| `thread_safe` | Read/write | Read-only (one-way lock) |
| All other settings | Read/write | Raises `ThreadSafetyError` |

### Connection-Scoped Settings (`conn.config`)

All settings become connection-scoped and are accessed via `conn.config` (read/write):

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `safemode` | bool | True | Require confirmation for destructive ops |
| `database_prefix` | str | "" | Schema name prefix |
| `stores` | dict | {} | Blob storage configuration |
| `cache` | Path | None | Local cache directory |
| `query_cache` | Path | None | Query cache directory |
| `reconnect` | bool | True | Auto-reconnect on lost connection |
| `display_limit` | int | 12 | Max rows to display |
| `display_width` | int | 14 | Column width for display |
| `show_tuple_count` | bool | True | Show tuple count in repr |
| `loglevel` | str | "INFO" | Logging level |
| `filepath_checksum_size_limit` | int | None | Max file size for checksum |

Connection parameters (set at creation, read-only after):

| Setting | Type | Default | Description |
|---------|------|---------|-------------|
| `host` | str | "localhost" | Database hostname |
| `port` | int | 3306/5432 | Database port |
| `user` | str | *required* | Database username |
| `password` | str | *required* | Database password |
| `backend` | str | "mysql" | Database backend |
| `use_tls` | bool/dict | None | TLS configuration |
| `init_function` | str | None | SQL init command |

---

## API Specification

### Enabling Thread-Safe Mode

```python
import datajoint as dj

# Method 1: Programmatic
dj.config.thread_safe = True

# Method 2: Environment variable
# DJ_THREAD_SAFE=true python app.py

# Method 3: Config file (datajoint.json)
# { "thread_safe": true }
```

Once enabled, cannot be disabled:
```python
dj.config.thread_safe = True
dj.config.thread_safe = False  # Raises ThreadSafetyError
```

### Global Config Access in Thread-Safe Mode

```python
dj.config.thread_safe = True

# Only thread_safe is accessible
dj.config.thread_safe            # OK (returns True)

# Everything else raises ThreadSafetyError
dj.config.database.host          # Raises ThreadSafetyError
dj.config.display.width          # Raises ThreadSafetyError
dj.config.safemode               # Raises ThreadSafetyError
```

### Creating Connections

```python
# Thread-safe connection creation
conn = dj.Connection.from_config(
    host="db.example.com",
    user="tenant_user",
    password="tenant_password",
    port=3306,
    backend="mysql",
    safemode=False,
    stores={
        "raw": {
            "protocol": "s3",
            "endpoint": "s3.amazonaws.com",
            "bucket": "tenant-data",
            "access_key": "...",
            "secret_key": "...",
        }
    },
)

# Or from a configuration dict
tenant_config = {
    "host": "db.example.com",
    "user": request.tenant.db_user,
    "password": request.tenant.db_password,
    "stores": request.tenant.stores,
}
conn = dj.Connection.from_config(tenant_config)
```

### Accessing Connection-Scoped Settings

```python
conn = dj.Connection.from_config(...)

# Read settings through connection
conn.config.safemode          # True (default)
conn.config.database_prefix   # ""
conn.config.stores            # {}
conn.config.display_limit     # 12

# Modify settings for this connection
conn.config.safemode = False
conn.config.display_limit = 25
conn.config.stores = {"raw": {"protocol": "file", "location": "/data"}}
```

### Using Schemas with Connections

```python
conn = dj.Connection.from_config(tenant_config)

# Explicit connection binding
schema = dj.Schema("my_pipeline", connection=conn)

@schema
class Subject(dj.Manual):
    definition = """
    subject_id : int
    """

# Schema uses connection's settings
Subject.insert([{"subject_id": 1}])  # Uses conn.config.safemode
```

---

## API Compatibility Matrix

| API | `thread_safe=False` | `thread_safe=True` |
|-----|---------------------|-------------------|
| `dj.conn()` | Works | Raises `ThreadSafetyError` |
| `dj.config.thread_safe` | Read/write | Read-only |
| `dj.config.*` (all else) | Read/write | Raises `ThreadSafetyError` |
| `Schema()` without connection | Works | Raises `ThreadSafetyError` |
| **`Connection.from_config()`** | **Works** | **Works** |
| **`conn.config.*`** | **Read/write** (forwards to global) | **Read/write** (connection-scoped) |

The new API (`Connection.from_config()` and `conn.config`) is the **universal API** that works in both modes.

## Backward Compatibility

### Legacy API (thread_safe=False only)

Existing code continues to work unchanged when `thread_safe=False`:

```python
import datajoint as dj

# Global config access - works
dj.config["database.host"] = "localhost"
dj.config["database.user"] = "root"
dj.config["database.password"] = "secret"

# Singleton connection - works
conn = dj.conn()

# Schema without explicit connection - works
schema = dj.Schema("my_schema")  # Uses dj.conn()
```

### New API (works in both modes)

The new API works identically whether `thread_safe` is on or off:

```python
import datajoint as dj

# Works with thread_safe=False OR thread_safe=True
conn = dj.Connection.from_config(
    host="localhost",
    user="root",
    password="secret",
    safemode=False,
    stores={"raw": {...}},
)

# Access settings through connection - works in both modes
conn.config.safemode        # False
conn.config.stores          # {"raw": {...}}
conn.config.database_prefix # ""

# Schema with explicit connection - works in both modes
schema = dj.Schema("my_schema", connection=conn)
```

### Connection.config Behavior

When `thread_safe=False`, `conn.config` provides access to settings but **forwards to global config** for settings not explicitly provided:

```python
dj.config.thread_safe = False
dj.config.safemode = True
dj.config.database_prefix = "dev_"

conn = dj.Connection.from_config(
    host="localhost",
    user="root",
    password="secret",
    # safemode not specified - will forward to global
)

conn.config.safemode         # True (from dj.config)
conn.config.database_prefix  # "dev_" (from dj.config)
```

When `thread_safe=True`, `conn.config` uses only explicitly provided values with defaults:

```python
dj.config.thread_safe = True

conn = dj.Connection.from_config(
    host="localhost",
    user="root",
    password="secret",
    # safemode not specified - uses default
)

conn.config.safemode         # True (default)
conn.config.database_prefix  # "" (default)
```

## Migration Path

Migration is immediate — adopt the new API and your code works in both modes:

```python
# Before (legacy API - only works with thread_safe=False)
dj.config["database.host"] = "localhost"
dj.config["database.user"] = "root"
dj.config["database.password"] = "secret"
conn = dj.conn()
schema = dj.Schema("pipeline")

# After (new API - works with thread_safe=False AND thread_safe=True)
conn = dj.Connection.from_config(
    host="localhost",
    user="root",
    password="secret",
)
schema = dj.Schema("pipeline", connection=conn)
```

Once migrated to the new API, enabling `thread_safe=True` requires no code changes.

---

## Implementation Details

### Config Class Changes

```python
class Config(BaseSettings):
    def __getattribute__(self, name):
        # Allow private attributes
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        # Allow Pydantic internals
        if name.startswith("model_"):
            return object.__getattribute__(self, name)

        # Always allow checking thread_safe itself
        if name == "thread_safe":
            return object.__getattribute__(self, name)

        # Block everything else in thread-safe mode
        if object.__getattribute__(self, "thread_safe"):
            raise ThreadSafetyError(
                f"Setting '{name}' is connection-scoped in thread-safe mode. "
                "Access it via connection.config instead."
            )

        return object.__getattribute__(self, name)

    def __setattr__(self, name, value):
        # Allow private attributes
        if name.startswith("_"):
            return object.__setattr__(self, name, value)

        # thread_safe: one-way lock (can only go False -> True)
        if name == "thread_safe":
            current = object.__getattribute__(self, "thread_safe")
            if current and not value:
                raise ThreadSafetyError("Cannot disable thread-safe mode once enabled.")
            return object.__setattr__(self, name, value)

        # Block everything else in thread-safe mode
        if object.__getattribute__(self, "thread_safe"):
            raise ThreadSafetyError(
                f"Setting '{name}' is connection-scoped in thread-safe mode. "
                "Modify it via connection.config instead."
            )

        return object.__setattr__(self, name, value)
```

### Connection Class Changes

```python
class Connection:
    def __init__(self, host, user, password, port=None, init_fun=None,
                 use_tls=None, backend=None, *, _config=None):
        # ... existing connection setup ...

        # Store connection-scoped config
        self.config = _config or ConnectionConfig()

    @classmethod
    def from_config(cls, cfg=None, *, host=None, user=None, password=None,
                    port=None, backend=None, safemode=None, stores=None,
                    database_prefix=None, cache=None, query_cache=None,
                    reconnect=None, init_fun=None,
                    use_tls=None) -> "Connection":
        """
        Create connection with explicit configuration.

        Works in both thread_safe=False and thread_safe=True modes.
        """
        # ... merge cfg dict with kwargs ...
        # ... validate required fields (host, user, password) ...

        # Determine thread-safe mode (check global config safely)
        from .settings import config
        is_thread_safe = config.thread_safe

        # Build ConnectionConfig with explicit values only
        # (unset values will forward to global or use defaults)
        conn_config = ConnectionConfig(
            _thread_safe=is_thread_safe,
            **({"safemode": safemode} if safemode is not None else {}),
            **({"stores": stores} if stores is not None else {}),
            **({"database_prefix": database_prefix} if database_prefix is not None else {}),
            **({"cache": cache} if cache is not None else {}),
            **({"query_cache": query_cache} if query_cache is not None else {}),
            **({"reconnect": reconnect} if reconnect is not None else {}),
        )

        return cls(
            host=effective_host,
            user=effective_user,
            password=effective_password,
            port=effective_port,
            init_fun=effective_init_fun,
            use_tls=effective_use_tls,
            backend=effective_backend,
            _config=conn_config,
        )
```

### ConnectionConfig Class

```python
class ConnectionConfig:
    """
    Connection-scoped configuration (read/write).

    When thread_safe=False, unset values forward to global dj.config.
    When thread_safe=True, unset values use defaults (no global access).
    """

    _DEFAULTS = {
        "safemode": True,
        "database_prefix": "",
        "stores": {},
        "cache": None,
        "query_cache": None,
        "reconnect": True,
        "display_limit": 12,
        "display_width": 14,
        "show_tuple_count": True,
        "loglevel": "INFO",
        "filepath_checksum_size_limit": None,
    }

    def __init__(self, **explicit_values):
        self._values = {}  # Mutable storage for this connection
        self._thread_safe = explicit_values.pop("_thread_safe", False)
        self._values.update(explicit_values)

    def __getattr__(self, name):
        if name.startswith("_"):
            return object.__getattribute__(self, name)

        # If set on this connection, return that value
        if name in self._values:
            return self._values[name]

        # If thread_safe=False, forward to global config
        if not self._thread_safe:
            from .settings import config
            return getattr(config, name, self._DEFAULTS.get(name))

        # If thread_safe=True, return default
        return self._DEFAULTS.get(name)

    def __setattr__(self, name, value):
        if name.startswith("_"):
            return object.__setattr__(self, name, value)

        # Store in connection-local values
        self._values[name] = value

    def get_store_spec(self, store_name: str) -> dict:
        """Get store specification by name."""
        stores = self.stores
        if store_name not in stores:
            raise DataJointError(f"Store '{store_name}' is not configured.")
        return stores[store_name]
```

---

## Error Handling

### ThreadSafetyError

```python
class ThreadSafetyError(DataJointError):
    """
    Raised when global state is accessed in thread-safe mode.

    This error indicates that code is attempting to use global
    configuration or connections that are not thread-safe.
    """
```

### Error Messages

```python
# Reading blocked config
dj.config.safemode
ThreadSafetyError: Setting 'safemode' is connection-scoped in thread-safe mode.
Access it via connection.config instead.

# Writing blocked config
dj.config.display_limit = 20
ThreadSafetyError: Setting 'display_limit' is connection-scoped in thread-safe mode.
Modify it via connection.config instead.

# Using dj.conn()
dj.conn()
ThreadSafetyError: dj.conn() is disabled in thread-safe mode.
Use Connection.from_config() with explicit configuration.

# Disabling thread-safe mode
dj.config.thread_safe = False
ThreadSafetyError: Cannot disable thread-safe mode once enabled.

# Schema without connection
dj.Schema("my_schema")
ThreadSafetyError: Schema requires explicit connection in thread-safe mode.
Use Schema('name', connection=conn).
```

---

## Testing Strategy

### Unit Tests

1. **Global config in thread-safe mode**
   - Verify only `thread_safe` is accessible
   - Verify all other settings raise ThreadSafetyError (read and write)
   - Verify one-way lock behavior

2. **Connection.from_config()**
   - Verify all parameters are accepted
   - Verify defaults are applied correctly
   - Verify cfg dict merging with kwargs
   - Verify works in both thread_safe modes

3. **ConnectionConfig**
   - Verify read/write access to all settings
   - Verify forwarding to global config when thread_safe=False
   - Verify defaults used when thread_safe=True
   - Verify store spec resolution

### Integration Tests

1. **Multi-tenant simulation**
   - Create multiple connections with different configs
   - Verify isolation between connections
   - Verify correct store resolution per connection

2. **Schema binding**
   - Verify schemas use connection's config
   - Verify safemode behavior per connection

---

## Future Considerations

### Potential Extensions

1. **Connection pooling** — Pool of connections per tenant configuration
2. **Async support** — Async connection management for async frameworks
3. **Context managers** — Temporary connection context for specific operations

### Out of Scope

1. **Thread-local storage** — Rejected in favor of explicit connection passing
2. **Automatic credential rotation** — Application responsibility
3. **Multi-database transactions** — Not supported by underlying backends

---

## References

- [DataJoint Python Documentation](https://docs.datajoint.com)
- [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/)
- [WSGI/ASGI Thread Safety](https://peps.python.org/pep-3333/)
