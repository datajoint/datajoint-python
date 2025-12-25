# Configuration Settings

DataJoint uses a type-checked configuration system built on [pydantic-settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/).

## Configuration Sources

Settings are loaded from the following sources (in priority order):

1. **Environment variables** (`DJ_*`)
2. **Secrets directory** (`.secrets/` or `/run/secrets/datajoint/`)
3. **Project config file** (`datajoint.json`, searched recursively)
4. **Default values**

## Project Structure

```
myproject/
├── .git/
├── datajoint.json      # Project config (commit this)
├── .secrets/           # Local secrets (add to .gitignore)
│   ├── database.password
│   └── aws.secret_access_key
└── src/
    └── analysis.py     # Config found via parent search
```

## Config File

Create a `datajoint.json` file in your project root:

```json
{
    "database": {
        "host": "db.example.com",
        "port": 3306
    },
    "stores": {
        "raw": {
            "protocol": "file",
            "location": "/data/raw"
        }
    },
    "display": {
        "limit": 20
    },
    "safemode": true
}
```

DataJoint searches for this file starting from the current directory and moving up through parent directories, stopping at the first `.git` or `.hg` directory (project boundary) or filesystem root.

## Credentials

**Never store credentials in config files.** Use one of these methods:

### Environment Variables (Recommended)

```bash
export DJ_USER=alice
export DJ_PASS=secret
export DJ_HOST=db.example.com
```

### Secrets Directory

Create files in `.secrets/` next to your `datajoint.json`:

```
.secrets/
├── database.password    # Contains: secret
├── database.user        # Contains: alice
├── aws.access_key_id
└── aws.secret_access_key
```

Add `.secrets/` to your `.gitignore`.

For Docker/Kubernetes, secrets can be mounted at `/run/secrets/datajoint/`.

## Accessing Settings

```python
import datajoint as dj

# Attribute access (preferred)
dj.config.database.host
dj.config.safemode

# Dict-style access
dj.config["database.host"]
dj.config["safemode"]
```

## Temporary Overrides

Use the context manager for temporary changes:

```python
with dj.config.override(safemode=False):
    # safemode is False here
    table.delete()
# safemode is restored
```

For nested settings, use double underscores:

```python
with dj.config.override(database__host="test.example.com"):
    # database.host is temporarily changed
    pass
```

## Available Settings

### Database Connection

| Setting | Environment Variable | Default | Description |
|---------|---------------------|---------|-------------|
| `database.host` | `DJ_HOST` | `localhost` | Database server hostname |
| `database.port` | `DJ_PORT` | `3306` | Database server port |
| `database.user` | `DJ_USER` | `None` | Database username |
| `database.password` | `DJ_PASS` | `None` | Database password (use env/secrets) |
| `database.reconnect` | — | `True` | Auto-reconnect on connection loss |
| `database.use_tls` | — | `None` | TLS mode: `True`, `False`, or `None` (auto) |

### Display

| Setting | Default | Description |
|---------|---------|-------------|
| `display.limit` | `12` | Max rows to display in previews |
| `display.width` | `14` | Column width in previews |
| `display.show_tuple_count` | `True` | Show total count in previews |

### Other Settings

| Setting | Default | Description |
|---------|---------|-------------|
| `safemode` | `True` | Prompt before destructive operations |
| `loglevel` | `INFO` | Logging level |
| `fetch_format` | `array` | Default fetch format (`array` or `frame`) |
| `enable_python_native_blobs` | `True` | Use Python-native blob serialization |

## TLS Configuration

DataJoint uses TLS by default if available. Control this with:

```python
dj.config.database.use_tls = True   # Require TLS
dj.config.database.use_tls = False  # Disable TLS
dj.config.database.use_tls = None   # Auto (default)
```

## External Storage

Configure external stores in the `stores` section. See [External Storage](../sysadmin/external-store.md) for details.

```json
{
    "stores": {
        "raw": {
            "protocol": "file",
            "location": "/data/external"
        }
    }
}
```

## Object Storage

Configure object storage for the [`object` type](../design/tables/object.md) in the `object_storage` section. This provides managed file and folder storage with fsspec backend support.

### Local Filesystem

```json
{
    "object_storage": {
        "project_name": "my_project",
        "protocol": "file",
        "location": "/data/my_project"
    }
}
```

### Amazon S3

```json
{
    "object_storage": {
        "project_name": "my_project",
        "protocol": "s3",
        "bucket": "my-bucket",
        "location": "my_project",
        "endpoint": "s3.amazonaws.com"
    }
}
```

### Object Storage Settings

| Setting | Environment Variable | Required | Description |
|---------|---------------------|----------|-------------|
| `object_storage.project_name` | `DJ_OBJECT_STORAGE_PROJECT_NAME` | Yes | Unique project identifier |
| `object_storage.protocol` | `DJ_OBJECT_STORAGE_PROTOCOL` | Yes | Backend: `file`, `s3`, `gcs`, `azure` |
| `object_storage.location` | `DJ_OBJECT_STORAGE_LOCATION` | Yes | Base path or bucket prefix |
| `object_storage.bucket` | `DJ_OBJECT_STORAGE_BUCKET` | For cloud | Bucket name |
| `object_storage.endpoint` | `DJ_OBJECT_STORAGE_ENDPOINT` | For S3 | S3 endpoint URL |
| `object_storage.partition_pattern` | `DJ_OBJECT_STORAGE_PARTITION_PATTERN` | No | Path pattern with `{attr}` placeholders |
| `object_storage.token_length` | `DJ_OBJECT_STORAGE_TOKEN_LENGTH` | No | Random suffix length (default: 8) |
| `object_storage.access_key` | — | For cloud | Access key (use secrets) |
| `object_storage.secret_key` | — | For cloud | Secret key (use secrets) |

### Object Storage Secrets

Store cloud credentials in the secrets directory:

```
.secrets/
├── object_storage.access_key
└── object_storage.secret_key
```
