# Credentials

Database credentials should never be stored in config files. Use environment variables or a secrets directory instead.

## Environment Variables (Recommended)

Set the following environment variables:

```bash
export DJ_HOST=db.example.com
export DJ_USER=alice
export DJ_PASS=secret
```

These take priority over all other configuration sources.

## Secrets Directory

Create a `.secrets/` directory next to your `datajoint.json`:

```
myproject/
├── datajoint.json
└── .secrets/
    ├── database.user      # Contains: alice
    └── database.password  # Contains: secret
```

Each file contains a single secret value (no JSON, just the raw value).

Add `.secrets/` to your `.gitignore`:

```
# .gitignore
.secrets/
```

## Docker / Kubernetes

Mount secrets at `/run/secrets/datajoint/`:

```yaml
# docker-compose.yml
services:
  app:
    volumes:
      - ./secrets:/run/secrets/datajoint:ro
```

## Interactive Prompt

If credentials are not provided via environment variables or secrets, DataJoint will prompt for them when connecting:

```python
>>> import datajoint as dj
>>> dj.conn()
Please enter DataJoint username: alice
Please enter DataJoint password:
```

## Programmatic Access

You can also set credentials in Python (useful for testing):

```python
import datajoint as dj

dj.config.database.user = "alice"
dj.config.database.password = "secret"
```

Note that `password` uses `SecretStr` internally, so it will be masked in logs and repr output.

## Changing Database Password

To change your database password, use your database's native tools:

```sql
ALTER USER 'alice'@'%' IDENTIFIED BY 'new_password';
```

Then update your environment variables or secrets file accordingly.
