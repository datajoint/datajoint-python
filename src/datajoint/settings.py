"""
DataJoint configuration system using pydantic-settings.

This module provides strongly-typed configuration with automatic loading
from environment variables, secrets directories, and JSON config files.

Configuration sources (in priority order):

1. Environment variables (``DJ_*``)
2. Secrets directories (``.secrets/`` in project, ``/run/secrets/datajoint/``)
3. Project config file (``datajoint.json``, searched recursively up to ``.git/.hg``)

Examples
--------
>>> import datajoint as dj
>>> dj.config.database.host
'localhost'
>>> with dj.config.override(safemode=False):
...     # dangerous operations here
...     pass

Project structure::

    myproject/
    ├── .git/
    ├── datajoint.json      # Project config (commit this)
    ├── .secrets/           # Local secrets (gitignore this)
    │   ├── database.password
    │   └── aws.secret_access_key
    └── src/
        └── analysis.py     # Config found via parent search
"""

from __future__ import annotations

import json
import logging
import os
import warnings
from contextlib import contextmanager
from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Any, Iterator, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import DataJointError

CONFIG_FILENAME = "datajoint.json"
SECRETS_DIRNAME = ".secrets"
SYSTEM_SECRETS_DIR = Path("/run/secrets/datajoint")
DEFAULT_SUBFOLDING = (2, 2)

# Mapping of config keys to environment variables
# Environment variables take precedence over config file values
ENV_VAR_MAPPING = {
    "database.host": "DJ_HOST",
    "database.user": "DJ_USER",
    "database.password": "DJ_PASS",
    "database.port": "DJ_PORT",
    "loglevel": "DJ_LOG_LEVEL",
}

Role = Enum("Role", "manual lookup imported computed job")
role_to_prefix = {
    Role.manual: "",
    Role.lookup: "#",
    Role.imported: "_",
    Role.computed: "__",
    Role.job: "~",
}
prefix_to_role = dict(zip(role_to_prefix.values(), role_to_prefix))

logger = logging.getLogger(__name__.split(".")[0])


def find_config_file(start: Path | None = None) -> Path | None:
    """
    Search for datajoint.json in current and parent directories.

    Searches upward from ``start`` until finding the config file or hitting
    a project boundary (``.git``, ``.hg``) or filesystem root.

    Parameters
    ----------
    start : Path, optional
        Directory to start search from. Defaults to current working directory.

    Returns
    -------
    Path or None
        Path to config file if found, None otherwise.
    """
    current = (start or Path.cwd()).resolve()

    while True:
        config_path = current / CONFIG_FILENAME
        if config_path.is_file():
            return config_path

        # Stop at project/repo root
        if (current / ".git").exists() or (current / ".hg").exists():
            return None

        # Stop at filesystem root
        if current == current.parent:
            return None

        current = current.parent


def find_secrets_dir(config_path: Path | None = None) -> Path | None:
    """
    Find the secrets directory.

    Priority:

    1. ``.secrets/`` in same directory as datajoint.json (project secrets)
    2. ``/run/secrets/datajoint/`` (Docker/Kubernetes secrets)

    Parameters
    ----------
    config_path : Path, optional
        Path to datajoint.json if found.

    Returns
    -------
    Path or None
        Path to secrets directory if found, None otherwise.
    """
    # Check project secrets directory (next to config file)
    if config_path is not None:
        project_secrets = config_path.parent / SECRETS_DIRNAME
        if project_secrets.is_dir():
            return project_secrets

    # Check system secrets directory (Docker/Kubernetes)
    if SYSTEM_SECRETS_DIR.is_dir():
        return SYSTEM_SECRETS_DIR

    return None


def read_secret_file(secrets_dir: Path | None, name: str) -> str | None:
    """
    Read a secret value from a file in the secrets directory.

    Parameters
    ----------
    secrets_dir : Path or None
        Path to secrets directory.
    name : str
        Name of the secret file (e.g., ``'database.password'``).

    Returns
    -------
    str or None
        Secret value as string, or None if not found.
    """
    if secrets_dir is None:
        return None

    secret_path = secrets_dir / name
    if secret_path.is_file():
        return secret_path.read_text().strip()

    return None


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    host: str = Field(default="localhost", validation_alias="DJ_HOST")
    user: str | None = Field(default=None, validation_alias="DJ_USER")
    password: SecretStr | None = Field(default=None, validation_alias="DJ_PASS")
    port: int = Field(default=3306, validation_alias="DJ_PORT")
    reconnect: bool = True
    use_tls: bool | None = None


class ConnectionSettings(BaseSettings):
    """Connection behavior settings."""

    model_config = SettingsConfigDict(extra="forbid", validate_assignment=True)

    init_function: str | None = None
    charset: str = ""  # pymysql uses '' as default


class DisplaySettings(BaseSettings):
    """Display and preview settings."""

    model_config = SettingsConfigDict(extra="forbid", validate_assignment=True)

    limit: int = 12
    width: int = 14
    show_tuple_count: bool = True


class StoresSettings(BaseSettings):
    """
    Unified object storage configuration.

    Stores configuration supports both hash-addressed and schema-addressed storage
    using the same named stores with _hash and _schema sections.
    """

    model_config = SettingsConfigDict(
        case_sensitive=False,
        extra="allow",  # Allow dynamic store names
        validate_assignment=True,
    )

    default: str | None = Field(default=None, description="Name of the default store")

    # Named stores are added dynamically as stores.<name>.*
    # Structure: stores.<name>.protocol, stores.<name>.location, etc.


class JobsSettings(BaseSettings):
    """Job queue configuration for AutoPopulate 2.0."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_JOBS_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    auto_refresh: bool = Field(default=True, description="Auto-refresh jobs queue on populate")
    keep_completed: bool = Field(default=False, description="Keep success records in jobs table")
    stale_timeout: int = Field(default=3600, ge=0, description="Seconds before pending job is checked for staleness")
    default_priority: int = Field(default=5, ge=0, le=255, description="Default priority for new jobs (lower = more urgent)")
    version_method: Literal["git", "none"] | None = Field(
        default=None, description="Method to obtain version: 'git' (commit hash), 'none' (empty), or None (disabled)"
    )
    allow_new_pk_fields_in_computed_tables: bool = Field(
        default=False,
        description="Allow native (non-FK) primary key fields in Computed/Imported tables. "
        "When True, bypasses the FK-only PK validation. Job granularity will be degraded for such tables.",
    )
    add_job_metadata: bool = Field(
        default=False,
        description="Add hidden job metadata attributes (_job_start_time, _job_duration, _job_version) "
        "to Computed and Imported tables during declaration. Tables created without this setting "
        "will not receive metadata updates during populate.",
    )


class Config(BaseSettings):
    """
    Main DataJoint configuration.

    Settings are loaded from (in priority order):

    1. Environment variables (``DJ_*``)
    2. Secrets directory (``.secrets/`` or ``/run/secrets/datajoint/``)
    3. Config file (``datajoint.json``, searched in parent directories)
    4. Default values

    Examples
    --------
    Access settings via attributes:

    >>> config.database.host
    >>> config.safemode

    Override temporarily with context manager:

    >>> with config.override(safemode=False):
    ...     pass
    """

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    # Nested settings groups
    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    connection: ConnectionSettings = Field(default_factory=ConnectionSettings)
    display: DisplaySettings = Field(default_factory=DisplaySettings)
    jobs: JobsSettings = Field(default_factory=JobsSettings)

    # Unified stores configuration (replaces external and object_storage)
    stores: dict[str, Any] = Field(
        default_factory=dict,
        description="Unified object storage configuration. "
        "Use stores.default to designate default store. "
        "Configure named stores as stores.<name>.protocol, stores.<name>.location, etc.",
    )

    # Top-level settings
    loglevel: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO", validation_alias="DJ_LOG_LEVEL")
    safemode: bool = True
    enable_python_native_blobs: bool = True
    filepath_checksum_size_limit: int | None = None

    # Cache paths
    cache: Path | None = None
    query_cache: Path | None = None

    # Download path for attachments and filepaths
    download_path: str = "."

    # Internal: track where config was loaded from
    _config_path: Path | None = None
    _secrets_dir: Path | None = None

    @field_validator("loglevel", mode="after")
    @classmethod
    def set_logger_level(cls, v: str) -> str:
        """Update logger level when loglevel changes."""
        logger.setLevel(v)
        return v

    @field_validator("cache", "query_cache", mode="before")
    @classmethod
    def convert_path(cls, v: Any) -> Path | None:
        """Convert string paths to Path objects."""
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v

    def get_store_spec(self, store: str | None = None, *, use_filepath_default: bool = False) -> dict[str, Any]:
        """
        Get configuration for a storage store.

        Parameters
        ----------
        store : str, optional
            Name of the store to retrieve. If None, uses the appropriate default.
        use_filepath_default : bool, optional
            If True and store is None, uses stores.filepath_default instead of
            stores.default. Use for filepath references which are not part of OAS.
            Default: False (use stores.default for integrated storage).

        Returns
        -------
        dict[str, Any]
            Store configuration dict with validated fields.

        Raises
        ------
        DataJointError
            If store is not configured or has invalid config.
        """
        # Handle default store
        if store is None:
            if use_filepath_default:
                # Filepath references use separate default (not part of OAS)
                if "filepath_default" not in self.stores:
                    raise DataJointError(
                        "stores.filepath_default is not configured. "
                        "Set stores.filepath_default or specify store explicitly with <filepath@store>"
                    )
                store = self.stores["filepath_default"]
            else:
                # Integrated storage (hash, schema) uses stores.default
                if "default" not in self.stores:
                    raise DataJointError("stores.default is not configured")
                store = self.stores["default"]

            if not isinstance(store, str):
                default_key = "filepath_default" if use_filepath_default else "default"
                raise DataJointError(f"stores.{default_key} must be a string")

        # Check store exists
        if store not in self.stores:
            raise DataJointError(f"Storage '{store}' is requested but not configured in stores")

        spec = dict(self.stores[store])

        # Set defaults for optional fields (common to all protocols)
        spec.setdefault("subfolding", None)  # No subfolding by default
        spec.setdefault("partition_pattern", None)  # No partitioning by default
        spec.setdefault("token_length", 8)  # Default token length

        # Set defaults for storage section prefixes
        spec.setdefault("hash_prefix", "_hash")  # Hash-addressed storage section
        spec.setdefault("schema_prefix", "_schema")  # Schema-addressed storage section
        spec.setdefault("filepath_prefix", None)  # Filepath storage (unrestricted by default)

        # Validate protocol
        protocol = spec.get("protocol", "").lower()
        supported_protocols = ("file", "s3", "gcs", "azure")
        if protocol not in supported_protocols:
            raise DataJointError(
                f'Missing or invalid protocol in config.stores["{store}"]. '
                f"Supported protocols: {', '.join(supported_protocols)}"
            )

        # Set protocol-specific defaults
        if protocol == "s3":
            spec.setdefault("secure", True)  # HTTPS by default for S3

        # Define required and allowed keys by protocol
        required_keys: dict[str, tuple[str, ...]] = {
            "file": ("protocol", "location"),
            "s3": ("protocol", "endpoint", "bucket", "access_key", "secret_key", "location"),
            "gcs": ("protocol", "bucket", "location"),
            "azure": ("protocol", "container", "location"),
        }
        allowed_keys: dict[str, tuple[str, ...]] = {
            "file": (
                "protocol",
                "location",
                "subfolding",
                "partition_pattern",
                "token_length",
                "hash_prefix",
                "schema_prefix",
                "filepath_prefix",
                "stage",
            ),
            "s3": (
                "protocol",
                "endpoint",
                "bucket",
                "access_key",
                "secret_key",
                "location",
                "secure",
                "subfolding",
                "partition_pattern",
                "token_length",
                "hash_prefix",
                "schema_prefix",
                "filepath_prefix",
                "stage",
                "proxy_server",
            ),
            "gcs": (
                "protocol",
                "bucket",
                "location",
                "token",
                "project",
                "subfolding",
                "partition_pattern",
                "token_length",
                "hash_prefix",
                "schema_prefix",
                "filepath_prefix",
                "stage",
            ),
            "azure": (
                "protocol",
                "container",
                "location",
                "account_name",
                "account_key",
                "connection_string",
                "subfolding",
                "partition_pattern",
                "token_length",
                "hash_prefix",
                "schema_prefix",
                "filepath_prefix",
                "stage",
            ),
        }

        # Check required keys
        missing = [k for k in required_keys[protocol] if k not in spec]
        if missing:
            raise DataJointError(f'config.stores["{store}"] is missing: {", ".join(missing)}')

        # Check for invalid keys
        invalid = [k for k in spec if k not in allowed_keys[protocol]]
        if invalid:
            raise DataJointError(f'Invalid key(s) in config.stores["{store}"]: {", ".join(invalid)}')

        # Validate prefix separation to prevent overlap
        self._validate_prefix_separation(
            store_name=store,
            hash_prefix=spec.get("hash_prefix"),
            schema_prefix=spec.get("schema_prefix"),
            filepath_prefix=spec.get("filepath_prefix"),
        )

        return spec

    def _validate_prefix_separation(
        self,
        store_name: str,
        hash_prefix: str | None,
        schema_prefix: str | None,
        filepath_prefix: str | None,
    ) -> None:
        """
        Validate that storage section prefixes don't overlap.

        Parameters
        ----------
        store_name : str
            Name of the store being validated (for error messages).
        hash_prefix : str or None
            Prefix for hash-addressed storage.
        schema_prefix : str or None
            Prefix for schema-addressed storage.
        filepath_prefix : str or None
            Prefix for filepath storage (None means unrestricted).

        Raises
        ------
        DataJointError
            If any prefixes overlap (one is a parent/child of another).
        """
        # Collect non-null prefixes with their names
        prefixes = []
        if hash_prefix:
            prefixes.append(("hash_prefix", hash_prefix))
        if schema_prefix:
            prefixes.append(("schema_prefix", schema_prefix))
        if filepath_prefix:
            prefixes.append(("filepath_prefix", filepath_prefix))

        # Normalize prefixes: remove leading/trailing slashes, ensure trailing slash for comparison
        def normalize(p: str) -> str:
            return p.strip("/") + "/"

        normalized = [(name, normalize(prefix)) for name, prefix in prefixes]

        # Check each pair for overlap
        for i, (name1, p1) in enumerate(normalized):
            for j, (name2, p2) in enumerate(normalized[i + 1 :], start=i + 1):
                # Check if one prefix is a parent of another
                if p1.startswith(p2) or p2.startswith(p1):
                    raise DataJointError(
                        f'config.stores["{store_name}"]: {name1}="{prefixes[i][1]}" and '
                        f'{name2}="{prefixes[j][1]}" overlap. '
                        f"Storage section prefixes must be mutually exclusive."
                    )

    def load(self, filename: str | Path) -> None:
        """
        Load settings from a JSON file.

        Parameters
        ----------
        filename : str or Path
            Path to load configuration from.
        """
        filepath = Path(filename)
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")

        logger.info(f"Loading configuration from {filepath.absolute()}")

        with open(filepath) as f:
            data = json.load(f)

        self._update_from_flat_dict(data)
        self._config_path = filepath

    def _update_from_flat_dict(self, data: dict[str, Any]) -> None:
        """
        Update settings from a dict (flat dot-notation or nested).

        Environment variables take precedence over config file values.
        If an env var is set for a setting, the file value is skipped.
        """
        for key, value in data.items():
            # Special handling for stores - accept nested dict directly
            if key == "stores" and isinstance(value, dict):
                # Merge stores dict
                for store_key, store_value in value.items():
                    self.stores[store_key] = store_value
                continue

            # Handle nested dicts by recursively updating
            if isinstance(value, dict) and hasattr(self, key):
                group_obj = getattr(self, key)
                for nested_key, nested_value in value.items():
                    if hasattr(group_obj, nested_key):
                        # Check if env var is set for this nested key
                        full_key = f"{key}.{nested_key}"
                        env_var = ENV_VAR_MAPPING.get(full_key)
                        if env_var and os.environ.get(env_var):
                            logger.debug(f"Skipping {full_key} from file (env var {env_var} takes precedence)")
                            continue
                        setattr(group_obj, nested_key, nested_value)
                continue

            # Handle flat dot-notation keys
            parts = key.split(".")
            if len(parts) == 1:
                if hasattr(self, key) and not key.startswith("_"):
                    # Check if env var is set for this key
                    env_var = ENV_VAR_MAPPING.get(key)
                    if env_var and os.environ.get(env_var):
                        logger.debug(f"Skipping {key} from file (env var {env_var} takes precedence)")
                        continue
                    setattr(self, key, value)
            elif len(parts) == 2:
                group, attr = parts
                if hasattr(self, group):
                    group_obj = getattr(self, group)
                    if hasattr(group_obj, attr):
                        # Check if env var is set for this key
                        env_var = ENV_VAR_MAPPING.get(key)
                        if env_var and os.environ.get(env_var):
                            logger.debug(f"Skipping {key} from file (env var {env_var} takes precedence)")
                            continue
                        setattr(group_obj, attr, value)
            elif len(parts) == 3:
                # Handle stores.<name>.<attr> pattern
                group, store_name, attr = parts
                if group == "stores":
                    if store_name not in self.stores:
                        self.stores[store_name] = {}
                    self.stores[store_name][attr] = value

    def _load_secrets(self, secrets_dir: Path) -> None:
        """Load secrets from a secrets directory."""
        self._secrets_dir = secrets_dir

        # Load database secrets
        db_user = read_secret_file(secrets_dir, "database.user")
        if db_user is not None and self.database.user is None:
            self.database.user = db_user
            logger.debug(f"Loaded database.user from {secrets_dir}")

        db_password = read_secret_file(secrets_dir, "database.password")
        if db_password is not None and self.database.password is None:
            self.database.password = db_password
            logger.debug(f"Loaded database.password from {secrets_dir}")

        # Load per-store secrets (stores.<name>.access_key, stores.<name>.secret_key)
        # Iterate through all files in secrets directory
        if secrets_dir.is_dir():
            for secret_file in secrets_dir.iterdir():
                if not secret_file.is_file() or secret_file.name.startswith("."):
                    continue

                parts = secret_file.name.split(".")
                # Check for stores.<name>.access_key or stores.<name>.secret_key pattern
                if len(parts) == 3 and parts[0] == "stores":
                    store_name, attr = parts[1], parts[2]
                    if attr in ("access_key", "secret_key"):
                        value = secret_file.read_text().strip()
                        # Initialize store dict if needed
                        if store_name not in self.stores:
                            self.stores[store_name] = {}
                        # Only set if not already present
                        if attr not in self.stores[store_name]:
                            self.stores[store_name][attr] = value
                            logger.debug(f"Loaded stores.{store_name}.{attr} from {secrets_dir}")

    @contextmanager
    def override(self, **kwargs: Any) -> Iterator["Config"]:
        """
        Temporarily override configuration values.

        Parameters
        ----------
        **kwargs : Any
            Settings to override. Use double underscore for nested settings
            (e.g., ``database__host="localhost"``).

        Yields
        ------
        Config
            The config instance with overridden values.

        Examples
        --------
        >>> with config.override(safemode=False, database__host="test"):
        ...     # config.safemode is False here
        ...     pass
        >>> # config.safemode is restored
        """
        # Store original values
        backup = {}

        # Convert double underscore to nested access
        converted = {}
        for key, value in kwargs.items():
            if "__" in key:
                parts = key.split("__")
                converted[tuple(parts)] = value
            else:
                converted[(key,)] = value

        try:
            # Save originals and apply overrides
            for key_parts, value in converted.items():
                if len(key_parts) == 1:
                    key = key_parts[0]
                    if hasattr(self, key):
                        backup[key_parts] = deepcopy(getattr(self, key))
                        setattr(self, key, value)
                elif len(key_parts) == 2:
                    group, attr = key_parts
                    if hasattr(self, group):
                        group_obj = getattr(self, group)
                        if hasattr(group_obj, attr):
                            backup[key_parts] = deepcopy(getattr(group_obj, attr))
                            setattr(group_obj, attr, value)

            yield self

        finally:
            # Restore original values
            for key_parts, original in backup.items():
                if len(key_parts) == 1:
                    setattr(self, key_parts[0], original)
                elif len(key_parts) == 2:
                    group, attr = key_parts
                    setattr(getattr(self, group), attr, original)

    @staticmethod
    def save_template(
        path: str | Path = "datajoint.json",
        minimal: bool = True,
        create_secrets_dir: bool = True,
    ) -> Path:
        """
        Create a template datajoint.json configuration file.

        Credentials should NOT be stored in datajoint.json. Instead, use either:

        - Environment variables (``DJ_USER``, ``DJ_PASS``, ``DJ_HOST``, etc.)
        - The ``.secrets/`` directory (created alongside datajoint.json)

        Parameters
        ----------
        path : str or Path, optional
            Where to save the template. Default ``'datajoint.json'``.
        minimal : bool, optional
            If True (default), create minimal template with just database settings.
            If False, create full template with all available settings.
        create_secrets_dir : bool, optional
            If True (default), also create a ``.secrets/`` directory with
            template files for credentials.

        Returns
        -------
        Path
            Absolute path to the created config file.

        Raises
        ------
        FileExistsError
            If config file already exists (won't overwrite).

        Examples
        --------
        >>> import datajoint as dj
        >>> dj.config.save_template()  # Creates minimal template + .secrets/
        >>> dj.config.save_template("full-config.json", minimal=False)
        """
        filepath = Path(path)
        if filepath.exists():
            raise FileExistsError(f"File already exists: {filepath}. Remove it first or choose a different path.")

        if minimal:
            template = {
                "database": {
                    "host": "localhost",
                    "port": 3306,
                },
            }
        else:
            template = {
                "database": {
                    "host": "localhost",
                    "port": 3306,
                    "reconnect": True,
                    "use_tls": None,
                },
                "connection": {
                    "init_function": None,
                    "charset": "",
                },
                "display": {
                    "limit": 12,
                    "width": 14,
                    "show_tuple_count": True,
                },
                "stores": {
                    "default": "main",
                    "filepath_default": "raw_data",
                    "main": {
                        "protocol": "file",
                        "location": "/data/my-project/main",
                        "partition_pattern": None,
                        "token_length": 8,
                        "subfolding": None,
                    },
                    "raw_data": {
                        "protocol": "file",
                        "location": "/data/my-project/raw",
                    },
                },
                "loglevel": "INFO",
                "safemode": True,
                "enable_python_native_blobs": True,
                "cache": None,
                "query_cache": None,
                "download_path": ".",
            }

        with open(filepath, "w") as f:
            json.dump(template, f, indent=2)
            f.write("\n")

        logger.info(f"Created template configuration at {filepath.absolute()}")

        # Create .secrets/ directory with template files
        if create_secrets_dir:
            secrets_dir = filepath.parent / SECRETS_DIRNAME
            secrets_dir.mkdir(exist_ok=True)

            # Create placeholder secret files
            secret_templates = {
                "database.user": "your_username",
                "database.password": "your_password",
            }
            for secret_name, placeholder in secret_templates.items():
                secret_file = secrets_dir / secret_name
                if not secret_file.exists():
                    secret_file.write_text(placeholder)

            # Create .gitignore to prevent committing secrets
            gitignore_path = secrets_dir / ".gitignore"
            if not gitignore_path.exists():
                gitignore_path.write_text("# Never commit secrets\n*\n!.gitignore\n")

            logger.info(
                f"Created {SECRETS_DIRNAME}/ directory with credential templates. "
                f"Edit the files in {secrets_dir.absolute()}/ to set your credentials."
            )

        return filepath.absolute()

    # Dict-like access for convenience
    def __getitem__(self, key: str) -> Any:
        """Get setting by dot-notation key (e.g., 'database.host')."""
        parts = key.split(".")
        obj: Any = self
        for part in parts:
            if hasattr(obj, part):
                obj = getattr(obj, part)
            elif isinstance(obj, dict):
                obj = obj[part]
            else:
                raise KeyError(f"Setting '{key}' not found")
        # Unwrap SecretStr for compatibility
        if isinstance(obj, SecretStr):
            return obj.get_secret_value()
        return obj

    def __setitem__(self, key: str, value: Any) -> None:
        """Set setting by dot-notation key (e.g., 'database.host')."""
        parts = key.split(".")
        if len(parts) == 1:
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                raise KeyError(f"Setting '{key}' not found")
        else:
            obj: Any = self
            for part in parts[:-1]:
                obj = getattr(obj, part)
            setattr(obj, parts[-1], value)

    def __delitem__(self, key: str) -> None:
        """Reset setting to default by dot-notation key."""
        # Get the default value from the model fields (access from class, not instance)
        parts = key.split(".")
        if len(parts) == 1:
            field_info = type(self).model_fields.get(key)
            if field_info is not None:
                default = field_info.default
                if default is not None:
                    setattr(self, key, default)
                elif field_info.default_factory is not None:
                    setattr(self, key, field_info.default_factory())
                else:
                    setattr(self, key, None)
            else:
                raise KeyError(f"Setting '{key}' not found")
        else:
            # For nested settings, reset to None or empty
            obj: Any = self
            for part in parts[:-1]:
                obj = getattr(obj, part)
            setattr(obj, parts[-1], None)

    def get(self, key: str, default: Any = None) -> Any:
        """Get setting with optional default value."""
        try:
            return self[key]
        except KeyError:
            return default


def _create_config() -> Config:
    """Create and initialize the global config instance."""
    cfg = Config()

    # Find config file (recursive parent search)
    config_path = find_config_file()

    if config_path is not None:
        try:
            cfg.load(config_path)
        except Exception as e:
            warnings.warn(f"Failed to load config from {config_path}: {e}")
    else:
        warnings.warn(
            f"No {CONFIG_FILENAME} found. Using defaults and environment variables. "
            f"Run `dj.config.save_template()` to create a template configuration.",
            stacklevel=2,
        )

    # Find and load secrets
    secrets_dir = find_secrets_dir(config_path)
    if secrets_dir is not None:
        cfg._load_secrets(secrets_dir)

    # Set initial log level
    logger.setLevel(cfg.loglevel)

    return cfg


# Global config instance
config = _create_config()
