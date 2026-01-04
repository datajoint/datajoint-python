"""
DataJoint Settings using pydantic-settings.

This module provides a strongly-typed configuration system for DataJoint.

Configuration sources (in priority order):
1. Environment variables (DJ_*)
2. Secrets directories (.secrets/ in project, /run/secrets/datajoint/)
3. Project config file (datajoint.json, searched recursively up to .git/.hg)

Example usage:
    >>> import datajoint as dj
    >>> dj.config.database.host
    'localhost'
    >>> with dj.config.override(safemode=False):
    ...     # dangerous operations here

Project structure:
    myproject/
    ├── .git/
    ├── datajoint.json      # Project config (commit this)
    ├── .secrets/           # Local secrets (gitignore this)
    │   ├── database.password
    │   └── aws.secret_access_key
    └── src/
        └── analysis.py     # Config found via parent search
"""

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
    "external.aws_access_key_id": "DJ_AWS_ACCESS_KEY_ID",
    "external.aws_secret_access_key": "DJ_AWS_SECRET_ACCESS_KEY",
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

    Searches upward from `start` (default: cwd) until finding the config file
    or hitting a project boundary (.git, .hg) or filesystem root.

    Args:
        start: Directory to start search from. Defaults to current working directory.

    Returns:
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
    1. .secrets/ in same directory as datajoint.json (project secrets)
    2. /run/secrets/datajoint/ (Docker/Kubernetes secrets)

    Args:
        config_path: Path to datajoint.json if found.

    Returns:
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

    Args:
        secrets_dir: Path to secrets directory.
        name: Name of the secret file (e.g., 'database.password').

    Returns:
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


class ExternalSettings(BaseSettings):
    """External storage credentials."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    aws_access_key_id: str | None = Field(default=None, validation_alias="DJ_AWS_ACCESS_KEY_ID")
    aws_secret_access_key: SecretStr | None = Field(default=None, validation_alias="DJ_AWS_SECRET_ACCESS_KEY")


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


class ObjectStorageSettings(BaseSettings):
    """Object storage configuration for the object type."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_OBJECT_STORAGE_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    # Required settings
    project_name: str | None = Field(default=None, description="Unique project identifier")
    protocol: str | None = Field(default=None, description="Storage protocol: file, s3, gcs, azure")
    location: str | None = Field(default=None, description="Base path or bucket prefix")

    # Cloud storage settings
    bucket: str | None = Field(default=None, description="Bucket name (S3, GCS)")
    container: str | None = Field(default=None, description="Container name (Azure)")
    endpoint: str | None = Field(default=None, description="S3 endpoint URL")
    access_key: str | None = Field(default=None, description="Access key")
    secret_key: SecretStr | None = Field(default=None, description="Secret key")
    secure: bool = Field(default=True, description="Use HTTPS")

    # Optional settings
    default_store: str | None = Field(default=None, description="Default store name when not specified")
    partition_pattern: str | None = Field(default=None, description="Path pattern with {attribute} placeholders")
    token_length: int = Field(default=8, ge=4, le=16, description="Random suffix length for filenames")

    # Named stores configuration (object_storage.stores.<name>.*)
    stores: dict[str, dict[str, Any]] = Field(default_factory=dict, description="Named object stores")


class Config(BaseSettings):
    """
    Main DataJoint configuration.

    Settings are loaded from (in priority order):
    1. Environment variables (DJ_*)
    2. Secrets directory (.secrets/ or /run/secrets/datajoint/)
    3. Config file (datajoint.json, searched in parent directories)
    4. Default values

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
    external: ExternalSettings = Field(default_factory=ExternalSettings)
    jobs: JobsSettings = Field(default_factory=JobsSettings)
    object_storage: ObjectStorageSettings = Field(default_factory=ObjectStorageSettings)

    # Top-level settings
    loglevel: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(default="INFO", validation_alias="DJ_LOG_LEVEL")
    safemode: bool = True
    fetch_format: Literal["array", "frame"] = "array"
    enable_python_native_blobs: bool = True
    filepath_checksum_size_limit: int | None = None

    # External stores configuration
    stores: dict[str, dict[str, Any]] = Field(default_factory=dict)

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

    def get_store_spec(self, store: str) -> dict[str, Any]:
        """
        Get configuration for an external store.

        Args:
            store: Name of the store to retrieve

        Returns:
            Store configuration dict with validated fields

        Raises:
            DataJointError: If store is not configured or has invalid config
        """
        if store not in self.stores:
            raise DataJointError(f"Storage '{store}' is requested but not configured")

        spec = dict(self.stores[store])
        spec.setdefault("subfolding", DEFAULT_SUBFOLDING)

        # Validate protocol
        protocol = spec.get("protocol", "").lower()
        supported_protocols = ("file", "s3", "gcs", "azure")
        if protocol not in supported_protocols:
            raise DataJointError(
                f'Missing or invalid protocol in config.stores["{store}"]. '
                f'Supported protocols: {", ".join(supported_protocols)}'
            )

        # Define required and allowed keys by protocol
        required_keys: dict[str, tuple[str, ...]] = {
            "file": ("protocol", "location"),
            "s3": ("protocol", "endpoint", "bucket", "access_key", "secret_key", "location"),
            "gcs": ("protocol", "bucket", "location"),
            "azure": ("protocol", "container", "location"),
        }
        allowed_keys: dict[str, tuple[str, ...]] = {
            "file": ("protocol", "location", "subfolding", "stage"),
            "s3": (
                "protocol",
                "endpoint",
                "bucket",
                "access_key",
                "secret_key",
                "location",
                "secure",
                "subfolding",
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

        return spec

    def get_object_storage_spec(self) -> dict[str, Any]:
        """
        Get validated object storage configuration.

        Returns:
            Object storage configuration dict

        Raises:
            DataJointError: If object storage is not configured or has invalid config
        """
        os_settings = self.object_storage

        # Check if object storage is configured
        if not os_settings.protocol:
            raise DataJointError(
                "Object storage is not configured. Set object_storage.protocol in datajoint.json "
                "or DJ_OBJECT_STORAGE_PROTOCOL environment variable."
            )

        if not os_settings.project_name:
            raise DataJointError(
                "Object storage project_name is required. Set object_storage.project_name in datajoint.json "
                "or DJ_OBJECT_STORAGE_PROJECT_NAME environment variable."
            )

        protocol = os_settings.protocol.lower()
        supported_protocols = ("file", "s3", "gcs", "azure")
        if protocol not in supported_protocols:
            raise DataJointError(
                f"Invalid object_storage.protocol: {protocol}. " f'Supported protocols: {", ".join(supported_protocols)}'
            )

        # Build spec dict
        spec = {
            "project_name": os_settings.project_name,
            "protocol": protocol,
            "location": os_settings.location or "",
            "partition_pattern": os_settings.partition_pattern,
            "token_length": os_settings.token_length,
        }

        # Add protocol-specific settings
        if protocol == "s3":
            if not os_settings.endpoint or not os_settings.bucket:
                raise DataJointError("object_storage.endpoint and object_storage.bucket are required for S3")
            if not os_settings.access_key or not os_settings.secret_key:
                raise DataJointError("object_storage.access_key and object_storage.secret_key are required for S3")
            spec.update(
                {
                    "endpoint": os_settings.endpoint,
                    "bucket": os_settings.bucket,
                    "access_key": os_settings.access_key,
                    "secret_key": os_settings.secret_key.get_secret_value() if os_settings.secret_key else None,
                    "secure": os_settings.secure,
                }
            )
        elif protocol == "gcs":
            if not os_settings.bucket:
                raise DataJointError("object_storage.bucket is required for GCS")
            spec["bucket"] = os_settings.bucket
        elif protocol == "azure":
            if not os_settings.container:
                raise DataJointError("object_storage.container is required for Azure")
            spec["container"] = os_settings.container

        return spec

    def get_object_store_spec(self, store_name: str | None = None) -> dict[str, Any]:
        """
        Get validated configuration for a specific object store.

        Args:
            store_name: Name of the store (None for default store)

        Returns:
            Object store configuration dict

        Raises:
            DataJointError: If store is not configured or has invalid config
        """
        if store_name is None:
            # Return default store spec
            return self.get_object_storage_spec()

        os_settings = self.object_storage

        # Check if named store exists
        if store_name not in os_settings.stores:
            raise DataJointError(
                f"Object store '{store_name}' is not configured. "
                f"Add object_storage.stores.{store_name}.* settings to datajoint.json"
            )

        store_config = os_settings.stores[store_name]
        protocol = store_config.get("protocol", "").lower()

        supported_protocols = ("file", "s3", "gcs", "azure")
        if protocol not in supported_protocols:
            raise DataJointError(
                f"Invalid protocol for store '{store_name}': {protocol}. "
                f'Supported protocols: {", ".join(supported_protocols)}'
            )

        # Use project_name from default config if not specified in store
        project_name = store_config.get("project_name") or os_settings.project_name
        if not project_name:
            raise DataJointError(
                f"project_name is required for object store '{store_name}'. "
                "Set object_storage.project_name or object_storage.stores.{store_name}.project_name"
            )

        # Build spec dict
        spec = {
            "project_name": project_name,
            "protocol": protocol,
            "location": store_config.get("location", ""),
            "partition_pattern": store_config.get("partition_pattern") or os_settings.partition_pattern,
            "token_length": store_config.get("token_length") or os_settings.token_length,
            "store_name": store_name,
        }

        # Add protocol-specific settings
        if protocol == "s3":
            endpoint = store_config.get("endpoint")
            bucket = store_config.get("bucket")
            if not endpoint or not bucket:
                raise DataJointError(f"endpoint and bucket are required for S3 store '{store_name}'")
            spec.update(
                {
                    "endpoint": endpoint,
                    "bucket": bucket,
                    "access_key": store_config.get("access_key"),
                    "secret_key": store_config.get("secret_key"),
                    "secure": store_config.get("secure", True),
                }
            )
        elif protocol == "gcs":
            bucket = store_config.get("bucket")
            if not bucket:
                raise DataJointError(f"bucket is required for GCS store '{store_name}'")
            spec["bucket"] = bucket
        elif protocol == "azure":
            container = store_config.get("container")
            if not container:
                raise DataJointError(f"container is required for Azure store '{store_name}'")
            spec["container"] = container

        return spec

    def load(self, filename: str | Path) -> None:
        """
        Load settings from a JSON file.

        Args:
            filename: Path to load configuration from.
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
            elif len(parts) == 4:
                # Handle object_storage.stores.<name>.<attr> pattern
                group, subgroup, store_name, attr = parts
                if group == "object_storage" and subgroup == "stores":
                    if store_name not in self.object_storage.stores:
                        self.object_storage.stores[store_name] = {}
                    self.object_storage.stores[store_name][attr] = value

    def _load_secrets(self, secrets_dir: Path) -> None:
        """Load secrets from a secrets directory."""
        self._secrets_dir = secrets_dir

        # Map of secret file names to config paths
        secret_mappings = {
            "database.password": ("database", "password"),
            "database.user": ("database", "user"),
            "aws.access_key_id": ("external", "aws_access_key_id"),
            "aws.secret_access_key": ("external", "aws_secret_access_key"),
        }

        for secret_name, (group, attr) in secret_mappings.items():
            value = read_secret_file(secrets_dir, secret_name)
            if value is not None:
                group_obj = getattr(self, group)
                # Only set if not already set by env var
                if getattr(group_obj, attr) is None:
                    setattr(group_obj, attr, value)
                    logger.debug(f"Loaded secret '{secret_name}' from {secrets_dir}")

    @contextmanager
    def override(self, **kwargs: Any) -> Iterator["Config"]:
        """
        Temporarily override configuration values.

        Args:
            **kwargs: Settings to override. Use double underscore for nested
                     settings (e.g., database__host="localhost")

        Yields:
            The config instance with overridden values

        Example:
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
        - Environment variables (DJ_USER, DJ_PASS, DJ_HOST, etc.)
        - The .secrets/ directory (created alongside datajoint.json)

        Args:
            path: Where to save the template. Defaults to 'datajoint.json' in current directory.
            minimal: If True (default), create a minimal template with just database settings.
                    If False, create a full template with all available settings.
            create_secrets_dir: If True (default), also create a .secrets/ directory
                               with template files for credentials.

        Returns:
            Path to the created config file.

        Raises:
            FileExistsError: If config file already exists (won't overwrite).

        Example:
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
                "object_storage": {
                    "project_name": None,
                    "protocol": None,
                    "location": None,
                    "bucket": None,
                    "endpoint": None,
                    "secure": True,
                    "partition_pattern": None,
                    "token_length": 8,
                },
                "stores": {},
                "loglevel": "INFO",
                "safemode": True,
                "fetch_format": "array",
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
