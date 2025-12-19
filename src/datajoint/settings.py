"""
DataJoint Settings using pydantic-settings.

This module provides a strongly-typed configuration system for DataJoint.
Settings can be configured via:
1. Environment variables (prefixed with DJ_)
2. Configuration files (dj_local_conf.json or ~/.datajoint_config.json)
3. Direct attribute assignment

Example usage:
    >>> import datajoint as dj
    >>> dj.config.database.host = "localhost"
    >>> dj.config.database.port = 3306
    >>> with dj.config.override(safemode=False):
    ...     # dangerous operations here
"""

import json
import logging
import os
from contextlib import contextmanager
from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, Literal, Optional, Tuple, Union

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import DataJointError

LOCALCONFIG = "dj_local_conf.json"
GLOBALCONFIG = ".datajoint_config.json"
DEFAULT_SUBFOLDING = (2, 2)

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


class DatabaseSettings(BaseSettings):
    """Database connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="forbid",
        validate_assignment=True,
    )

    host: str = Field(default="localhost", validation_alias="DJ_HOST")
    user: Optional[str] = Field(default=None, validation_alias="DJ_USER")
    password: Optional[str] = Field(default=None, validation_alias="DJ_PASS")
    port: int = Field(default=3306, validation_alias="DJ_PORT")
    reconnect: bool = True
    use_tls: Optional[bool] = None


class ConnectionSettings(BaseSettings):
    """Connection behavior settings."""

    model_config = SettingsConfigDict(extra="forbid", validate_assignment=True)

    init_function: Optional[str] = None
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

    aws_access_key_id: Optional[str] = Field(
        default=None, validation_alias="DJ_AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key: Optional[str] = Field(
        default=None, validation_alias="DJ_AWS_SECRET_ACCESS_KEY"
    )


class StoreSpec(BaseSettings):
    """Configuration for an external store."""

    model_config = SettingsConfigDict(extra="forbid")

    protocol: Literal["file", "s3"]
    location: str
    subfolding: Tuple[int, ...] = DEFAULT_SUBFOLDING
    stage: Optional[str] = None

    # S3-specific fields
    endpoint: Optional[str] = None
    bucket: Optional[str] = None
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    secure: bool = True
    proxy_server: Optional[str] = None

    @model_validator(mode="after")
    def validate_s3_fields(self) -> "StoreSpec":
        """Ensure S3-specific fields are provided for S3 protocol."""
        if self.protocol == "s3":
            required = ["endpoint", "bucket", "access_key", "secret_key"]
            missing = [f for f in required if getattr(self, f) is None]
            if missing:
                raise ValueError(f"S3 store requires: {', '.join(missing)}")
        return self


class Config(BaseSettings):
    """
    Main DataJoint configuration.

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

    # Top-level settings
    loglevel: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO", validation_alias="DJ_LOG_LEVEL"
    )
    safemode: bool = True
    fetch_format: Literal["array", "frame"] = "array"
    enable_python_native_blobs: bool = True
    add_hidden_timestamp: bool = False
    filepath_checksum_size_limit: Optional[int] = None

    # External stores configuration
    stores: Dict[str, Dict[str, Any]] = Field(default_factory=dict)

    # Cache paths
    cache: Optional[Path] = None
    query_cache: Optional[Path] = None

    @field_validator("loglevel", mode="after")
    @classmethod
    def set_logger_level(cls, v: str) -> str:
        """Update logger level when loglevel changes."""
        logger.setLevel(v)
        return v

    @field_validator("cache", "query_cache", mode="before")
    @classmethod
    def convert_path(cls, v: Any) -> Optional[Path]:
        """Convert string paths to Path objects."""
        if v is None:
            return None
        return Path(v) if not isinstance(v, Path) else v

    def get_store_spec(self, store: str) -> Dict[str, Any]:
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
        if protocol not in ("file", "s3"):
            raise DataJointError(
                f'Missing or invalid protocol in config.stores["{store}"]'
            )

        # Define required and allowed keys by protocol
        required_keys: Dict[str, Tuple[str, ...]] = {
            "file": ("protocol", "location"),
            "s3": ("protocol", "endpoint", "bucket", "access_key", "secret_key", "location"),
        }
        allowed_keys: Dict[str, Tuple[str, ...]] = {
            "file": ("protocol", "location", "subfolding", "stage"),
            "s3": (
                "protocol", "endpoint", "bucket", "access_key", "secret_key",
                "location", "secure", "subfolding", "stage", "proxy_server",
            ),
        }

        # Check required keys
        missing = [k for k in required_keys[protocol] if k not in spec]
        if missing:
            raise DataJointError(
                f'config.stores["{store}"] is missing: {", ".join(missing)}'
            )

        # Check for invalid keys
        invalid = [k for k in spec if k not in allowed_keys[protocol]]
        if invalid:
            raise DataJointError(
                f'Invalid key(s) in config.stores["{store}"]: {", ".join(invalid)}'
            )

        return spec

    def save(self, filename: Union[str, Path], verbose: bool = False) -> None:
        """
        Save settings to a JSON file.

        Args:
            filename: Path to save the configuration
            verbose: If True, log the save operation
        """
        data = self._to_flat_dict()
        with open(filename, "w") as f:
            json.dump(data, f, indent=4, default=str)
        if verbose:
            logger.info(f"Saved settings to {filename}")

    def save_local(self, verbose: bool = False) -> None:
        """Save settings to local config file (dj_local_conf.json)."""
        self.save(LOCALCONFIG, verbose)

    def save_global(self, verbose: bool = False) -> None:
        """Save settings to global config file (~/.datajoint_config.json)."""
        self.save(Path.home() / GLOBALCONFIG, verbose)

    def load(self, filename: Union[str, Path, None] = None) -> None:
        """
        Load settings from a JSON file.

        Args:
            filename: Path to load configuration from. If None, uses LOCALCONFIG.
        """
        if filename is None:
            filename = LOCALCONFIG

        filepath = Path(filename)
        if not filepath.exists():
            raise FileNotFoundError(f"Config file not found: {filepath}")

        logger.info(f"Loading configuration from {filepath.absolute()}")

        with open(filepath) as f:
            data = json.load(f)

        self._update_from_flat_dict(data)

    def _to_flat_dict(self) -> Dict[str, Any]:
        """Convert settings to flat dict with dot notation keys."""
        result: Dict[str, Any] = {}

        def flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, BaseSettings):
                for name in obj.model_fields:
                    value = getattr(obj, name)
                    key = f"{prefix}.{name}" if prefix else name
                    if isinstance(value, BaseSettings):
                        flatten(value, key)
                    elif isinstance(value, Path):
                        result[key] = str(value)
                    else:
                        result[key] = value
            elif isinstance(obj, dict):
                result[prefix] = obj

        flatten(self)
        return result

    def _update_from_flat_dict(self, data: Dict[str, Any]) -> None:
        """Update settings from a flat dict with dot notation keys."""
        for key, value in data.items():
            parts = key.split(".")
            if len(parts) == 1:
                if hasattr(self, key):
                    setattr(self, key, value)
            elif len(parts) == 2:
                group, attr = parts
                if hasattr(self, group):
                    group_obj = getattr(self, group)
                    if hasattr(group_obj, attr):
                        setattr(group_obj, attr, value)

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

    # Backward compatibility: dict-like access
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

    def get(self, key: str, default: Any = None) -> Any:
        """Get setting with optional default value."""
        try:
            return self[key]
        except KeyError:
            return default


def _create_config() -> Config:
    """Create and initialize the global config instance."""
    cfg = Config()

    # Try to load from config file
    config_paths = [
        Path(LOCALCONFIG),
        Path.home() / GLOBALCONFIG,
    ]

    for path in config_paths:
        if path.exists():
            try:
                cfg.load(path)
                break
            except Exception as e:
                logger.warning(f"Failed to load config from {path}: {e}")
    else:
        logger.debug("No config file found, using defaults and environment variables")

    # Set initial log level
    logger.setLevel(cfg.loglevel)

    return cfg


# Global config instance
config = _create_config()
