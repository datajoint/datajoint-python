"""
Settings for DataJoint using pydantic-settings
"""

import collections
import json
import logging
import os
import pprint
from contextlib import contextmanager
from enum import Enum
from typing import Any, Dict, Iterator, List, Literal, Optional, Tuple

from pydantic import Field, field_validator
from pydantic.aliases import AliasChoices
from pydantic_settings import BaseSettings, SettingsConfigDict

from .errors import DataJointError

LOCALCONFIG = "dj_local_conf.json"
GLOBALCONFIG = ".datajoint_config.json"
# subfolding for external storage in filesystem.
# 2, 2 means that file abcdef is stored as /ab/cd/abcdef
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
log_levels = {
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "CRITICAL": logging.CRITICAL,
    "DEBUG": logging.DEBUG,
    "ERROR": logging.ERROR,
    None: logging.NOTSET,
}


class DatabaseSettings(BaseSettings):
    """Database connection settings"""

    host: str = "localhost"
    password: Optional[str] = None
    user: Optional[str] = None
    port: int = 3306
    reconnect: bool = True
    use_tls: Optional[bool] = None

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="allow",
    )


class ConnectionSettings(BaseSettings):
    """Connection settings"""

    init_function: Optional[str] = None
    charset: str = ""  # pymysql uses '' as default

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="allow",
    )


class DisplaySettings(BaseSettings):
    """Display settings"""

    limit: int = 12
    width: int = 14
    show_tuple_count: bool = True

    model_config = SettingsConfigDict(
        env_prefix="",
        case_sensitive=False,
        extra="allow",
    )


class ExternalSettings(BaseSettings):
    """External storage settings"""

    aws_access_key_id: Optional[str] = None
    aws_secret_access_key: Optional[str] = None

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="allow",
    )


class DataJointSettings(BaseSettings):
    """Main DataJoint Settings using Pydantic"""

    database: DatabaseSettings = Field(default_factory=DatabaseSettings)
    connection: ConnectionSettings = Field(default_factory=ConnectionSettings)
    display: DisplaySettings = Field(default_factory=DisplaySettings)
    external: ExternalSettings = Field(default_factory=ExternalSettings)

    loglevel: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        validation_alias=AliasChoices("loglevel", "DJ_LOG_LEVEL"),
    )
    safemode: bool = True
    fetch_format: Literal["array", "frame"] = "array"
    enable_python_native_blobs: bool = True
    add_hidden_timestamp: bool = False
    filepath_checksum_size_limit: Optional[int] = None

    # External stores configuration (not managed by pydantic directly)
    stores: Dict[str, Dict[str, Any]] = Field(default_factory=dict)
    cache: Optional[str] = None
    query_cache: Optional[str] = None

    model_config = SettingsConfigDict(
        env_prefix="DJ_",
        case_sensitive=False,
        extra="allow",
        validate_assignment=True,
    )

    @field_validator("cache", "query_cache", mode="before")
    @classmethod
    def validate_path(cls, v: Any) -> Optional[str]:
        """Convert path-like objects to strings"""
        if v is None:
            return v
        # Convert Path-like objects to strings
        if hasattr(v, "__fspath__"):
            return str(v)
        return v

    @field_validator("loglevel")
    @classmethod
    def validate_loglevel(cls, v: str) -> str:
        """Validate and set logging level"""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v not in valid_levels:
            raise ValueError(f"'{v}' is not a valid logging value {tuple(valid_levels)}")
        # Set the logger level
        logger.setLevel(v)
        return v


class ConfigWrapper(collections.abc.MutableMapping):
    """
    Wrapper class that provides backward compatibility with the old Config interface.
    Wraps a pydantic Settings instance to provide dict-like access with dot notation support.
    """

    def __init__(self, settings: DataJointSettings):
        self._settings = settings
        self._original_values: Dict[str, Any] = {}  # For context manager support
        self._extra: Dict[str, Any] = {}  # Store arbitrary extra keys not in pydantic model

    @property
    def _conf(self) -> Dict[str, Any]:
        """Backward compatibility: expose internal config as _conf"""
        result = self._to_dict()
        result.update(self._extra)
        return result

    def _get_nested(self, key: str) -> Any:
        """Get a value using dot notation (e.g., 'database.host')"""
        # Check if it's in the extra dict first
        if key in self._extra:
            return self._extra[key]

        parts = key.split(".")
        obj = self._settings

        for part in parts:
            if hasattr(obj, part):
                obj = getattr(obj, part)
            elif isinstance(obj, dict):
                obj = obj[part]
            else:
                raise KeyError(f"Key '{key}' not found")
        return obj

    def _set_nested(self, key: str, value: Any) -> None:
        """Set a value using dot notation (e.g., 'database.host')"""
        # Apply validators if they exist
        if key in validators and not validators[key](value):
            raise DataJointError(f"Validator for {key} did not pass")

        parts = key.split(".")

        if len(parts) == 1:
            # Top-level attribute
            if hasattr(self._settings, key):
                setattr(self._settings, key, value)
            else:
                # Store in extra dict for arbitrary keys
                self._extra[key] = value
        else:
            # Try to set in pydantic model first
            try:
                obj = self._settings
                for part in parts[:-1]:
                    if hasattr(obj, part):
                        obj = getattr(obj, part)
                    elif isinstance(obj, dict):
                        if part not in obj:
                            obj[part] = {}
                        obj = obj[part]
                    else:
                        # Can't navigate, store as arbitrary key
                        self._extra[key] = value
                        return

                # Set the final value
                final_key = parts[-1]
                if hasattr(obj, final_key):
                    setattr(obj, final_key, value)
                elif isinstance(obj, dict):
                    obj[final_key] = value
                else:
                    # Store as arbitrary key
                    self._extra[key] = value
            except (AttributeError, KeyError):
                # If we can't set it in the model, store as arbitrary key
                self._extra[key] = value

        # Special handling for loglevel
        if key == "loglevel":
            logger.setLevel(value)

    def __getitem__(self, key: str) -> Any:
        """Get item using dict notation"""
        try:
            return self._get_nested(key)
        except (AttributeError, KeyError):
            raise KeyError(f"Key '{key}' not found")

    def __setitem__(self, key: str, value: Any) -> None:
        """Set item using dict notation"""
        logger.debug(f"Setting {key} to {value}")
        self._set_nested(key, value)

    def __delitem__(self, key: str) -> None:
        """Delete item by setting it to None (pydantic fields) or removing it (_extra dict)"""
        # Check if it's in extra dict first
        if key in self._extra:
            del self._extra[key]
            return

        parts = key.split(".")
        if len(parts) == 1:
            # For pydantic model fields, set to None instead of deleting
            # (deleting would break iteration over model_fields)
            if key in self._settings.__class__.model_fields:
                setattr(self._settings, key, None)
            else:
                raise KeyError(f"Key '{key}' not found")
        else:
            # For nested fields, also set to None
            obj = self._settings
            for part in parts[:-1]:
                obj = getattr(obj, part)
            field_name = parts[-1]
            if field_name in obj.__class__.model_fields:
                setattr(obj, field_name, None)
            else:
                raise KeyError(f"Key '{key}' not found")

    def __iter__(self) -> Iterator[str]:
        """Iterate over all configuration keys (flattened)"""
        return iter(self._get_all_keys())

    def __len__(self) -> int:
        """Return number of configuration keys"""
        return len(self._get_all_keys())

    def _get_all_keys(self) -> List[str]:
        """Get all configuration keys in flat dot notation"""
        keys: List[str] = []

        def _extract_keys(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, BaseSettings):
                for field_name in obj.__class__.model_fields:
                    field_value = getattr(obj, field_name)
                    full_key = f"{prefix}.{field_name}" if prefix else field_name
                    if isinstance(field_value, BaseSettings):
                        _extract_keys(field_value, full_key)
                    else:
                        keys.append(full_key)
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    full_key = f"{prefix}.{k}" if prefix else k
                    if isinstance(v, (dict, BaseSettings)):
                        _extract_keys(v, full_key)
                    else:
                        keys.append(full_key)

        _extract_keys(self._settings)
        # Add extra keys
        keys.extend(self._extra.keys())
        return keys

    def __str__(self) -> str:
        """String representation"""
        return pprint.pformat(self._to_dict(), indent=4)

    def __repr__(self) -> str:
        """Repr representation"""
        return self.__str__()

    def _to_dict(self) -> Dict[str, Any]:
        """Convert settings to a flat dict with dot notation keys"""
        result: Dict[str, Any] = {}

        def _flatten(obj: Any, prefix: str = "") -> None:
            if isinstance(obj, BaseSettings):
                for field_name in obj.__class__.model_fields:
                    field_value = getattr(obj, field_name)
                    full_key = f"{prefix}.{field_name}" if prefix else field_name
                    if isinstance(field_value, BaseSettings):
                        _flatten(field_value, full_key)
                    elif isinstance(field_value, dict):
                        result[full_key] = field_value
                    else:
                        result[full_key] = field_value
            elif isinstance(obj, dict):
                for k, v in obj.items():
                    full_key = f"{prefix}.{k}" if prefix else k
                    result[full_key] = v

        _flatten(self._settings)
        return result

    def __eq__(self, other: Any) -> bool:
        """Compare two Config instances"""
        if isinstance(other, ConfigWrapper):
            return self._to_dict() == other._to_dict()
        elif isinstance(other, dict):
            return self._to_dict() == other
        return False

    def save(self, filename: str, verbose: bool = False) -> None:
        """
        Saves the settings in JSON format to the given file path.

        :param filename: filename of the local JSON settings file.
        :param verbose: report having saved the settings file
        """
        with open(filename, "w") as fid:
            json.dump(self._to_dict(), fid, indent=4)
        if verbose:
            logger.info("Saved settings in " + filename)

    def load(self, filename: str) -> None:
        """
        Updates the setting from config file in JSON format.

        :param filename: filename of the local JSON settings file.
        """
        if filename is None:
            filename = LOCALCONFIG

        with open(filename, "r") as fid:
            logger.info(f"DataJoint is configured from {os.path.abspath(filename)}")
            data = json.load(fid)

            # Update settings from loaded data
            for key, value in data.items():
                try:
                    self[key] = value
                except Exception as e:
                    logger.warning(f"Could not set config key '{key}': {e}")

    def save_local(self, verbose: bool = False) -> None:
        """saves the settings in the local config file"""
        self.save(LOCALCONFIG, verbose)

    def save_global(self, verbose: bool = False) -> None:
        """saves the settings in the global config file"""
        self.save(os.path.expanduser(os.path.join("~", GLOBALCONFIG)), verbose)

    def get_store_spec(self, store: str) -> Dict[str, Any]:
        """
        find configuration of external stores for blobs and attachments
        """
        try:
            spec = self._settings.stores[store]
        except KeyError:
            raise DataJointError(f"Storage {store} is requested but not configured")

        spec: Dict[str, Any] = dict(spec)  # Make a copy
        spec["subfolding"] = spec.get("subfolding", DEFAULT_SUBFOLDING)

        spec_keys_by_protocol: Dict[str, Tuple[str, ...]] = {  # REQUIRED in uppercase and allowed in lowercase
            "file": ("PROTOCOL", "LOCATION", "subfolding", "stage"),
            "s3": (
                "PROTOCOL",
                "ENDPOINT",
                "BUCKET",
                "ACCESS_KEY",
                "SECRET_KEY",
                "LOCATION",
                "secure",
                "subfolding",
                "stage",
                "proxy_server",
            ),
        }

        try:
            spec_keys: Tuple[str, ...] = spec_keys_by_protocol[spec.get("protocol", "").lower()]
        except KeyError:
            raise DataJointError(f'Missing or invalid protocol in dj.config["stores"]["{store}"]')

        # check that all required keys are present in spec
        try:
            raise DataJointError(
                f'dj.config["stores"]["{store}"] is missing "{next(k.lower() for k in spec_keys if k.isupper() and k.lower() not in spec)}"'  # noqa: E501
            )
        except StopIteration:
            pass

        # check that only allowed keys are present in spec
        try:
            raise DataJointError(
                f'Invalid key "{next(k for k in spec if k.upper() not in spec_keys and k.lower() not in spec_keys)}" in dj.config["stores"]["{store}"]'  # noqa: E501
            )
        except StopIteration:
            pass  # no invalid keys

        return spec

    @contextmanager
    def __call__(self, **kwargs: Any) -> Iterator["ConfigWrapper"]:
        """
        The config object can also be used in a with statement to change the state of the configuration
        temporarily. kwargs to the context manager are the keys into config, where '.' is replaced by a
        double underscore '__'. The context manager yields the changed config object.

        Example:
        >>> import datajoint as dj
        >>> with dj.config(safemode=False, database__host="localhost") as cfg:
        >>>     # do dangerous stuff here
        """
        # Save current values
        backup_values: Dict[str, Any] = {}
        converted_kwargs: Dict[str, Any] = {k.replace("__", "."): v for k, v in kwargs.items()}

        try:
            # Save original values
            for key in converted_kwargs:
                try:
                    backup_values[key] = self[key]
                except KeyError:
                    backup_values[key] = None

            # Apply new values
            for key, value in converted_kwargs.items():
                self[key] = value

            yield self

        finally:
            # Restore original values
            for key, value in backup_values.items():
                if value is not None:
                    self[key] = value


# Default configuration dictionary for backward compatibility
default = {
    "database.host": "localhost",
    "database.password": None,
    "database.user": None,
    "database.port": 3306,
    "database.reconnect": True,
    "connection.init_function": None,
    "connection.charset": "",
    "loglevel": "INFO",
    "safemode": True,
    "fetch_format": "array",
    "display.limit": 12,
    "display.width": 14,
    "display.show_tuple_count": True,
    "database.use_tls": None,
    "enable_python_native_blobs": True,
    "add_hidden_timestamp": False,
    "filepath_checksum_size_limit": None,
}

# Validators for backward compatibility
validators = collections.defaultdict(lambda: lambda value: True)
validators["database.port"] = lambda a: isinstance(a, int)


# Create settings instance
_settings = DataJointSettings()

# Create config wrapper for backward compatibility
config = ConfigWrapper(_settings)

# Load configuration from file
config_files = (os.path.expanduser(n) for n in (LOCALCONFIG, os.path.join("~", GLOBALCONFIG)))
try:
    config.load(next(n for n in config_files if os.path.exists(n)))
except StopIteration:
    logger.info("No config file was found.")

# Override login credentials with environment variables
# Note: pydantic-settings already handles this through validation_alias,
# but we keep this for any custom env vars not directly mapped
mapping = {}

# Check for any environment variables that weren't caught by pydantic
for env_key, config_key in [
    ("DJ_HOST", "database.host"),
    ("DJ_USER", "database.user"),
    ("DJ_PASS", "database.password"),
    ("DJ_PORT", "database.port"),
    ("DJ_AWS_ACCESS_KEY_ID", "external.aws_access_key_id"),
    ("DJ_AWS_SECRET_ACCESS_KEY", "external.aws_secret_access_key"),
    ("DJ_LOG_LEVEL", "loglevel"),
]:
    env_value = os.getenv(env_key)
    if env_value is not None:
        # Only add to mapping if pydantic didn't already set it
        try:
            current_value = config[config_key]
            if current_value is None or (config_key == "database.port" and current_value == 3306):
                if config_key == "database.port":
                    try:
                        mapping[config_key] = int(env_value)
                    except ValueError:
                        logger.warning(f"Invalid DJ_PORT value: {env_value}, using default port 3306")
                else:
                    mapping[config_key] = env_value
        except KeyError:
            # Key doesn't exist yet, add it
            if config_key == "database.port":
                try:
                    mapping[config_key] = int(env_value)
                except ValueError:
                    logger.warning(f"Invalid DJ_PORT value: {env_value}, using default port 3306")
            else:
                mapping[config_key] = env_value

if mapping:
    logger.info(f"Overloaded settings {tuple(mapping.keys())} from environment variables.")
    for key, value in mapping.items():
        config[key] = value

# Set logging level
logger.setLevel(log_levels[config["loglevel"]])


# Maintain singleton behavior for compatibility
class Config:
    """
    Backward compatibility class that mimics the old Config singleton behavior.
    This redirects all access to the global config instance.
    """

    instance = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Always use the global config instance
        pass

    def __getattr__(self, name: str) -> Any:
        return getattr(config, name)

    def __getitem__(self, item: str) -> Any:
        return config[item]

    def __setitem__(self, item: str, value: Any) -> None:
        config[item] = value

    def __str__(self) -> str:
        return str(config)

    def __repr__(self) -> str:
        return repr(config)

    def __delitem__(self, key: str) -> None:
        del config[key]

    def __iter__(self) -> Iterator[str]:
        return iter(config)

    def __len__(self) -> int:
        return len(config)

    def __eq__(self, other: Any) -> bool:
        return config.__eq__(other)
