"""Plugin system for third-party storage protocols.

Third-party packages register adapters via entry points::

    [project.entry-points."datajoint.storage"]
    myprotocol = "my_package:MyStorageAdapter"

The adapter is auto-discovered when DataJoint encounters the protocol name
in a store configuration. No explicit import is needed.
"""

from abc import ABC, abstractmethod
from typing import Any
import logging

import fsspec

from . import errors

logger = logging.getLogger(__name__)


class StorageAdapter(ABC):
    """Base class for storage protocol adapters.

    Subclass this and declare an entry point to add a new storage protocol
    to DataJoint. At minimum, implement ``create_filesystem`` and set
    ``protocol``, ``required_keys``, and ``allowed_keys``.
    """

    protocol: str
    required_keys: tuple[str, ...] = ()
    allowed_keys: tuple[str, ...] = ()

    @abstractmethod
    def create_filesystem(self, spec: dict[str, Any]) -> fsspec.AbstractFileSystem:
        """Return an fsspec filesystem instance for this protocol."""
        ...

    def validate_spec(self, spec: dict[str, Any]) -> None:
        """Validate protocol-specific config fields."""
        missing = [k for k in self.required_keys if k not in spec]
        if missing:
            raise errors.DataJointError(
                f'{self.protocol} store is missing: {", ".join(missing)}'
            )
        all_allowed = set(self.allowed_keys) | _COMMON_STORE_KEYS
        invalid = [k for k in spec if k not in all_allowed]
        if invalid:
            raise errors.DataJointError(
                f'Invalid key(s) for {self.protocol}: {", ".join(invalid)}'
            )

    def full_path(self, spec: dict[str, Any], relpath: str) -> str:
        """Construct storage path from a relative path."""
        location = spec.get("location", "")
        return f"{location}/{relpath}" if location else relpath

    def get_url(self, spec: dict[str, Any], path: str) -> str:
        """Return a display URL for the stored object."""
        return f"{self.protocol}://{path}"


_COMMON_STORE_KEYS = frozenset({
    "protocol",
    "location",
    "subfolding",
    "partition_pattern",
    "token_length",
    "hash_prefix",
    "schema_prefix",
    "filepath_prefix",
    "stage",
})

_adapter_registry: dict[str, StorageAdapter] = {}
_adapters_loaded: bool = False


def get_storage_adapter(protocol: str) -> StorageAdapter | None:
    """Look up a registered storage adapter by protocol name."""
    global _adapters_loaded
    if not _adapters_loaded:
        _discover_adapters()
        _adapters_loaded = True
    return _adapter_registry.get(protocol)


def _discover_adapters() -> None:
    """Load storage adapters from datajoint.storage entry points."""
    try:
        from importlib.metadata import entry_points
    except ImportError:
        logger.debug("importlib.metadata not available, skipping adapter discovery")
        return

    try:
        eps = entry_points(group="datajoint.storage")
    except TypeError:
        eps = entry_points().get("datajoint.storage", [])

    for ep in eps:
        if ep.name in _adapter_registry:
            continue
        try:
            adapter_cls = ep.load()
            adapter = adapter_cls()
            _adapter_registry[adapter.protocol] = adapter
            logger.debug(f"Loaded storage adapter: {adapter.protocol}")
        except Exception as e:
            logger.warning(f"Failed to load storage adapter '{ep.name}': {e}")
