"""
Custom attribute type system for DataJoint.

This module provides the AttributeType base class and registration mechanism
for creating custom data types that extend DataJoint's native type system.

Custom types enable seamless integration of complex Python objects (like NumPy arrays,
graphs, or domain-specific structures) with DataJoint's relational storage.

Example:
    @dj.register_type
    class GraphType(dj.AttributeType):
        type_name = "graph"
        dtype = "longblob"

        def encode(self, graph: nx.Graph) -> list:
            return list(graph.edges)

        def decode(self, edges: list) -> nx.Graph:
            return nx.Graph(edges)

    # Then use in table definitions:
    class MyTable(dj.Manual):
        definition = '''
        id : int
        ---
        data : <graph>
        '''
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .errors import DataJointError

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__.split(".")[0])

# Global type registry - maps type_name to AttributeType instance
_type_registry: dict[str, AttributeType] = {}
_entry_points_loaded: bool = False


class AttributeType(ABC):
    """
    Base class for custom DataJoint attribute types.

    Subclass this to create custom types that can be used in table definitions
    with the ``<type_name>`` syntax. Custom types define bidirectional conversion
    between Python objects and DataJoint's storage format.

    Attributes:
        type_name: Unique identifier used in ``<type_name>`` syntax
        dtype: Underlying DataJoint storage type

    Example:
        @dj.register_type
        class GraphType(dj.AttributeType):
            type_name = "graph"
            dtype = "longblob"

            def encode(self, graph):
                return list(graph.edges)

            def decode(self, edges):
                import networkx as nx
                return nx.Graph(edges)

    The type can then be used in table definitions::

        class Connectivity(dj.Manual):
            definition = '''
            id : int
            ---
            graph_data : <graph>
            '''
    """

    @property
    @abstractmethod
    def type_name(self) -> str:
        """
        Unique identifier for this type, used in table definitions as ``<type_name>``.

        This name must be unique across all registered types. It should be lowercase
        with underscores (e.g., "graph", "zarr_array", "compressed_image").

        Returns:
            The type name string without angle brackets.
        """
        ...

    @property
    @abstractmethod
    def dtype(self) -> str:
        """
        The underlying DataJoint type used for storage.

        Can be:
            - A native type: ``"longblob"``, ``"blob"``, ``"varchar(255)"``, ``"int"``, ``"json"``
            - An external type: ``"blob@store"``, ``"attach@store"``
            - The object type: ``"object"``
            - Another custom type: ``"<other_type>"`` (enables type chaining)

        Returns:
            The storage type specification string.
        """
        ...

    @abstractmethod
    def encode(self, value: Any, *, key: dict | None = None) -> Any:
        """
        Convert a Python object to the storable format.

        Called during INSERT operations to transform user-provided objects
        into a format suitable for storage in the underlying ``dtype``.

        Args:
            value: The Python object to store.
            key: Primary key values as a dict. Available when the dtype uses
                 object storage and may be needed for path construction.

        Returns:
            Value in the format expected by ``dtype``. For example:
                - For ``dtype="longblob"``: any picklable Python object
                - For ``dtype="object"``: path string or file-like object
                - For ``dtype="varchar(N)"``: string
        """
        ...

    @abstractmethod
    def decode(self, stored: Any, *, key: dict | None = None) -> Any:
        """
        Convert stored data back to a Python object.

        Called during FETCH operations to reconstruct the original Python
        object from the stored format.

        Args:
            stored: Data retrieved from storage. Type depends on ``dtype``:
                - For ``"object"``: an ``ObjectRef`` handle
                - For blob types: the unpacked Python object
                - For native types: the native Python value (str, int, etc.)
            key: Primary key values as a dict.

        Returns:
            The reconstructed Python object.
        """
        ...

    def validate(self, value: Any) -> None:
        """
        Validate a value before encoding.

        Override this method to add type checking or domain constraints.
        Called automatically before ``encode()`` during INSERT operations.
        The default implementation accepts any value.

        Args:
            value: The value to validate.

        Raises:
            TypeError: If the value has an incompatible type.
            ValueError: If the value fails domain validation.
        """
        pass

    def default(self) -> Any:
        """
        Return a default value for this type.

        Override if the type has a sensible default value. The default
        implementation raises NotImplementedError, indicating no default exists.

        Returns:
            The default value for this type.

        Raises:
            NotImplementedError: If no default exists (the default behavior).
        """
        raise NotImplementedError(f"No default value for type <{self.type_name}>")

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(type_name={self.type_name!r}, dtype={self.dtype!r})>"


def register_type(cls: type[AttributeType]) -> type[AttributeType]:
    """
    Register a custom attribute type with DataJoint.

    Can be used as a decorator or called directly. The type becomes available
    for use in table definitions with the ``<type_name>`` syntax.

    Args:
        cls: An AttributeType subclass to register.

    Returns:
        The same class, unmodified (allows use as decorator).

    Raises:
        DataJointError: If a type with the same name is already registered
            by a different class.
        TypeError: If cls is not an AttributeType subclass.

    Example:
        As a decorator::

            @dj.register_type
            class GraphType(dj.AttributeType):
                type_name = "graph"
                ...

        Or called directly::

            dj.register_type(GraphType)
    """
    if not isinstance(cls, type) or not issubclass(cls, AttributeType):
        raise TypeError(f"register_type requires an AttributeType subclass, got {cls!r}")

    instance = cls()
    name = instance.type_name

    if not isinstance(name, str) or not name:
        raise DataJointError(f"type_name must be a non-empty string, got {name!r}")

    if name in _type_registry:
        existing = _type_registry[name]
        if type(existing) is not cls:
            raise DataJointError(
                f"Type <{name}> is already registered by " f"{type(existing).__module__}.{type(existing).__name__}"
            )
        # Same class registered twice - idempotent, no error
        return cls

    _type_registry[name] = instance
    logger.debug(f"Registered attribute type <{name}> from {cls.__module__}.{cls.__name__}")
    return cls


def parse_type_spec(spec: str) -> tuple[str, str | None]:
    """
    Parse a type specification into type name and optional store parameter.

    Handles formats like:
    - "<xblob>" -> ("xblob", None)
    - "<xblob@cold>" -> ("xblob", "cold")
    - "xblob@cold" -> ("xblob", "cold")
    - "xblob" -> ("xblob", None)

    Args:
        spec: Type specification string, with or without angle brackets.

    Returns:
        Tuple of (type_name, store_name). store_name is None if not specified.
    """
    # Strip angle brackets
    spec = spec.strip("<>").strip()

    if "@" in spec:
        type_name, store_name = spec.split("@", 1)
        return type_name.strip(), store_name.strip()

    return spec, None


def unregister_type(name: str) -> None:
    """
    Remove a type from the registry.

    Primarily useful for testing. Use with caution in production code.

    Args:
        name: The type_name to unregister.

    Raises:
        DataJointError: If the type is not registered.
    """
    name = name.strip("<>")
    if name not in _type_registry:
        raise DataJointError(f"Type <{name}> is not registered")
    del _type_registry[name]


def get_type(name: str) -> AttributeType:
    """
    Retrieve a registered attribute type by name.

    Looks up the type in the explicit registry first, then attempts
    to load from installed packages via entry points.

    Args:
        name: The type name, with or without angle brackets.
              Store parameters (e.g., "<xblob@cold>") are stripped.

    Returns:
        The registered AttributeType instance.

    Raises:
        DataJointError: If the type is not found.
    """
    # Strip angle brackets and store parameter
    type_name, _ = parse_type_spec(name)

    # Check explicit registry first
    if type_name in _type_registry:
        return _type_registry[type_name]

    # Lazy-load entry points
    _load_entry_points()

    if type_name in _type_registry:
        return _type_registry[type_name]

    raise DataJointError(
        f"Unknown attribute type: <{type_name}>. "
        f"Ensure the type is registered via @dj.register_type or installed as a package."
    )


def list_types() -> list[str]:
    """
    List all registered type names.

    Returns:
        Sorted list of registered type names.
    """
    _load_entry_points()
    return sorted(_type_registry.keys())


def is_type_registered(name: str) -> bool:
    """
    Check if a type name is registered.

    Args:
        name: The type name to check (store parameters are ignored).

    Returns:
        True if the type is registered.
    """
    type_name, _ = parse_type_spec(name)
    if type_name in _type_registry:
        return True
    _load_entry_points()
    return type_name in _type_registry


def _load_entry_points() -> None:
    """
    Load attribute types from installed packages via entry points.

    Types are discovered from the ``datajoint.types`` entry point group.
    Packages declare types in pyproject.toml::

        [project.entry-points."datajoint.types"]
        zarr_array = "dj_zarr:ZarrArrayType"

    This function is idempotent - entry points are only loaded once.
    """
    global _entry_points_loaded
    if _entry_points_loaded:
        return

    _entry_points_loaded = True

    try:
        from importlib.metadata import entry_points
    except ImportError:
        # Python < 3.10 fallback
        try:
            from importlib_metadata import entry_points
        except ImportError:
            logger.debug("importlib.metadata not available, skipping entry point discovery")
            return

    try:
        # Python 3.10+ / importlib_metadata 3.6+
        eps = entry_points(group="datajoint.types")
    except TypeError:
        # Older API
        eps = entry_points().get("datajoint.types", [])

    for ep in eps:
        if ep.name in _type_registry:
            # Already registered explicitly, skip entry point
            continue
        try:
            type_class = ep.load()
            register_type(type_class)
            logger.debug(f"Loaded attribute type <{ep.name}> from entry point {ep.value}")
        except Exception as e:
            logger.warning(f"Failed to load attribute type '{ep.name}' from {ep.value}: {e}")


def resolve_dtype(
    dtype: str, seen: set[str] | None = None, store_name: str | None = None
) -> tuple[str, list[AttributeType], str | None]:
    """
    Resolve a dtype string, following type chains.

    If dtype references another custom type (e.g., "<other_type>"), recursively
    resolves to find the ultimate storage type. Store parameters are propagated
    through the chain.

    Args:
        dtype: The dtype string to resolve (e.g., "<xblob>", "<xblob@cold>", "longblob").
        seen: Set of already-seen type names (for cycle detection).
        store_name: Store name from outer type specification (propagated inward).

    Returns:
        Tuple of (final_storage_type, list_of_types_in_chain, resolved_store_name).
        The chain is ordered from outermost to innermost type.

    Raises:
        DataJointError: If a circular type reference is detected.

    Examples:
        >>> resolve_dtype("<xblob>")
        ("json", [XBlobType, ContentType], None)

        >>> resolve_dtype("<xblob@cold>")
        ("json", [XBlobType, ContentType], "cold")

        >>> resolve_dtype("longblob")
        ("longblob", [], None)
    """
    if seen is None:
        seen = set()

    chain: list[AttributeType] = []

    # Check if dtype is a custom type reference
    if dtype.startswith("<") and dtype.endswith(">"):
        type_name, dtype_store = parse_type_spec(dtype)

        # Store from this level overrides inherited store
        effective_store = dtype_store if dtype_store is not None else store_name

        if type_name in seen:
            raise DataJointError(f"Circular type reference detected: <{type_name}>")

        seen.add(type_name)
        attr_type = get_type(type_name)
        chain.append(attr_type)

        # Recursively resolve the inner dtype, propagating store
        inner_dtype, inner_chain, resolved_store = resolve_dtype(attr_type.dtype, seen, effective_store)
        chain.extend(inner_chain)
        return inner_dtype, chain, resolved_store

    # Not a custom type - check if it has a store suffix (e.g., "blob@store")
    if "@" in dtype:
        base_type, dtype_store = dtype.split("@", 1)
        effective_store = dtype_store if dtype_store else store_name
        return base_type, chain, effective_store

    # Plain type - return as-is with propagated store
    return dtype, chain, store_name


# =============================================================================
# Built-in Attribute Types
# =============================================================================


class DJBlobType(AttributeType):
    """
    Built-in type for DataJoint's native serialization format.

    This type handles serialization of arbitrary Python objects (including NumPy arrays,
    dictionaries, lists, etc.) using DataJoint's binary blob format. The format includes:

    - Protocol headers (``mYm`` for MATLAB-compatible, ``dj0`` for Python-native)
    - Optional compression (zlib)
    - Support for NumPy arrays, datetime objects, UUIDs, and nested structures

    The ``<djblob>`` type is the explicit way to specify DataJoint's serialization.
    It stores data in a MySQL ``LONGBLOB`` column.

    Example:
        @schema
        class ProcessedData(dj.Manual):
            definition = '''
            data_id : int
            ---
            results : <djblob>      # Serialized Python objects
            raw_bytes : longblob    # Raw bytes (no serialization)
            '''

    Note:
        Plain ``longblob`` columns store and return raw bytes without serialization.
        Use ``<djblob>`` when you need automatic serialization of Python objects.
        Existing schemas using implicit blob serialization should migrate to ``<djblob>``
        using ``dj.migrate.migrate_blob_columns()``.
    """

    type_name = "djblob"
    dtype = "longblob"

    def encode(self, value: Any, *, key: dict | None = None) -> bytes:
        """
        Serialize a Python object to DataJoint's blob format.

        Args:
            value: Any serializable Python object (dict, list, numpy array, etc.)
            key: Primary key values (unused for blob serialization).

        Returns:
            Serialized bytes with protocol header and optional compression.
        """
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """
        Deserialize DataJoint blob format back to a Python object.

        Args:
            stored: Serialized blob bytes.
            key: Primary key values (unused for blob serialization).

        Returns:
            The deserialized Python object.
        """
        from . import blob

        return blob.unpack(stored, squeeze=False)


class DJBlobExternalType(AttributeType):
    """
    Built-in type for externally-stored DataJoint blobs.

    Similar to ``<djblob>`` but stores data in external blob storage instead
    of inline in the database. Useful for large objects.

    The store name is specified when defining the column type.

    Example:
        @schema
        class LargeData(dj.Manual):
            definition = '''
            data_id : int
            ---
            large_array : blob@mystore  # External storage with auto-serialization
            '''
    """

    # Note: This type isn't directly usable via <djblob_external> syntax
    # It's used internally when blob@store syntax is detected
    type_name = "djblob_external"
    dtype = "blob@store"  # Placeholder - actual store is determined at declaration time

    def encode(self, value: Any, *, key: dict | None = None) -> bytes:
        """Serialize a Python object to DataJoint's blob format."""
        from . import blob

        return blob.pack(value, compress=True)

    def decode(self, stored: bytes, *, key: dict | None = None) -> Any:
        """Deserialize DataJoint blob format back to a Python object."""
        from . import blob

        return blob.unpack(stored, squeeze=False)


def _register_builtin_types() -> None:
    """
    Register DataJoint's built-in attribute types.

    Called automatically during module initialization.
    """
    register_type(DJBlobType)


# Register built-in types when module is loaded
_register_builtin_types()
