"""
Codec type system for DataJoint.

This module provides the Codec base class for creating custom data types
that extend DataJoint's native type system. Codecs provide encode/decode
semantics for complex Python objects.

Codecs auto-register when subclassed - no decorator needed (Python 3.10+).

Example:
    class GraphCodec(dj.Codec):
        name = "graph"

        def get_dtype(self, is_external: bool) -> str:
            return "<blob>"

        def encode(self, graph, *, key=None, store_name=None):
            return {'nodes': list(graph.nodes()), 'edges': list(graph.edges())}

        def decode(self, stored, *, key=None):
            import networkx as nx
            G = nx.Graph()
            G.add_nodes_from(stored['nodes'])
            G.add_edges_from(stored['edges'])
            return G

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
from typing import Any

from .errors import DataJointError

logger = logging.getLogger(__name__.split(".")[0])

# Global codec registry - maps name to Codec instance
_codec_registry: dict[str, Codec] = {}
_entry_points_loaded: bool = False


class Codec(ABC):
    """
    Base class for codec types. Subclasses auto-register by name.

    Requires Python 3.10+.

    Attributes
    ----------
    name : str or None
        Unique identifier used in ``<name>`` syntax. Must be set by subclasses.

    Examples
    --------
    >>> class GraphCodec(dj.Codec):
    ...     name = "graph"
    ...
    ...     def get_dtype(self, is_external: bool) -> str:
    ...         return "<blob>"
    ...
    ...     def encode(self, graph, *, key=None, store_name=None):
    ...         return {'nodes': list(graph.nodes()), 'edges': list(graph.edges())}
    ...
    ...     def decode(self, stored, *, key=None):
    ...         import networkx as nx
    ...         G = nx.Graph()
    ...         G.add_nodes_from(stored['nodes'])
    ...         G.add_edges_from(stored['edges'])
    ...         return G

    Use in table definitions::

        class Connectivity(dj.Manual):
            definition = '''
            id : int
            ---
            graph_data : <graph>
            '''

    Skip auto-registration for abstract base classes::

        class ExternalOnlyCodec(dj.Codec, register=False):
            '''Abstract base - not registered.'''
            ...
    """

    name: str | None = None  # Must be set by concrete subclasses

    def __init_subclass__(cls, *, register: bool = True, **kwargs):
        """Auto-register concrete codecs when subclassed."""
        super().__init_subclass__(**kwargs)

        if not register:
            return  # Skip registration for abstract bases

        if cls.name is None:
            return  # Skip registration if no name (abstract)

        if not isinstance(cls.name, str) or not cls.name:
            raise DataJointError(f"Codec name must be a non-empty string, got {cls.name!r}")

        if cls.name in _codec_registry:
            existing = _codec_registry[cls.name]
            if type(existing) is not cls:
                raise DataJointError(
                    f"Codec <{cls.name}> already registered by " f"{type(existing).__module__}.{type(existing).__name__}"
                )
            return  # Same class, idempotent

        _codec_registry[cls.name] = cls()
        logger.debug(f"Registered codec <{cls.name}> from {cls.__module__}.{cls.__name__}")

    def get_dtype(self, is_external: bool) -> str:
        """
        Return the storage dtype for this codec.

        Parameters
        ----------
        is_external : bool
            True if ``@`` modifier present (external storage).

        Returns
        -------
        str
            A core type (e.g., ``"bytes"``, ``"json"``) or another codec
            (e.g., ``"<hash>"``).

        Raises
        ------
        NotImplementedError
            If not overridden by subclass.
        DataJointError
            If external storage not supported but requested.
        """
        raise NotImplementedError(f"Codec <{self.name}> must implement get_dtype()")

    @abstractmethod
    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> Any:
        """
        Encode Python value for storage.

        Parameters
        ----------
        value : any
            The Python object to store.
        key : dict, optional
            Primary key values. May be needed for path construction.
        store_name : str, optional
            Target store name for external storage.

        Returns
        -------
        any
            Value in the format expected by the dtype.
        """
        ...

    @abstractmethod
    def decode(self, stored: Any, *, key: dict | None = None) -> Any:
        """
        Decode stored value back to Python.

        Parameters
        ----------
        stored : any
            Data retrieved from storage.
        key : dict, optional
            Primary key values.

        Returns
        -------
        any
            The reconstructed Python object.
        """
        ...

    def validate(self, value: Any) -> None:
        """
        Validate a value before encoding.

        Override this method to add type checking or domain constraints.
        Called automatically before ``encode()`` during INSERT operations.
        The default implementation accepts any value.

        Parameters
        ----------
        value : any
            The value to validate.

        Raises
        ------
        TypeError
            If the value has an incompatible type.
        ValueError
            If the value fails domain validation.
        """
        pass

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}(name={self.name!r})>"


def parse_type_spec(spec: str) -> tuple[str, str | None]:
    """
    Parse a type specification into type name and optional store parameter.

    Parameters
    ----------
    spec : str
        Type specification string, with or without angle brackets.

    Returns
    -------
    tuple[str, str | None]
        ``(type_name, store_name)``. ``store_name`` is None if not specified,
        empty string if ``@`` present without name (default store).

    Examples
    --------
    >>> parse_type_spec("<blob>")
    ("blob", None)
    >>> parse_type_spec("<blob@cold>")
    ("blob", "cold")
    >>> parse_type_spec("<blob@>")
    ("blob", "")
    """
    # Strip angle brackets
    spec = spec.strip("<>").strip()

    if "@" in spec:
        type_name, store_name = spec.split("@", 1)
        return type_name.strip(), store_name.strip()

    return spec, None


def unregister_codec(name: str) -> None:
    """
    Remove a codec from the registry.

    Primarily useful for testing. Use with caution in production code.

    Parameters
    ----------
    name : str
        The codec name to unregister.

    Raises
    ------
    DataJointError
        If the codec is not registered.
    """
    name = name.strip("<>")
    if name not in _codec_registry:
        raise DataJointError(f"Codec <{name}> is not registered")
    del _codec_registry[name]


def get_codec(name: str) -> Codec:
    """
    Retrieve a registered codec by name.

    Looks up the codec in the explicit registry first, then attempts
    to load from installed packages via entry points.

    Parameters
    ----------
    name : str
        The codec name, with or without angle brackets.
        Store parameters (e.g., ``"<blob@cold>"``) are stripped.

    Returns
    -------
    Codec
        The registered Codec instance.

    Raises
    ------
    DataJointError
        If the codec is not found.
    """
    # Strip angle brackets and store parameter
    type_name, _ = parse_type_spec(name)

    # Check explicit registry first
    if type_name in _codec_registry:
        return _codec_registry[type_name]

    # Lazy-load entry points
    _load_entry_points()

    if type_name in _codec_registry:
        return _codec_registry[type_name]

    raise DataJointError(
        f"Unknown codec: <{type_name}>. " f"Ensure the codec is defined (inherit from dj.Codec with name='{type_name}')."
    )


def list_codecs() -> list[str]:
    """
    List all registered codec names.

    Returns
    -------
    list[str]
        Sorted list of registered codec names.
    """
    _load_entry_points()
    return sorted(_codec_registry.keys())


def is_codec_registered(name: str) -> bool:
    """
    Check if a codec name is registered.

    Parameters
    ----------
    name : str
        The codec name to check (store parameters are ignored).

    Returns
    -------
    bool
        True if the codec is registered.
    """
    type_name, _ = parse_type_spec(name)
    if type_name in _codec_registry:
        return True
    _load_entry_points()
    return type_name in _codec_registry


def _load_entry_points() -> None:
    """
    Load codecs from installed packages via entry points.

    Codecs are discovered from the ``datajoint.codecs`` entry point group
    (also checks legacy ``datajoint.types`` for backward compatibility).

    Packages declare codecs in pyproject.toml::

        [project.entry-points."datajoint.codecs"]
        zarr_array = "dj_zarr:ZarrArrayCodec"

    This function is idempotent - entry points are only loaded once.
    """
    global _entry_points_loaded
    if _entry_points_loaded:
        return

    _entry_points_loaded = True

    try:
        from importlib.metadata import entry_points
    except ImportError:
        logger.debug("importlib.metadata not available, skipping entry point discovery")
        return

    # Load from both new and legacy entry point groups
    for group in ("datajoint.codecs", "datajoint.types"):
        try:
            eps = entry_points(group=group)
        except TypeError:
            # Older API fallback
            eps = entry_points().get(group, [])

        for ep in eps:
            if ep.name in _codec_registry:
                # Already registered explicitly, skip entry point
                continue
            try:
                codec_class = ep.load()
                # The class should auto-register via __init_subclass__
                # But if it's an old-style class, manually register
                if ep.name not in _codec_registry and hasattr(codec_class, "name"):
                    _codec_registry[ep.name] = codec_class()
                logger.debug(f"Loaded codec <{ep.name}> from entry point {ep.value}")
            except Exception as e:
                logger.warning(f"Failed to load codec '{ep.name}' from {ep.value}: {e}")


def resolve_dtype(
    dtype: str, seen: set[str] | None = None, store_name: str | None = None
) -> tuple[str, list[Codec], str | None]:
    """
    Resolve a dtype string, following codec chains.

    If dtype references another codec (e.g., ``"<hash>"``), recursively
    resolves to find the ultimate storage type. Store parameters are propagated
    through the chain.

    Parameters
    ----------
    dtype : str
        The dtype string to resolve (e.g., ``"<blob>"``, ``"<blob@cold>"``, ``"bytes"``).
    seen : set[str], optional
        Set of already-seen codec names (for cycle detection).
    store_name : str, optional
        Store name from outer type specification (propagated inward).

    Returns
    -------
    tuple[str, list[Codec], str | None]
        ``(final_storage_type, codec_chain, resolved_store_name)``.
        Chain is ordered from outermost to innermost codec.

    Raises
    ------
    DataJointError
        If a circular type reference is detected.

    Examples
    --------
    >>> resolve_dtype("<blob>")
    ("bytes", [BlobCodec], None)
    >>> resolve_dtype("<blob@cold>")
    ("<hash>", [BlobCodec], "cold")
    >>> resolve_dtype("bytes")
    ("bytes", [], None)
    """
    if seen is None:
        seen = set()

    chain: list[Codec] = []

    # Check if dtype is a codec reference
    if dtype.startswith("<") and dtype.endswith(">"):
        type_name, dtype_store = parse_type_spec(dtype)

        # Store from this level overrides inherited store
        # Empty string means default store (@), None means no store specified
        if dtype_store is not None:
            effective_store = dtype_store
        else:
            effective_store = store_name

        if type_name in seen:
            raise DataJointError(f"Circular codec reference detected: <{type_name}>")

        seen.add(type_name)
        codec = get_codec(type_name)
        chain.append(codec)

        # Determine if external based on whether @ is present
        is_external = effective_store is not None

        # Get the inner dtype from the codec
        inner_dtype = codec.get_dtype(is_external)

        # Recursively resolve the inner dtype, propagating store
        final_dtype, inner_chain, resolved_store = resolve_dtype(inner_dtype, seen, effective_store)
        chain.extend(inner_chain)
        return final_dtype, chain, resolved_store

    # Not a codec - check if it has a store suffix (e.g., "blob@store")
    if "@" in dtype:
        base_type, dtype_store = dtype.split("@", 1)
        effective_store = dtype_store if dtype_store else store_name
        return base_type, chain, effective_store

    # Plain type - return as-is with propagated store
    return dtype, chain, store_name


def lookup_codec(codec_spec: str) -> tuple[Codec, str | None]:
    """
    Look up a codec from a type specification string.

    Parses a codec specification (e.g., ``"<blob@store>"``) and returns
    the codec instance along with any store name.

    Parameters
    ----------
    codec_spec : str
        The codec specification, with or without angle brackets.
        May include store parameter (e.g., ``"<blob@cold>"``).

    Returns
    -------
    tuple[Codec, str | None]
        ``(codec_instance, store_name)`` or ``(codec_instance, None)``.

    Raises
    ------
    DataJointError
        If the codec is not found.
    """
    type_name, store_name = parse_type_spec(codec_spec)

    if is_codec_registered(type_name):
        return get_codec(type_name), store_name

    raise DataJointError(f"Codec <{type_name}> is not registered. " "Define a Codec subclass with name='{type_name}'.")


# =============================================================================
# Decode Helper
# =============================================================================


def decode_attribute(attr, data, squeeze: bool = False):
    """
    Decode raw database value using attribute's codec or native type handling.

    This is the central decode function used by all fetch methods. It handles:
    - Codec chains (e.g., <blob@store> → <hash> → bytes)
    - Native type conversions (JSON, UUID)
    - External storage downloads (via config["download_path"])

    Args:
        attr: Attribute from the table's heading.
        data: Raw value fetched from the database.
        squeeze: If True, remove singleton dimensions from numpy arrays.

    Returns:
        Decoded Python value.
    """
    import json
    import uuid as uuid_module

    import numpy as np

    if data is None:
        return None

    if attr.codec:
        # Get store if present for external storage
        store = getattr(attr, "store", None)
        if store is not None:
            dtype_spec = f"<{attr.codec.name}@{store}>"
        else:
            dtype_spec = f"<{attr.codec.name}>"

        final_dtype, type_chain, _ = resolve_dtype(dtype_spec)

        # Process the final storage type (what's in the database)
        if final_dtype.lower() == "json":
            data = json.loads(data)
        elif final_dtype.lower() in ("longblob", "blob", "mediumblob", "tinyblob"):
            pass  # Blob data is already bytes
        elif final_dtype.lower() == "binary(16)":
            data = uuid_module.UUID(bytes=data)

        # Apply decoders in reverse order: innermost first, then outermost
        for codec in reversed(type_chain):
            data = codec.decode(data, key=None)

        # Squeeze arrays if requested
        if squeeze and isinstance(data, np.ndarray):
            data = data.squeeze()

        return data

    # No codec - handle native types
    if attr.json:
        return json.loads(data)

    if attr.uuid:
        import uuid as uuid_module

        return uuid_module.UUID(bytes=data)

    if attr.is_blob:
        return data  # Raw bytes

    # Native types - pass through unchanged
    return data


# =============================================================================
# Auto-register built-in codecs
# =============================================================================

# Import builtin_codecs module to register built-in codecs
# This import has a side effect: it registers the codecs via __init_subclass__
from . import builtin_codecs as _builtin_codecs  # noqa: F401, E402
