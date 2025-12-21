"""
Legacy attribute adapter module.

This module provides backward compatibility for the deprecated AttributeAdapter class.
New code should use :class:`datajoint.AttributeType` instead.

.. deprecated:: 0.15
    Use :class:`datajoint.AttributeType` with ``encode``/``decode`` methods.
"""

import re
import warnings
from typing import Any

from .attribute_type import AttributeType, get_type, is_type_registered
from .errors import DataJointError


class AttributeAdapter(AttributeType):
    """
    Legacy base class for attribute adapters.

    .. deprecated:: 0.15
        Use :class:`datajoint.AttributeType` with ``encode``/``decode`` methods instead.

    This class provides backward compatibility for existing adapters that use
    the ``attribute_type``, ``put()``, and ``get()`` API.

    Migration guide::

        # Old style (deprecated):
        class GraphAdapter(dj.AttributeAdapter):
            attribute_type = "longblob"

            def put(self, graph):
                return list(graph.edges)

            def get(self, edges):
                return nx.Graph(edges)

        # New style (recommended):
        @dj.register_type
        class GraphType(dj.AttributeType):
            type_name = "graph"
            dtype = "longblob"

            def encode(self, graph, *, key=None):
                return list(graph.edges)

            def decode(self, edges, *, key=None):
                return nx.Graph(edges)
    """

    # Subclasses can set this as a class attribute instead of property
    attribute_type: str = None  # type: ignore

    def __init__(self):
        # Emit deprecation warning on instantiation
        warnings.warn(
            f"{self.__class__.__name__} uses the deprecated AttributeAdapter API. "
            "Migrate to AttributeType with encode/decode methods.",
            DeprecationWarning,
            stacklevel=2,
        )

    @property
    def type_name(self) -> str:
        """
        Infer type name from class name for legacy adapters.

        Legacy adapters were identified by their variable name in the context dict,
        not by a property. For backward compatibility, we use the lowercase class name.
        """
        # Check if a _type_name was explicitly set (for context-based lookup)
        if hasattr(self, "_type_name"):
            return self._type_name
        # Fall back to class name
        return self.__class__.__name__.lower()

    @property
    def dtype(self) -> str:
        """Map legacy attribute_type to new dtype property."""
        attr_type = self.attribute_type
        if attr_type is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} must define 'attribute_type' " "(or migrate to AttributeType with 'dtype')"
            )
        return attr_type

    def encode(self, value: Any, *, key: dict | None = None) -> Any:
        """Delegate to legacy put() method."""
        return self.put(value)

    def decode(self, stored: Any, *, key: dict | None = None) -> Any:
        """Delegate to legacy get() method."""
        return self.get(stored)

    def put(self, obj: Any) -> Any:
        """
        Convert an object of the adapted type into a storable value.

        .. deprecated:: 0.15
            Override ``encode()`` instead.

        Args:
            obj: An object of the adapted type.

        Returns:
            Value to store in the database.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement put() or migrate to encode()")

    def get(self, value: Any) -> Any:
        """
        Convert a value from the database into the adapted type.

        .. deprecated:: 0.15
            Override ``decode()`` instead.

        Args:
            value: Value from the database.

        Returns:
            Object of the adapted type.
        """
        raise NotImplementedError(f"{self.__class__.__name__} must implement get() or migrate to decode()")


def get_adapter(context: dict | None, adapter_name: str) -> AttributeType:
    """
    Get an attribute type/adapter by name.

    This function provides backward compatibility by checking both:
    1. The global type registry (new system)
    2. The schema context dict (legacy system)

    Args:
        context: Schema context dictionary (for legacy adapters).
        adapter_name: The adapter/type name, with or without angle brackets.

    Returns:
        The AttributeType instance.

    Raises:
        DataJointError: If the adapter is not found or invalid.
    """
    adapter_name = adapter_name.lstrip("<").rstrip(">")

    # First, check the global type registry (new system)
    if is_type_registered(adapter_name):
        return get_type(adapter_name)

    # Fall back to context-based lookup (legacy system)
    if context is None:
        raise DataJointError(
            f"Attribute type <{adapter_name}> is not registered. " "Use @dj.register_type to register custom types."
        )

    try:
        adapter = context[adapter_name]
    except KeyError:
        raise DataJointError(
            f"Attribute type <{adapter_name}> is not defined. "
            "Register it with @dj.register_type or include it in the schema context."
        )

    # Validate it's an AttributeType (or legacy AttributeAdapter)
    if not isinstance(adapter, AttributeType):
        raise DataJointError(
            f"Attribute adapter '{adapter_name}' must be an instance of "
            "datajoint.AttributeType (or legacy datajoint.AttributeAdapter)"
        )

    # For legacy adapters from context, store the name they were looked up by
    if isinstance(adapter, AttributeAdapter):
        adapter._type_name = adapter_name

    # Validate the dtype/attribute_type
    dtype = adapter.dtype
    if not isinstance(dtype, str) or not re.match(r"^\w", dtype):
        raise DataJointError(f"Invalid dtype '{dtype}' in attribute type <{adapter_name}>")

    return adapter
