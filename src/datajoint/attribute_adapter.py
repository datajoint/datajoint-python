"""
Attribute adapter module - compatibility shim.

This module re-exports functions from attribute_type for backward compatibility
with code that imports from attribute_adapter.

.. deprecated:: 0.15
    Import directly from :mod:`datajoint.attribute_type` instead.
"""

from .attribute_type import (
    AttributeType,
    get_type,
    is_type_registered,
    parse_type_spec,
)
from .errors import DataJointError


def get_adapter(context: dict | None, adapter_name: str) -> tuple[AttributeType, str | None]:
    """
    Get an attribute type by name.

    Args:
        context: Ignored (legacy parameter, kept for API compatibility).
        adapter_name: The type name, with or without angle brackets.
                      May include store parameter (e.g., "<xblob@cold>").

    Returns:
        Tuple of (AttributeType instance, store_name or None).

    Raises:
        DataJointError: If the type is not found.
    """
    # Parse type name and optional store parameter
    type_name, store_name = parse_type_spec(adapter_name)

    # Look up in the global type registry
    if is_type_registered(type_name):
        return get_type(type_name), store_name

    raise DataJointError(f"Attribute type <{type_name}> is not registered. " "Use @dj.register_type to register custom types.")
