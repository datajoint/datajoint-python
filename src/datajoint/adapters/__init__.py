"""
Database adapter registry for DataJoint.

This module provides the adapter factory function and exports all adapters.
"""

from __future__ import annotations

from .base import DatabaseAdapter
from .mysql import MySQLAdapter
from .postgres import PostgreSQLAdapter

__all__ = ["DatabaseAdapter", "MySQLAdapter", "PostgreSQLAdapter", "get_adapter"]

# Adapter registry mapping backend names to adapter classes
ADAPTERS: dict[str, type[DatabaseAdapter]] = {
    "mysql": MySQLAdapter,
    "postgresql": PostgreSQLAdapter,
    "postgres": PostgreSQLAdapter,  # Alias for postgresql
}


def get_adapter(backend: str) -> DatabaseAdapter:
    """
    Get adapter instance for the specified database backend.

    Parameters
    ----------
    backend : str
        Backend name: 'mysql', 'postgresql', or 'postgres'.

    Returns
    -------
    DatabaseAdapter
        Adapter instance for the specified backend.

    Raises
    ------
    ValueError
        If the backend is not supported.

    Examples
    --------
    >>> from datajoint.adapters import get_adapter
    >>> mysql_adapter = get_adapter('mysql')
    >>> postgres_adapter = get_adapter('postgresql')
    """
    backend_lower = backend.lower()

    if backend_lower not in ADAPTERS:
        supported = sorted(set(ADAPTERS.keys()))
        raise ValueError(f"Unknown database backend: {backend}. " f"Supported backends: {', '.join(supported)}")

    return ADAPTERS[backend_lower]()
