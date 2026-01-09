"""
Type definitions for DataJoint.

This module defines type aliases used throughout the DataJoint codebase
to improve code clarity and enable better static type checking.

Python 3.10+ is required.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, TypeAlias

# Primary key types
PrimaryKey: TypeAlias = dict[str, Any]
"""A dictionary mapping attribute names to values that uniquely identify an entity."""

# Row/record types
Row: TypeAlias = dict[str, Any]
"""A single row/record as a dictionary mapping attribute names to values."""

# Attribute types
AttributeName: TypeAlias = str
"""Name of a table attribute/column."""

AttributeNames: TypeAlias = list[str]
"""List of attribute/column names."""

# Table and schema names
TableName: TypeAlias = str
"""Simple table name (e.g., 'session')."""

FullTableName: TypeAlias = str
"""Fully qualified table name (e.g., '`schema`.`table`')."""

SchemaName: TypeAlias = str
"""Database schema name."""

# Foreign key mapping
ForeignKeyMap: TypeAlias = dict[str, tuple[str, str]]
"""Mapping of child_attr -> (parent_table, parent_attr) for foreign keys."""

# Restriction types
Restriction: TypeAlias = str | dict[str, Any] | bool | "QueryExpression" | list[Any] | None
"""Valid restriction types for query operations."""

# Fetch result types
FetchResult: TypeAlias = list[dict[str, Any]]
"""Result of a fetch operation as list of dictionaries."""


# For avoiding circular imports
if TYPE_CHECKING:
    from .expression import QueryExpression
