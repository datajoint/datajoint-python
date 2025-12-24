"""
Backward compatibility re-exports.

Lineage functionality has been merged into heading.py.
"""

from .heading import LineageTable, compute_schema_lineage

__all__ = ["LineageTable", "compute_schema_lineage"]
