"""
DataJoint for Python — a framework for scientific data pipelines.

DataJoint introduces the Relational Workflow Model, where your database schema
is an executable specification of your workflow. Tables represent workflow steps,
foreign keys encode dependencies, and computations are declarative.

Documentation: https://docs.datajoint.com
Source: https://github.com/datajoint/datajoint-python

Copyright 2014-2026 DataJoint Inc. and contributors.
Licensed under the Apache License, Version 2.0.

If DataJoint contributes to a publication, please cite:
https://doi.org/10.1101/031658
"""

__author__ = "DataJoint Contributors"
__date__ = "November 7, 2020"
__all__ = [
    "__author__",
    "__version__",
    "config",
    "conn",
    "Connection",
    "Schema",
    "schema",
    "VirtualModule",
    "virtual_schema",
    "list_schemas",
    "Table",
    "FreeTable",
    "Manual",
    "Lookup",
    "Imported",
    "Computed",
    "Part",
    "Not",
    "AndList",
    "Top",
    "U",
    "Diagram",
    "Di",
    "ERD",
    "kill",
    "MatCell",
    "MatStruct",
    # Codec API
    "Codec",
    "list_codecs",
    "get_codec",
    "errors",
    "migrate",
    "DataJointError",
    "key",
    "key_hash",
    "logger",
    "cli",
    "ObjectRef",
    "ValidationResult",
]

# =============================================================================
# Eager imports — core functionality needed immediately
# =============================================================================
from . import errors
from . import migrate
from .codecs import (
    Codec,
    get_codec,
    list_codecs,
)
from .blob import MatCell, MatStruct
from .connection import Connection, conn
from .errors import DataJointError
from .expression import AndList, Not, Top, U
from .hash import key_hash
from .logging import logger
from .objectref import ObjectRef
from .schemas import Schema, VirtualModule, list_schemas, virtual_schema
from .settings import config
from .table import FreeTable, Table, ValidationResult
from .user_tables import Computed, Imported, Lookup, Manual, Part
from .version import __version__

schema = Schema  # Alias for Schema

# =============================================================================
# Lazy imports — heavy dependencies loaded on first access
# =============================================================================
# These modules import heavy dependencies (networkx, matplotlib, click, pymysql)
# that slow down `import datajoint`. They are loaded on demand.

_lazy_modules = {
    # Diagram imports networkx and matplotlib
    "Diagram": (".diagram", "Diagram"),
    "Di": (".diagram", "Diagram"),
    "ERD": (".diagram", "Diagram"),
    "diagram": (".diagram", None),  # Return the module itself
    # kill imports pymysql via connection
    "kill": (".admin", "kill"),
    # cli imports click
    "cli": (".cli", "cli"),
}


def __getattr__(name: str):
    """Lazy import for heavy dependencies."""
    if name in _lazy_modules:
        module_path, attr_name = _lazy_modules[name]
        import importlib

        module = importlib.import_module(module_path, __package__)
        # If attr_name is None, return the module itself
        attr = module if attr_name is None else getattr(module, attr_name)
        # Cache in module __dict__ to avoid repeated __getattr__ calls
        # and to override the submodule that importlib adds automatically
        globals()[name] = attr
        return attr
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
