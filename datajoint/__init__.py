"""
DataJoint for Python is a framework for building data pipelines using MySQL databases
to represent pipeline structure and bulk storage systems for large objects.
DataJoint is built on the foundation of the relational data model and prescribes a
consistent method for organizing, populating, and querying data.

The DataJoint data model is described in https://arxiv.org/abs/1807.11104

DataJoint is free software under the LGPL License. In addition, we request
that any use of DataJoint leading to a publication be acknowledged in the publication.

Please cite:

  - http://biorxiv.org/content/early/2015/11/14/031658
  - http://dx.doi.org/10.1101/031658
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
    "create_virtual_module",
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
    "set_password",
    "kill",
    "MatCell",
    "MatStruct",
    "AttributeAdapter",
    "errors",
    "DataJointError",
    "key",
    "key_hash",
    "logger",
    "cli",
]
import importlib
from typing import TYPE_CHECKING

from . import errors  
from .errors import DataJointError
from .logging import logger  
from .settings import config  
from .version import __version__  

from .connection import Connection, conn  

if TYPE_CHECKING:
    from .admin import kill, set_password
    from .attribute_adapter import AttributeAdapter
    from .blob import MatCell, MatStruct
    from .cli import cli
    from .diagram import Diagram
    from .expression import AndList, Not, Top, U
    from .fetch import key
    from .hash import key_hash
    from .schemas import Schema, VirtualModule, list_schemas
    from .table import FreeTable, Table
    from .user_tables import Computed, Imported, Lookup, Manual, Part


_LAZY: dict[str, tuple[str, str]] = {
    # admin
    "kill": ("datajoint.admin", "kill"),
    "set_password": ("datajoint.admin", "set_password"),

    # core objects
    "Schema": ("datajoint.schemas", "Schema"),
    "VirtualModule": ("datajoint.schemas", "VirtualModule"),
    "list_schemas": ("datajoint.schemas", "list_schemas"),

    # tables
    "Table": ("datajoint.table", "Table"),
    "FreeTable": ("datajoint.table", "FreeTable"),
    "Manual": ("datajoint.user_tables", "Manual"),
    "Lookup": ("datajoint.user_tables", "Lookup"),
    "Imported": ("datajoint.user_tables", "Imported"),
    "Computed": ("datajoint.user_tables", "Computed"),
    "Part": ("datajoint.user_tables", "Part"),

    # diagram
    "Diagram": ("datajoint.diagram", "Diagram"),

    # expressions
    "Not": ("datajoint.expression", "Not"),
    "AndList": ("datajoint.expression", "AndList"),
    "Top": ("datajoint.expression", "Top"),
    "U": ("datajoint.expression", "U"),

    # misc utilities
    "MatCell": ("datajoint.blob", "MatCell"),
    "MatStruct": ("datajoint.blob", "MatStruct"),
    "AttributeAdapter": ("datajoint.attribute_adapter", "AttributeAdapter"),
    "key": ("datajoint.fetch", "key"),
    "key_hash": ("datajoint.hash", "key_hash"),
    "cli": ("datajoint.cli", "cli"),
}
_ALIAS: dict[str, str] = {
    "ERD": "Diagram",
    "Di": "Diagram",
    "schema": "Schema",
    "create_virtual_module": "VirtualModule",
}


def __getattr__(name: str):
    if name in _ALIAS:
        target = _ALIAS[name]
        value = getattr(importlib.import_module(_LAZY[target][0]), _LAZY[target][1])
        globals()[target] = value
        globals()[name] = value
        return value

    if name in _LAZY:
        module_name, attr = _LAZY[name]
        module = importlib.import_module(module_name)
        value = getattr(module, attr)
        globals()[name] = value  # cache
        return value

    raise AttributeError(f"module 'datajoint' has no attribute {name}")