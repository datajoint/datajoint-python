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
]

from .logging import logger
from .version import __version__
from .settings import config
from .connection import conn, Connection
from .schemas import Schema
from .schemas import VirtualModule, list_schemas
from .table import Table, FreeTable
from .user_tables import Manual, Lookup, Imported, Computed, Part
from .expression import Not, AndList, U, Top
from .diagram import Diagram
from .admin import set_password, kill
from .blob import MatCell, MatStruct
from .fetch import key
from .hash import key_hash
from .attribute_adapter import AttributeAdapter
from . import errors
from .errors import DataJointError

ERD = Di = Diagram  # Aliases for Diagram
schema = Schema  # Aliases for Schema
create_virtual_module = VirtualModule  # Aliases for VirtualModule
