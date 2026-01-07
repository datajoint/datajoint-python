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

from . import errors
from . import migrate
from .admin import kill
from .codecs import (
    Codec,
    get_codec,
    list_codecs,
)
from .blob import MatCell, MatStruct
from .cli import cli
from .connection import Connection, conn
from .diagram import Diagram
from .errors import DataJointError
from .expression import AndList, Not, Top, U
from .hash import key_hash
from .logging import logger
from .objectref import ObjectRef
from .schemas import Schema, VirtualModule, list_schemas
from .settings import config
from .table import FreeTable, Table, ValidationResult
from .user_tables import Computed, Imported, Lookup, Manual, Part
from .version import __version__

ERD = Di = Diagram  # Aliases for Diagram
schema = Schema  # Aliases for Schema
create_virtual_module = VirtualModule  # Aliases for VirtualModule
