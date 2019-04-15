"""
DataJoint for Python is a framework for building data piplines using MySQL databases
to represent pipeline structure and bulk storage systems for large objects.
DataJoint is built on the foundation of the relational data model and prescribes a
consistent method for organizing, populating, and querying data.

The DataJoint data model is described in https://arxiv.org/abs/1807.11104

DataJoint is free software under the LGPL License. In addition, we request
that any use of DataJoint leading to a publication be acknowledged in the publication.

Please cite:
    http://biorxiv.org/content/early/2015/11/14/031658
    http://dx.doi.org/10.1101/031658
"""

__author__ = "DataJoint Contributors"
__date__ = "February 7, 2019"
__all__ = ['__author__', '__version__',
           'config', 'conn', 'Connection',
           'schema', 'create_virtual_module', 'list_schemas',
           'Table', 'FreeTable',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part',
           'Not', 'AndList', 'U', 'Diagram', 'Di', 'ERD',
           'set_password', 'kill',
           'MatCell', 'MatStruct',
           'DataJointError', 'DuplicateError', 'key']


from .version import __version__
from .settings import config
from .connection import conn, Connection
from .schema import Schema as schema
from .schema import create_virtual_module, list_schemas
from .table import Table, FreeTable
from .user_tables import Manual, Lookup, Imported, Computed, Part
from .expression import Not, AndList, U
from .diagram import Diagram
from .admin import set_password, kill
from .blob import MatCell, MatStruct
from .errors import DataJointError, DuplicateError
from .fetch import key

ERD = Di = Diagram   # Aliases for Diagram
