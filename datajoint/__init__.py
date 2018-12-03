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

__author__ = "Dimitri Yatsenko, Edgar Y. Walker, and Fabian Sinz at Baylor College of Medicine"
__date__ = "Nov 15, 2018"
__all__ = ['__author__', '__version__',
           'config', 'conn', 'kill', 'Table',
           'Connection', 'Heading', 'FreeTable', 'Not', 'schema',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part',
           'AndList', 'ERD', 'U', 'key',
           'DataJointError', 'DuplicateError',
           'set_password', 'create_virtual_module']


# ------------- flatten import hierarchy -------------------------
from .version import __version__
from .settings import config
from .connection import conn, Connection
from .table import FreeTable, Table
from .user_tables import Manual, Lookup, Imported, Computed, Part
from .expression import Not, AndList, U
from .heading import Heading
from .schema import Schema as schema
from .schema import create_virtual_module
from .erd import ERD
from .admin import set_password, kill
from .errors import DataJointError, DuplicateError
from .fetch import key


