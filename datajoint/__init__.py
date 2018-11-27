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

import os
from .version import __version__

__author__ = "Dimitri Yatsenko, Edgar Y. Walker, and Fabian Sinz at Baylor College of Medicine"
__date__ = "Nov 15, 2018"
__all__ = ['__author__', '__version__',
           'config', 'conn', 'kill', 'Table',
           'Connection', 'Heading', 'FreeTable', 'Not', 'schema',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part',
           'AndList', 'ERD', 'U',
           'DataJointError', 'DuplicateError',
           'set_password']


class key:
    """
    object that allows requesting the primary key in Fetch.__getitem__
    """
    pass


# ----------- loads local configuration from file ----------------
from .settings import Config, LOCALCONFIG, GLOBALCONFIG, logger, log_levels
config = Config()
config_files = (os.path.expanduser(n) for n in (LOCALCONFIG, os.path.join('~', GLOBALCONFIG)))
try:
    config_file = next(n for n in config_files if os.path.exists(n))
except StopIteration:
    config.add_history('No config file found, using default settings.')
else:
    config.load(config_file)
    del config_file

del config_files

# override login credentials with environment variables
mapping = {k: v for k, v in zip(
    ('database.host', 'database.user', 'database.password',
     'external.aws_access_key_id', 'external.aws_secret_access_key',),
    map(os.getenv, ('DJ_HOST', 'DJ_USER', 'DJ_PASS',
                    'DJ_AWS_ACCESS_KEY_ID', 'DJ_AWS_SECRET_ACCESS_KEY',)))
           if v is not None}
for k in mapping:
    config.add_history('Updated login credentials from %s' % k)
config.update(mapping)
del mapping 

logger.setLevel(log_levels[config['loglevel']])

# ------------- flatten import hierarchy -------------------------
from .connection import conn, Connection
from .table import FreeTable, Table
from .user_tables import Manual, Lookup, Imported, Computed, Part
from .expression import Not, AndList, U
from .heading import Heading
from .schema import Schema as schema
from .erd import ERD
from .admin import set_password, kill
from .errors import DataJointError, DuplicateError


def create_virtual_module(module_name, schema_name, create_schema=False, create_tables=False, connection=None):
    """
    Creates a python module with the given name from the name of a schema on the server and
    automatically adds classes to it corresponding to the tables in the schema.

    :param module_name: displayed module name
    :param schema_name: name of the database in mysql
    :param create_schema: if True, create the schema on the database server
    :param create_tables: if True, module.schema can be used as the decorator for declaring new
    :return: the python module containing classes from the schema object and the table classes
    """
    import types
    module = types.ModuleType(module_name)
    _schema = schema(schema_name, create_schema=create_schema, create_tables=create_tables, connection=connection)
    _schema.spawn_missing_classes(context=module.__dict__)
    module.__dict__['schema'] = _schema
    return module
