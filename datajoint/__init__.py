"""
DataJoint for Python is a high-level programming interface for MySQL databases
to support data processing chains in science labs. DataJoint is built on the
foundation of the relational data model and prescribes a consistent method for
organizing, populating, and querying data.

DataJoint is free software under the LGPL License. In addition, we request
that any use of DataJoint leading to a publication be acknowledged in the publication.

Please cite:
    http://biorxiv.org/content/early/2015/11/14/031658
    http://dx.doi.org/10.1101/031658
"""

import logging
import os
from types import ModuleType
from .version import __version__

__author__ = "Dimitri Yatsenko, Edgar Y. Walker, and Fabian Sinz at Baylor College of Medicine"
__date__ = "June 1, 2017"
__all__ = ['__author__', '__version__',
           'config', 'conn', 'kill', 'BaseRelation',
           'Connection', 'Heading', 'FreeRelation', 'Not', 'schema',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part',
           'AndList', 'OrList', 'ERD', 'U',
           'set_password']

print('DataJoint', __version__, '('+__date__+')')


class key:
    """
    object that allows requesting the primary key in Fetch.__getitem__
    """
    pass


class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal operation.
    """
    pass

# ----------- loads local configuration from file ----------------
from .settings import Config, LOCALCONFIG, GLOBALCONFIG, logger, log_levels
config = Config()
config_files = (os.path.expanduser(n) for n in (LOCALCONFIG, os.path.join('~', GLOBALCONFIG)))
try:
    config_file = next(n for n in config_files if os.path.exists(n))
except StopIteration:
    print('No config file found, using default settings.')
    config_file = None
else:
    print("Loading settings from {0:s}".format(config_file))
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(config_file))
    config.load(config_file)

# override login credentials with environment variables
mapping = {k: v for k, v in zip(
    ('database.host', 'database.user', 'database.password'),
    map(os.getenv, ('DJ_HOST', 'DJ_USER', 'DJ_PASS')))
           if v is not None}
config.update(mapping)

logger.setLevel(log_levels[config['loglevel']])

# ------------- flatten import hierarchy -------------------------
from .connection import conn, Connection
from .base_relation import FreeRelation, BaseRelation
from .user_relations import Manual, Lookup, Imported, Computed, Part
from .relational_operand import Not, AndList, OrList, U
from .heading import Heading
from .schema import Schema as schema
from .erd import ERD
from .admin import set_password, kill


def create_virtual_module(modulename, dbname):
    """
    Creates a python module with the given name from a database name in mysql with datajoint tables.
    Automatically creates the classes of the appropriate tier in the module.

    :param modulename: desired name of the module
    :param dbname:     name of the database in mysql
    :return: the python module
    """
    mod = ModuleType(modulename)
    s = schema(dbname, mod.__dict__)
    s.spawn_missing_classes()
    mod.__dict__['schema'] = s
    return mod
