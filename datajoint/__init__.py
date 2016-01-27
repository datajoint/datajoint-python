"""
DataJoint for Python is a high-level programming interface for MySQL databases
to support data processing chains in science labs. DataJoint is built on the
foundation of the relational data model and prescribes a consistent method for
organizing, populating, and querying data.

DataJoint is free software under the LGPL License. In addition, we request
that any use of DataJoint leading to a publication be acknowledged in the publication.
"""

import logging
import os

__author__ = "Dimitri Yatsenko, Edgar Walker, and Fabian Sinz at Baylor College of Medicine"
__version__ = "0.2"
__all__ = ['__author__', '__version__',
           'config', 'conn', 'kill',
           'Connection', 'Heading', 'BaseRelation', 'FreeRelation', 'Not', 'schema',
           'Manual', 'Lookup', 'Imported', 'Computed', 'Part']


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

class NoDefinitionError(DataJointError):
    """
    Error raised if table definition is not there.
    """
    pass

# ----------- loads local configuration from file ----------------
from .settings import Config, LOCALCONFIG, GLOBALCONFIG, logger, log_levels
config = Config()


if os.getenv('DJ_HOST') is not None and os.getenv('DJ_USER') is not None and os.getenv('DJ_PASS') is not None:
    print("Loading local settings from environment variables")
    config['database.host'] = os.getenv('DJ_HOST')
    config['database.user'] = os.getenv('DJ_USER')
    config['database.password'] = os.getenv('DJ_PASS')
elif os.path.exists(LOCALCONFIG):
    local_config_file = os.path.expanduser(LOCALCONFIG)
    print("Loading local settings from {0:s}".format(local_config_file))
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(local_config_file))
    config.load(local_config_file)
elif os.path.exists(os.path.expanduser('~/') + GLOBALCONFIG):
    local_config_file = os.path.expanduser('~/') + GLOBALCONFIG
    print("Loading local settings from {0:s}".format(local_config_file))
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(local_config_file))
    config.load(local_config_file)
else:
    print("""Cannot find configuration settings. Using default configuration. To change that, either
    * modify the local copy of %s that datajoint just saved for you
    * put a file named %s with the same configuration format in your home
    * specify the environment variables DJ_USER, DJ_HOST, DJ_PASS
          """)
    local_config_file = os.path.expanduser(LOCALCONFIG)
    logger.log(logging.INFO, "No config found. Generating {0:s}".format(local_config_file))
    config.save(local_config_file)

logger.setLevel(log_levels[config['loglevel']])


# ------------- flatten import hierarchy -------------------------
from .connection import conn, Connection
from .base_relation import BaseRelation
from .user_relations import Manual, Lookup, Imported, Computed, Part
from .relational_operand import Not
from .heading import Heading
from .schema import Schema as schema
from .kill import kill
