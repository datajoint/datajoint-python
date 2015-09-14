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
           'Connection', 'Heading', 'Relation', 'FreeRelation', 'Not', 'schema',
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

# ----------- loads local configuration from file ----------------
from .settings import Config, CONFIGVAR, LOCALCONFIG, logger, log_levels
config = Config()
local_config_file = os.environ.get(CONFIGVAR, None)
if local_config_file is None:
    local_config_file = LOCALCONFIG
local_config_file = os.path.expanduser(local_config_file)

try:
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(local_config_file))
    config.load(local_config_file)
except FileNotFoundError:
    logger.warn("Local config file {0:s} does not exist! Creating it.".format(local_config_file))
    config.save(local_config_file)

logger.setLevel(log_levels[config['loglevel']])


# ------------- flatten import hierarchy -------------------------
from .connection import conn, Connection
from .relation import Relation
from .user_relations import Manual, Lookup, Imported, Computed, Part
from .relational_operand import Not
from .heading import Heading
from .schema import Schema as schema
from .kill import kill
