import logging
import os

__author__ = "Dimitri Yatsenko, Edgar Walker, and Fabian Sinz at Baylor College of Medicine"
__version__ = "0.2"
__all__ = ['__author__', '__version__',
           'Connection', 'Heading', 'Relation', 'FreeRelation', 'Not',
           'Relation',
           'Manual', 'Lookup', 'Imported', 'Computed',
           'AutoPopulate', 'conn', 'DataJointError', 'blob']


class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal operation.
    """
    pass


class TransactionError(DataJointError):
    """
    Base class for errors specific to DataJoint internal operation.
    """
    def __init__(self, msg, f, args=None, kwargs=None):
        super(TransactionError, self).__init__(msg)
        self.operations = (f, args if args is not None else tuple(),
                           kwargs if kwargs is not None else {})

    def resolve(self):
        f, args, kwargs = self.operations
        return f(*args, **kwargs)

    @property
    def culprit(self):
        return self.operations[0].__name__


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
from .user_relations import Manual, Lookup, Imported, Computed
from .abstract_relation import Relation
from .autopopulate import AutoPopulate
from . import blob
from .relational_operand import Not
from .heading import Heading