import logging
import os

__author__ = "Dimitri Yatsenko, Edgar Walker, and Fabian Sinz   at Baylor College of Medicine"
__version__ = "0.2"
__all__ = ['__author__', '__version__',
           'Connection', 'Heading', 'Base', 'Not',
           'AutoPopulate', 'TaskQueue', 'conn', 'DataJointError', 'blob']

# ------------ define datajoint error before the import hierarchy is flattened ------------
class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal
    operation.
    """
    pass



# ----------- loads local configuration from file ----------------
from .settings import Config, logger
config = Config()
local_config_file = os.environ.get(config['config.varname'], None)
if local_config_file is None:
    local_config_file = os.path.expanduser(config['config.file'])
else:
    local_config_file = os.path.expanduser(local_config_file)
    config['config.file'] = local_config_file
try:
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(local_config_file))
    config.load(local_config_file)
except FileNotFoundError:
    logger.warn("Local config file {0:s} does not exist! Creating it.".format(local_config_file))
    config.save(local_config_file)


# ------------- flatten import hierarchy -------------------------
from .connection import conn, Connection
from .base import Base
from .task import TaskQueue
from .autopopulate import AutoPopulate
from . import blob
from .relational import Not



