import logging
import os

__author__ = "Dimitri Yatsenko, Edgar Walker, and Fabian Sinz   at Baylor College of Medicine"
__version__ = "0.2"

class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal
    operation.
    """
    pass


from .settings import Config, logger

# ----------- loads local configuration from file ----------------
config = Config()
local_config_file = os.path.expanduser(os.environ.get(config['settings']['local config var'], None))
if local_config_file is None:
    local_config_file = os.path.expanduser(config['settings']['local config file'])

try:
    logger.log(logging.INFO, "Loading local settings from {0:s}".format(local_config_file))
    config.load(local_config_file)
except:
    logger.warn("Local config file {0:s} does not exist! Creating it.".format(local_config_file))
    config.save(local_config_file)



from .connection import conn, Connection
from .base import Base
from .task import TaskQueue
from .autopopulate import AutoPopulate
from . import blob
from .relational import Not



__all__ = ['__author__', '__version__',
           'Connection', 'Heading', 'Base', 'Not',
           'AutoPopulate', 'TaskQueue', 'conn', 'DataJointError', 'blob']
