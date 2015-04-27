__author__ = "Dimitri Yatsenko and Edgar Walker at Baylor College of Medicine"
__version__ = "0.2"

class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal
    operation.
    """
    pass

from .connection import conn, Connection
from .base import Base
from .task import TaskQueue
from .autopopulate import AutoPopulate
from . import blob
from .relational import Not





__all__ = ['__author__', '__version__',
           'Connection', 'Heading', 'Base', 'Not',
           'AutoPopulate', 'TaskQueue', 'conn', 'DataJointError', 'blob']
