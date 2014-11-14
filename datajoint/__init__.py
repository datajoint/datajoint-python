from .connection import conn, Connection
from .core import DataJointError
from .base import Base
from .task import TaskQueue
from .autopopulate import AutoPopulate
from . import blob
from .relational import Not

__author__ = "Dimitri Yatsenko and Edgar Walker at Baylor College of Medicine"
__version__ = "0.2"

__all__ = ['__author__','__version__',
    'Connection', 'Heading', 'Base', 'Not',
    'AutoPopulate', 'TaskQueue', 'conn', 'DataJointError','blob']
