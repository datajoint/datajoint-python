"""
Collection of test cases for base module. Tests functionalities such as
creating tables using docstring table declarations
"""
__author__ = 'eywalker'

from . import BASE_CONN, CONN_INFO, cleanup
import datajoint as dj #TODO: probably make this a relative import"
from ..base import Base
from .schemas import test1


def setup():
    """
    Setup connections and bindings
    """
    pass

class TestBaseObject(object):
    """
    Test cases for Base relational object mapped to a table.
    """
