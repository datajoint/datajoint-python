"""
Collection of test cases to test relational methods
"""
from test.schemata.schema1 import test1

__author__ = 'eywalker'
from . import (CONN_INFO, PREFIX, BASE_CONN, cleanup)
from nose.tools import assert_true, assert_raises, assert_equal
import datajoint as dj
from datajoint.core import DataJointError

def setup():
    """
    Setup
    :return:
    """

class TestRelationalAlgebra(object):

    def setup(self):
        pass

    def test_mul(self):
        pass

    def test_pro(self):
        pass

    def test_iand(self):
        pass

    def test_isub(self):
        pass

    def test_sub(self):
        pass

    def test_count(self):
        pass

    def test_fetch(self):
        pass

    def test_repr(self):
        pass

    def test_iter(self):
        pass

    def test_not(self):
        pass