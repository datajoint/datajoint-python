"""
Collection of test cases to test connection module.
"""
from tests.schemata.test1 import Subjects

__author__ = 'eywalker, fabee'
from . import CONN_INFO, PREFIX, BASE_CONN, cleanup
from nose.tools import assert_true, assert_raises, assert_equal, raises
import datajoint as dj
from datajoint import DataJointError
import numpy as np

def setup():
    cleanup()


def test_dj_conn():
    """
    Should be able to establish a connection
    """
    c = dj.conn(**CONN_INFO)
    assert c.is_connected


def test_persistent_dj_conn():
    """
    conn() method should provide persistent connection
    across calls.
    """
    c1 = dj.conn(**CONN_INFO)
    c2 = dj.conn()
    assert_true(c1 is c2)


def test_dj_conn_reset():
    """
    Passing in reset=True should allow for new persistent
    connection to be created.
    """
    c1 = dj.conn(**CONN_INFO)
    c2 = dj.conn(reset=True, **CONN_INFO)
    assert_true(c1 is not c2)


def test_repr():
    c1 = dj.conn(**CONN_INFO)
    assert_true('disconnected' not in c1.__repr__() and 'connected' in c1.__repr__())

def test_del():
    c1 = dj.conn(**CONN_INFO)
    assert_true('disconnected' not in c1.__repr__() and 'connected' in c1.__repr__())
    del c1



class TestContextManager(object):
    def __init__(self):
        self.relvar = None
        self.setup()

    """
    Test cases for FreeRelation objects
    """

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        cleanup()  # drop all databases with PREFIX
        self.conn = dj.conn()
        self.relvar = Subjects()

    def teardown(self):
        cleanup()

    def test_active(self):
        with self.conn.transaction() as conn:
            assert_true(conn.in_transaction, "Transaction is not active")

    def test_rollback(self):

        tmp = np.array([(1,'Peter','mouse'),(2, 'Klara', 'monkey')],
                       dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])

        self.relvar.insert(tmp[0])
        try:
            with self.conn.transaction():
                self.relvar.insert(tmp[1])
                raise DataJointError("Just to test")
        except DataJointError as e:
            pass
        testt2 = (self.relvar & 'subject_id = 2').fetch()
        assert_equal(len(testt2), 0, "Length is not 0. Expected because rollback should have happened.")

    def test_cancel(self):
        """Tests cancelling a transaction"""
        tmp = np.array([(1,'Peter','mouse'),(2, 'Klara', 'monkey')],
                       dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])

        self.relvar.insert(tmp[0])
        with self.conn.transaction() as conn:
            self.relvar.insert(tmp[1])
            conn.cancel_transaction()

        testt2 = (self.relvar & 'subject_id = 2').fetch()
        assert_equal(len(testt2), 0, "Length is not 0. Expected because rollback should have happened.")



