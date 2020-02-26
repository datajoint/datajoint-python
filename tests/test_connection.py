"""
Collection of test cases to test connection module.
"""

from nose.tools import assert_true, assert_equal
import datajoint as dj
import numpy as np
from datajoint import DataJointError
from . import CONN_INFO, PREFIX


def test_dj_conn():
    """
    Should be able to establish a connection
    """
    c = dj.conn(**CONN_INFO)
    assert_true(c.is_connected)


def test_persistent_dj_conn():
    """
    conn() method should provide persistent connection across calls.
    Setting reset=True should create a new persistent connection.
    """
    c1 = dj.conn(**CONN_INFO)
    c2 = dj.conn()
    c3 = dj.conn(**CONN_INFO)
    c4 = dj.conn(reset=True, **CONN_INFO)
    c5 = dj.conn(**CONN_INFO)
    assert_true(c1 is c2)
    assert_true(c1 is c3)
    assert_true(c1 is not c4)
    assert_true(c4 is c5)


def test_repr():
    c1 = dj.conn(**CONN_INFO)
    assert_true('disconnected' not in repr(c1) and 'connected' in repr(c1))


class TestTransactions:
    """
    test transaction management
    """

    schema = dj.Schema(PREFIX + '_transactions', locals(), connection=dj.conn(**CONN_INFO))

    @schema
    class Subjects(dj.Manual):
        definition = """
        #Basic subject
        subject_id                  : int      # unique subject id
        ---
        real_id                     :  varchar(40)    #  real-world name
        species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
        """

    @classmethod
    def setup_class(cls):
        cls.relation = cls.Subjects()
        cls.conn = dj.conn(**CONN_INFO)

    def teardown(self):
        self.relation.delete_quick()

    def test_active(self):
        with self.conn.transaction as conn:
            assert_true(conn.in_transaction, "Transaction is not active")

    def test_transaction_rollback(self):
        """Test transaction cancellation using a with statement"""
        tmp = np.array([
            (1, 'Peter', 'mouse'),
            (2, 'Klara', 'monkey')
        ],  self.relation.heading.as_dtype)

        self.relation.delete()
        with self.conn.transaction:
            self.relation.insert1(tmp[0])
        try:
            with self.conn.transaction:
                self.relation.insert1(tmp[1])
                raise DataJointError("Testing rollback")
        except DataJointError:
            pass
        assert_equal(len(self.relation), 1,
                     "Length is not 1. Expected because rollback should have happened.")
        assert_equal(len(self.relation & 'subject_id = 2'), 0,
                     "Length is not 0. Expected because rollback should have happened.")

    def test_cancel(self):
        """Tests cancelling a transaction explicitly"""
        tmp = np.array([
            (1, 'Peter', 'mouse'),
            (2, 'Klara', 'monkey')
        ],  self.relation.heading.as_dtype)
        self.relation.delete_quick()
        self.relation.insert1(tmp[0])
        self.conn.start_transaction()
        self.relation.insert1(tmp[1])
        self.conn.cancel_transaction()
        assert_equal(len(self.relation), 1,
                     "Length is not 1. Expected because rollback should have happened.")
        assert_equal(len(self.relation & 'subject_id = 2'), 0,
                     "Length is not 0. Expected because rollback should have happened.")
