"""
Collection of test cases to test connection module.
"""

import datajoint as dj
from datajoint import DataJointError
import numpy as np
import pytest


class Subjects(dj.Manual):
    definition = """
    #Basic subject
    subject_id                  : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """


@pytest.fixture
def schema_tx(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_transactions",
        context=dict(Subjects=Subjects),
        connection=connection_test,
    )
    schema(Subjects)
    yield schema
    schema.drop()


def test_dj_conn(db_creds_root):
    """
    Should be able to establish a connection as root user
    """
    c = dj.conn(**db_creds_root)
    assert c.is_connected


def test_dj_connection_class(connection_test):
    """
    Should be able to establish a connection as test user
    """
    assert connection_test.is_connected


def test_persistent_dj_conn(db_creds_root):
    """
    conn() method should provide persistent connection across calls.
    Setting reset=True should create a new persistent connection.
    """
    c1 = dj.conn(**db_creds_root)
    c2 = dj.conn()
    c3 = dj.conn(**db_creds_root)
    c4 = dj.conn(reset=True, **db_creds_root)
    c5 = dj.conn(**db_creds_root)
    assert c1 is c2
    assert c1 is c3
    assert c1 is not c4
    assert c4 is c5


def test_repr(db_creds_root):
    c1 = dj.conn(**db_creds_root)
    assert "disconnected" not in repr(c1) and "connected" in repr(c1)


def test_active(connection_test):
    with connection_test.transaction as conn:
        assert conn.in_transaction, "Transaction is not active"


def test_transaction_rollback(schema_tx, connection_test):
    """Test transaction cancellation using a with statement"""
    tmp = np.array(
        [(1, "Peter", "mouse"), (2, "Klara", "monkey")],
        Subjects.heading.as_dtype,
    )

    Subjects.delete()
    with connection_test.transaction:
        Subjects.insert1(tmp[0])
    try:
        with connection_test.transaction:
            Subjects.insert1(tmp[1])
            raise DataJointError("Testing rollback")
    except DataJointError:
        pass
    assert (
        len(Subjects()) == 1
    ), "Length is not 1. Expected because rollback should have happened."

    assert (
        len(Subjects & "subject_id = 2") == 0
    ), "Length is not 0. Expected because rollback should have happened."


def test_cancel(schema_tx, connection_test):
    """Tests cancelling a transaction explicitly"""
    tmp = np.array(
        [(1, "Peter", "mouse"), (2, "Klara", "monkey")],
        Subjects().heading.as_dtype,
    )
    Subjects().delete_quick()
    Subjects.insert1(tmp[0])
    connection_test.start_transaction()
    Subjects.insert1(tmp[1])
    connection_test.cancel_transaction()
    assert (
        len(Subjects()) == 1
    ), "Length is not 1. Expected because rollback should have happened."
    assert (
        len(Subjects & "subject_id = 2") == 0
    ), "Length is not 0. Expected because rollback should have happened."
