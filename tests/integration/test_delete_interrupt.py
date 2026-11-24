"""
Ctrl-C (KeyboardInterrupt) or end of input (EOFError) during delete() rolls
back the transaction delete() opened and leaves the connection usable (#1691).
"""

import time
from unittest import mock

import pytest

import datajoint as dj
from datajoint.table import FreeTable


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema = dj.Schema(f"djtest_delete_interrupt_{backend}_{test_id}"[:64], connection=connection_by_backend)
    yield schema
    if connection_by_backend.in_transaction:
        connection_by_backend.cancel_transaction()
    schema.drop(prompt=False)


@pytest.fixture
def tables(schema_by_backend):
    schema = schema_by_backend

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    @schema
    class Session(dj.Manual):
        definition = """
        -> Subject
        session : int32
        """

    Subject.insert([{"subject_id": s} for s in (1, 2, 3)])
    Session.insert([{"subject_id": s, "session": n} for s in (1, 2, 3) for n in (1, 2)])
    return Subject, Session


def _assert_rolled_back_and_usable(Subject, Session):
    conn = Subject.connection
    assert not conn.in_transaction
    assert sorted(Subject.to_arrays("subject_id")) == [1, 2, 3]
    assert len(Session()) == 6
    # The connection is usable: a confirmed delete commits on it
    with mock.patch("builtins.input", return_value="yes"):
        assert (Subject & {"subject_id": 2}).delete(prompt=True) == 1
    assert not conn.in_transaction
    assert sorted(Subject.to_arrays("subject_id")) == [1, 3]
    assert len(Session()) == 4
    with conn.transaction:
        Subject.insert1({"subject_id": 4})
    assert sorted(Subject.to_arrays("subject_id")) == [1, 3, 4]


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, EOFError])
def test_interruption_at_prompt_rolls_back(tables, interruption):
    """Ctrl-C or end of input at "Commit deletes?" rolls the deletes back."""
    Subject, Session = tables
    with mock.patch("builtins.input", side_effect=interruption):
        with pytest.raises(interruption):
            (Subject & {"subject_id": 1}).delete(prompt=True)
    _assert_rolled_back_and_usable(Subject, Session)


@pytest.mark.parametrize("prompt", [True, False])
def test_interruption_while_deleting_rolls_back(tables, prompt):
    """Ctrl-C between the cascade's DELETE statements rolls back those already run."""
    Subject, Session = tables
    delete_quick = FreeTable.delete_quick
    calls = []

    def interrupt_second_delete(self, get_count=False):
        calls.append(self.full_table_name)
        if len(calls) == 2:
            raise KeyboardInterrupt
        return delete_quick(self, get_count=get_count)

    with mock.patch.object(FreeTable, "delete_quick", interrupt_second_delete):
        with pytest.raises(KeyboardInterrupt):
            (Subject & {"subject_id": 1}).delete(prompt=prompt)
    assert calls == [Session.full_table_name, Subject.full_table_name]  # Session's rows were deleted first
    _assert_rolled_back_and_usable(Subject, Session)


def _fail_once(method, error):
    """Wrap a bound method so that its first call raises ``error`` and later calls run it."""
    calls = []

    def wrapper(*args, **kwargs):
        calls.append(None)
        if len(calls) == 1:
            raise error
        return method(*args, **kwargs)

    return wrapper


@pytest.mark.parametrize("failure", [dj.errors.LostConnectionError("rollback failed"), KeyboardInterrupt()])
def test_failed_rollback_closes_connection_and_resets_transaction(tables, failure):
    """
    If the rollback fails or is itself interrupted, the connection is closed and no
    longer counts as being in a transaction: the next query reconnects and runs.
    """
    Subject, Session = tables
    conn = Subject.connection
    with (
        mock.patch.object(conn, "cancel_transaction", _fail_once(conn.cancel_transaction, failure)),
        mock.patch("builtins.input", side_effect=KeyboardInterrupt),
    ):
        with pytest.raises(KeyboardInterrupt):
            (Subject & {"subject_id": 1}).delete(prompt=True)
    assert not conn._in_transaction
    # Query before reading conn.in_transaction, which would reset a stale flag itself.
    # The server discarded the transaction when the connection closed.
    assert sorted(Subject.to_arrays("subject_id")) == [1, 2, 3]
    _assert_rolled_back_and_usable(Subject, Session)


def test_interrupted_commit_closes_connection_and_resets_transaction(tables):
    """Ctrl-C during the commit closes the connection and leaves it outside any transaction."""
    Subject, Session = tables
    conn = Subject.connection
    with (
        mock.patch.object(conn, "commit_transaction", side_effect=KeyboardInterrupt),
        mock.patch("builtins.input", return_value="yes"),
    ):
        with pytest.raises(KeyboardInterrupt):
            (Subject & {"subject_id": 1}).delete(prompt=True)
    assert not conn._in_transaction
    # COMMIT was not sent, so the server discarded the deletes when the connection closed
    assert sorted(Subject.to_arrays("subject_id")) == [1, 2, 3]
    _assert_rolled_back_and_usable(Subject, Session)
