"""
Unit tests: delete() rolls back the transaction it opened when the prompt or
the cascade is interrupted (#1691). The connection and the cascade are mocks.
"""

from unittest import mock

import pytest

from datajoint.adapters import MySQLAdapter
from datajoint.table import FreeTable


def _table(delete_side_effect=None):
    """A FreeTable on a mock connection whose cascade is the table alone."""
    conn = mock.MagicMock()
    conn.adapter = MySQLAdapter()
    conn._config = {"safemode": True}
    conn.in_transaction = False
    table = FreeTable(conn, "`s`.`subject`")
    step = mock.MagicMock(full_table_name=table.full_table_name)
    step.delete_quick.side_effect = delete_side_effect
    step.delete_quick.return_value = 1
    return table, conn, _Cascade([step])


class _Cascade(list):
    """Stands in for the Diagram that Diagram.cascade() returns: iterable both ways."""


def _delete(table, cascade, **kwargs):
    with mock.patch("datajoint.diagram.Diagram.cascade", return_value=cascade):
        return table.delete(**kwargs)


@pytest.mark.parametrize("interruption", [KeyboardInterrupt, EOFError])
def test_interruption_at_prompt_rolls_back(interruption):
    table, conn, cascade = _table()
    with mock.patch("builtins.input", side_effect=interruption):
        with pytest.raises(interruption):
            _delete(table, cascade, prompt=True)
    conn.start_transaction.assert_called_once()
    conn.cancel_transaction.assert_called_once()
    conn.commit_transaction.assert_not_called()


@pytest.mark.parametrize("prompt", [True, False])
def test_interruption_while_deleting_rolls_back(prompt):
    table, conn, cascade = _table(delete_side_effect=KeyboardInterrupt)
    with pytest.raises(KeyboardInterrupt):
        _delete(table, cascade, prompt=prompt)
    conn.cancel_transaction.assert_called_once()
    conn.commit_transaction.assert_not_called()


def test_failed_rollback_closes_connection_and_keeps_the_interruption():
    """If the rollback fails, the connection is closed and Ctrl-C still reaches the caller."""
    table, conn, cascade = _table(delete_side_effect=KeyboardInterrupt)
    conn.cancel_transaction.side_effect = RuntimeError("connection out of sync")
    with pytest.raises(KeyboardInterrupt):
        _delete(table, cascade, prompt=False)
    conn.close.assert_called_once()


def test_interruption_inside_enclosing_transaction_is_left_to_it():
    """With transaction=False, delete() opened no transaction and does not roll back."""
    table, conn, cascade = _table(delete_side_effect=KeyboardInterrupt)
    with pytest.raises(KeyboardInterrupt):
        _delete(table, cascade, prompt=False, transaction=False)
    conn.start_transaction.assert_not_called()
    conn.cancel_transaction.assert_not_called()


def test_confirmed_delete_commits():
    table, conn, cascade = _table()
    with mock.patch("builtins.input", return_value="yes"):
        assert _delete(table, cascade, prompt=True) == 1
    conn.commit_transaction.assert_called_once()
    conn.cancel_transaction.assert_not_called()
