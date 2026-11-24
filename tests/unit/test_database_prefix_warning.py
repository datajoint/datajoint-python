"""The database_prefix deprecation warning names no release that has already shipped (#1675)."""

import warnings
from unittest import mock

import pytest

import datajoint as dj


class _Stop(Exception):
    """Raised by the stub connection once activate() is past the deprecation check."""


def _activate(prefix):
    """Run Schema.activate() up to its first query, with ``database_prefix`` set to ``prefix``."""
    connection = mock.MagicMock()
    connection._config = {"database.database_prefix": prefix}
    connection.query.side_effect = _Stop
    connection.adapter.schema_exists_sql.side_effect = _Stop
    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        with pytest.raises(_Stop):
            dj.Schema("prefix_check", connection=connection)
    return [str(w.message) for w in record if issubclass(w.category, DeprecationWarning)]


def test_warning_says_future_release():
    messages = [m for m in _activate("myproject_") if "database_prefix" in m]
    assert len(messages) == 1
    assert "will be removed in a future release" in messages[0]
    assert "2.3" not in messages[0]


def test_no_warning_when_unset():
    assert not [m for m in _activate("") if "database_prefix" in m]
