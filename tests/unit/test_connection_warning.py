"""Unit tests for the MariaDB compatibility warning emitted at connect time."""

import warnings

import pytest

from datajoint.connection import _warn_if_mariadb


@pytest.mark.parametrize(
    "version_str",
    [
        "10.11.5-MariaDB",
        "10.5.5-MariaDB-1~bionic",
        "5.5.68-MariaDB",
    ],
)
def test_warn_on_mariadb(version_str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_if_mariadb(version_str)
    assert len(caught) == 1
    assert issubclass(caught[0].category, UserWarning)
    assert "MariaDB is not officially supported" in str(caught[0].message)
    assert version_str in str(caught[0].message)


@pytest.mark.parametrize(
    "version_str",
    [
        "8.0.40",
        "8.0.13",
        "8.0.40-0ubuntu0.22.04.1",
        "8.4.2-log",
        "9.0.0",
    ],
)
def test_no_warn_on_mysql(version_str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_if_mariadb(version_str)
    assert caught == []
