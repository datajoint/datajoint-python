"""Unit tests for the server compatibility warnings emitted at connect time."""

import warnings

import pytest

from datajoint.connection import MIN_MYSQL_VERSION, _warn_if_mariadb, _warn_if_old_mysql


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


@pytest.mark.parametrize(
    "version_str",
    [
        "8.0.13",
        "8.0.16",
        "8.0.17",
        "8.0.17-log",
        "5.7.44",
    ],
)
def test_warn_on_old_mysql(version_str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_if_old_mysql(version_str)
    assert len(caught) == 1
    assert issubclass(caught[0].category, UserWarning)
    message = str(caught[0].message)
    assert "requires MySQL 8.0.18 or later" in message
    assert version_str in message
    assert "(A - B)" in message
    for context in ("DELETE", "HAVING", "OR condition", "populate(reserve_jobs=True)", "jobs refresh"):
        assert context in message


@pytest.mark.parametrize(
    "version_str",
    [
        "8.0.18",
        "8.0.18-log",
        "8.0.21",
        "8.0.40",
        "8.0.40-0ubuntu0.22.04.1",
        "8.4.2-log",
        "8.4.0",
        "9.0.0",
        "9.1.0",
        "10.11.5-MariaDB",  # MariaDB has its own warning
        "5.5.68-MariaDB",
        "unknown",
    ],
)
def test_no_warn_on_current_mysql(version_str):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        _warn_if_old_mysql(version_str)
    assert caught == []


def test_min_mysql_version():
    assert MIN_MYSQL_VERSION == (8, 0, 18)
