"""How ``database.name`` maps schema names on each backend, without a server."""

import pytest

import datajoint as dj
from datajoint.adapters import get_adapter


def _connection(backend, database_name):
    conn = dj.Connection.__new__(dj.Connection)
    conn.conn_info = {"database_name": database_name}
    conn.adapter = get_adapter(backend)
    return conn


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_no_namespace_keeps_names(backend):
    conn = _connection(backend, None)
    assert conn.schema_prefix == ""
    assert conn.qualify_schema_name("subject") == "subject"


def test_mysql_namespace_prefixes_names():
    conn = _connection("mysql", "lab_project")
    assert conn.schema_prefix == "lab_project_"
    assert conn.qualify_schema_name("subject") == "lab_project_subject"


def test_postgresql_namespace_is_the_database():
    conn = _connection("postgresql", "lab_project")
    assert conn.schema_prefix == ""
    assert conn.qualify_schema_name("subject") == "subject"


def test_mysql_prefixed_name_is_not_prefixed_twice():
    conn = _connection("mysql", "lab_project")
    with pytest.warns(UserWarning, match="write `subject` instead"):
        assert conn.qualify_schema_name("lab_project_subject") == "lab_project_subject"
