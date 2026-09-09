"""Unit tests for the DDL schema-placeholder substitution in Table.declare."""

from datajoint.table import _substitute_database


def test_placeholder_fragment_substituted():
    """The exact adapter-inserted fragment (see adapters/postgres.py enum
    qualification) is replaced with the quoted schema name."""
    assert _substitute_database('"{database}".enum_abc NOT NULL', "myschema") == '"myschema".enum_abc NOT NULL'


def test_user_braces_pass_through():
    """Brace text outside the adapter fragment — including a bare literal
    {database} in a comment — is never touched."""
    ddl = '`payload` varchar(32) COMMENT "{data, config} payload for {database}"'
    assert _substitute_database(ddl, "myschema") == ddl
