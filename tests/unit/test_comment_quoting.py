"""
Comments are written as string literals that hold exactly the declared text.

``read_literal`` reads a string literal the way the server does. It checks that
the literal written for a comment ends where the statement ends and reads back as
the comment. MySQL's NO_BACKSLASH_ESCAPES and PostgreSQL's
standard_conforming_strings change how a backslash is read, so both settings are
checked.
"""

import pytest

from datajoint.adapters import get_adapter
from datajoint.declare import compile_attribute
from datajoint.heading import Attribute, default_attribute_properties

COMMENTS = [
    'say "hi"',
    "it's a test",
    "back\\slash",
    "C:\\new\\table\\0",
    "a \\\" b \\' c",
    "a '' b \"\" c",
    'x\\" , injected int NOT NULL COMMENT "',
    "x\\'; SELECT 1; --",
    "ends with a backslash \\",
    "\\",
    '"',
    "'",
]

# Backslash sequences that MySQL and PostgreSQL E'' literals read as one character
ESCAPES = {"0": "\0", "b": "\b", "n": "\n", "r": "\r", "t": "\t", "Z": "\x1a"}


def read_literal(sql, *, backslash_escapes):
    """
    Read the string literal at the start of ``sql``.

    A quote character written twice stands for one. With ``backslash_escapes``,
    a backslash and the character after it stand for one character.

    Returns
    -------
    tuple
        The text of the literal and the SQL that follows it.
    """
    quote = sql[0]
    text, i = [], 1
    while i < len(sql):
        char = sql[i]
        if char == "\\" and backslash_escapes and i + 1 < len(sql):
            text.append(ESCAPES.get(sql[i + 1], sql[i + 1]))
            i += 2
        elif char == quote and sql[i + 1 : i + 2] == quote:
            text.append(quote)
            i += 2
        elif char == quote:
            return "".join(text), sql[i + 1 :]
        else:
            text.append(char)
            i += 1
    raise AssertionError(f"The literal is not closed: {sql}")


@pytest.fixture
def mysql():
    return get_adapter("mysql")


@pytest.fixture
def postgres():
    pytest.importorskip("psycopg2")
    return get_adapter("postgresql")


def read_mysql(sql, prefix, backslash_escapes=True):
    assert sql.startswith(prefix)
    text, rest = read_literal(sql[len(prefix) :], backslash_escapes=backslash_escapes)
    assert rest == "", f"The comment literal ended early: {sql}"
    return text


def read_postgres(sql, prefix, standard_conforming_strings=True):
    assert sql.startswith(prefix)
    literal = sql[len(prefix) :]
    # An E'' literal reads backslash escapes under either setting
    escapes = literal.startswith("E'") or not standard_conforming_strings
    text, rest = read_literal(literal.removeprefix("E"), backslash_escapes=escapes)
    assert rest == "", f"The comment literal ended early: {sql}"
    return text


@pytest.mark.parametrize("comment", COMMENTS)
def test_mysql_column_comment(mysql, comment):
    sql = mysql.format_column_definition("note", "varchar(16)", comment=comment)
    prefix = "`note` varchar(16) NOT NULL COMMENT "
    assert read_mysql(sql, prefix) == comment
    text = read_mysql(sql, prefix, backslash_escapes=False)
    if "\\" not in comment:
        assert text == comment


@pytest.mark.parametrize("comment", COMMENTS)
def test_mysql_table_comment(mysql, comment):
    sql = mysql.table_options_clause(comment)
    prefix = "ENGINE=InnoDB, COMMENT "
    assert read_mysql(sql, prefix) == comment
    text = read_mysql(sql, prefix, backslash_escapes=False)
    if "\\" not in comment:
        assert text == comment


def test_mysql_comment_sql(mysql):
    """Only backslashes and double quotes are escaped; other comments are written as before."""
    assert mysql.quote_comment('a "b" c') == '"a ""b"" c"'
    assert mysql.quote_comment("a\\b") == '"a\\\\b"'
    sql = mysql.format_column_definition("status", "enum('active', 'retired')", comment=":enum('active', 'retired'):status")
    assert sql == "`status` enum('active', 'retired') NOT NULL COMMENT \":enum('active', 'retired'):status\""


@pytest.mark.parametrize("standard_conforming_strings", [True, False], ids=["standard", "backslash_escapes"])
@pytest.mark.parametrize("comment", COMMENTS)
def test_postgres_column_comment(postgres, comment, standard_conforming_strings):
    sql = postgres.column_comment_ddl('"schema"."table"', "note", comment)
    prefix = 'COMMENT ON COLUMN "schema"."table"."note" IS '
    assert read_postgres(sql, prefix, standard_conforming_strings) == comment


@pytest.mark.parametrize("standard_conforming_strings", [True, False], ids=["standard", "backslash_escapes"])
@pytest.mark.parametrize("comment", COMMENTS)
def test_postgres_table_comment(postgres, comment, standard_conforming_strings):
    sql = postgres.table_comment_ddl('"schema"."table"', comment)
    prefix = 'COMMENT ON TABLE "schema"."table" IS '
    assert read_postgres(sql, prefix, standard_conforming_strings) == comment


def test_postgres_comment_without_backslash_is_a_plain_literal(postgres):
    assert postgres.quote_comment('say "hi", it\'s') == "'say \"hi\", it''s'"
    assert postgres.quote_comment("a\\b") == "E'a\\\\b'"


@pytest.mark.parametrize("backend", ["mysql", "postgresql"])
def test_compile_attribute_keeps_the_declared_comment(backend):
    """The comment returned for COMMENT ON is the declared text, with no escaping added."""
    if backend == "postgresql":
        pytest.importorskip("psycopg2")
    adapter = get_adapter(backend)
    for comment in COMMENTS:
        line = f"note : varchar(16)  # {comment}"
        name, sql, _store, stored = compile_attribute(line, in_key=False, foreign_key_sql=[], context={}, adapter=adapter)
        assert name == "note"
        assert stored == f":varchar(16):{comment}"
        if backend == "mysql":
            assert read_mysql(sql, "`note` varchar(16) NOT NULL COMMENT ") == stored


def make_attribute(comment):
    properties = dict(default_attribute_properties, name="note", type="varchar(16)", original_type="varchar(16)")
    return Attribute(**dict(properties, comment=comment))


@pytest.mark.parametrize("comment", COMMENTS)
def test_attribute_sql_quotes_the_comment(comment):
    """Attribute.sql writes the comment through the MySQL adapter's quoting."""
    sql = make_attribute(comment).sql
    prefix = "`note` varchar(16) NOT NULL COMMENT "
    assert read_mysql(sql, prefix) == f":varchar(16):{comment}"
    text = read_mysql(sql, prefix, backslash_escapes=False)
    if "\\" not in comment:
        assert text == f":varchar(16):{comment}"


def test_attribute_sql_without_quotes_is_unchanged():
    assert make_attribute("plain").sql == '`note` varchar(16) NOT NULL COMMENT ":varchar(16):plain"'
