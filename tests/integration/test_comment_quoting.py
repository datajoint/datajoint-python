"""
Comments with quotes and backslashes are stored as declared, on both backends.

Each backend writes a comment as a string literal of its own syntax. A double
quote, a single quote or a backslash in the comment is part of the comment: it is
stored and read back as written, ``describe()`` writes it back unchanged, and it
never ends the literal early.
"""

import pytest

import datajoint as dj

TABLE_COMMENT = r"""a table note with "double quotes", it's at C:\new\table, a backslash and a quote \" , and more"""

DEFINITION = rf"""
# {TABLE_COMMENT}
item_id : int32  # the item's "number", with a trailing backslash \
---
quote_double : varchar(16)  # a note with "double quotes"
quote_single : varchar(16)  # it's a single-quoted note
backslash : varchar(16)  # a path like back\slash
backslash_pair : varchar(16)  # two backslashes \\ in a row
backslash_sequences : varchar(16)  # a path like C:\new\table\0
backslash_quotes : varchar(16)  # a backslash before quotes \" and \'
doubled_quotes : varchar(16)  # doubled quotes '' and ""
backslash_double_comma : varchar(16)  # a backslash and a double quote \", then a comma
backslash_single_semicolon : varchar(16)  # a backslash and a single quote \'; with a semicolon -- and dashes
trailing_backslash : varchar(16)  # a note with "double quotes", a comma, and a trailing backslash \
"""


def declared_comments(definition):
    """The comment of each attribute line in a definition, as written."""
    return {
        line.split(":", 1)[0].strip(): line.split("#", 1)[1].strip()
        for line in definition.strip().splitlines()[1:]
        if "#" in line
    }


COMMENTS = declared_comments(DEFINITION)


@pytest.fixture
def schema(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_comment_quoting_{backend}", connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def stored_comments(table):
    return {name: attr.comment for name, attr in table.heading.attributes.items()}


def test_comments_stored_as_declared(schema):
    @schema
    class Item(dj.Manual):
        definition = DEFINITION

    assert stored_comments(Item) == COMMENTS
    assert Item.heading.table_status["comment"] == TABLE_COMMENT


def test_describe_round_trip(schema):
    """A table declared from describe() has the same comments and the same describe()."""

    @schema
    class Item(dj.Manual):
        definition = DEFINITION

    described = Item().describe()

    @schema
    class Copy(dj.Manual):
        definition = described

    assert stored_comments(Copy) == COMMENTS
    assert Copy.heading.table_status["comment"] == TABLE_COMMENT
    assert Copy().describe() == described


def test_alter_finds_nothing_to_alter(schema, monkeypatch):
    @schema
    class Item(dj.Manual):
        definition = DEFINITION

    def changes_proposed(message):
        raise AssertionError(f"alter() proposed changes to an unchanged table:\n{message}")

    monkeypatch.setattr(dj.table, "user_choice", changes_proposed)
    Item.alter(prompt=True)


def test_foreign_key_copies_comment(schema):
    @schema
    class Item(dj.Manual):
        definition = DEFINITION

    @schema
    class Child(dj.Manual):
        definition = """
        -> Item
        child_id : int32
        """

    assert Child.heading["item_id"].comment == COMMENTS["item_id"]
    assert Child.heading["item_id"].type == Item.heading["item_id"].type


def test_alter_writes_comments_as_declared(schema, backend):
    if backend == "postgresql":
        pytest.skip("alter() changes comments on PostgreSQL from PR 1639")

    # alter() leaves the primary key as it is, so it starts with the key comment
    @schema
    class Item(dj.Manual):
        definition = f"""
        item_id : int32  # {COMMENTS['item_id']}
        ---
        note : varchar(16)
        """

    Item.definition = DEFINITION
    Item.alter(prompt=False)
    assert stored_comments(Item) == COMMENTS
    assert Item.heading.table_status["comment"] == TABLE_COMMENT


def test_mysql_comments_without_backslash_escapes(schema, backend):
    """With NO_BACKSLASH_ESCAPES, comments are stored as declared, with each backslash doubled."""
    if backend != "mysql":
        pytest.skip("sql_mode is MySQL-specific")
    connection = schema.connection
    sql_mode = connection.query("SELECT @@SESSION.sql_mode").fetchone()[0]
    try:
        connection.query("SET SESSION sql_mode = %s", (",".join(filter(None, [sql_mode, "NO_BACKSLASH_ESCAPES"])),))

        @schema
        class Item(dj.Manual):
            definition = DEFINITION

    finally:
        connection.query("SET SESSION sql_mode = %s", (sql_mode,))
    # Without backslash escapes, MySQL reads each written backslash literally, so a
    # backslash is stored doubled and every other character as declared
    assert stored_comments(Item) == {name: comment.replace("\\", "\\\\") for name, comment in COMMENTS.items()}


def test_postgresql_comments_without_standard_conforming_strings(schema, backend):
    """With standard_conforming_strings off, comments are still stored as declared."""
    if backend != "postgresql":
        pytest.skip("standard_conforming_strings is PostgreSQL-specific")
    connection = schema.connection
    setting = connection.query("SHOW standard_conforming_strings").fetchone()[0]
    try:
        connection.query("SET standard_conforming_strings = off")

        @schema
        class Item(dj.Manual):
            definition = DEFINITION

    finally:
        connection.query(f"SET standard_conforming_strings = {setting}")
    assert stored_comments(Item) == COMMENTS
    assert Item.heading.table_status["comment"] == TABLE_COMMENT
