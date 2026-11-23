"""
Comments with quotes and backslashes are stored as declared, on both backends.

Each backend writes a comment as a string literal of its own syntax. A double
quote, a single quote or a backslash in the comment is part of the comment: it is
stored and read back as written, ``describe()`` writes it back unchanged, and it
never ends the literal early.
"""

import pytest

import datajoint as dj

TABLE_COMMENT = r"""a "quoted" table, it's at C:\new\table \" , x"""

DEFINITION = rf"""
# {TABLE_COMMENT}
item_id : int32  # key "quoted", it's \
---
double_quotes : varchar(16)  # say "hi"
single_quote : varchar(16)  # it's a test
backslash : varchar(16)  # back\slash
escape_sequences : varchar(16)  # C:\new\table\0
escaped_quotes : varchar(16)  # a \" b \' c
doubled_quotes : varchar(16)  # a '' b "" c
breakout_double : varchar(16)  # x\" , injected int NOT NULL COMMENT "
breakout_single : varchar(16)  # x\'; SELECT 1; --
trailing_backslash : varchar(16)  # ends with a backslash \
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

    assert Item.heading.names == list(COMMENTS)
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
    """With NO_BACKSLASH_ESCAPES, each comment still ends where it should."""
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
    assert Item.heading.names == list(COMMENTS)
    assert stored_comments(Item)["double_quotes"] == COMMENTS["double_quotes"]


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
