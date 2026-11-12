"""
Unit tests for the statements ``alter()`` generates on each backend (#1625).

The MySQL statements are pinned to what ``alter()`` produced before PostgreSQL
learned to change attributes, so that work leaves MySQL as it was. The one
exception is renaming an attribute of a core type, which used to drop the
column and add a new one.
"""

import pytest

from datajoint.adapters import get_adapter
from datajoint.declare import _renamed_from, alter
from datajoint.errors import DataJointError

MYSQL_TABLE = "`djtest_alter`.`measurement`"
PG_TABLE = '"djtest_alter"."measurement"'

BASE = """
measurement_id : int32
---
value : float32   # the value
label : varchar(16)   # a label
"""

CHANGES = {
    "type": """
        measurement_id : int32
        ---
        value : float64   # the value
        label : varchar(16)   # a label
        """,
    "nullable": """
        measurement_id : int32
        ---
        value = null : float32   # the value
        label : varchar(16)   # a label
        """,
    "default": """
        measurement_id : int32
        ---
        value = 1.5 : float32   # the value
        label = "none" : varchar(16)   # a label
        """,
    "comment": """
        measurement_id : int32
        ---
        value : float32   # another comment
        label : varchar(16)   # a label
        """,
    "combined": """
        measurement_id : int32
        ---
        value = 2.5 : float64   # a wider value
        label = null : varchar(32)   # a longer label
        """,
    "add": """
        measurement_id : int32
        ---
        value : float32   # the value
        extra = null : int16   # an added attribute
        label : varchar(16)   # a label
        """,
    "drop": """
        measurement_id : int32
        ---
        label : varchar(16)   # a label
        """,
    "reorder": """
        measurement_id : int32
        ---
        label : varchar(16)   # a label
        value : float32   # the value
        """,
    "table_comment": """
        # measurements
        measurement_id : int32
        ---
        value : float32   # the value
        label : varchar(16)   # a label
        """,
    "enum": """
        measurement_id : int32
        ---
        value : float32   # the value
        label : varchar(16)   # a label
        status = "active" : enum('active', 'retired')   # status
        """,
    "rename": """
        measurement_id : int32
        ---
        reading : float32   # {value} the value
        label : varchar(16)   # a label
        """,
    "rename_and_change": """
        measurement_id : int32
        ---
        reading = null : float64   # {value} the value
        label : varchar(16)   # a label
        """,
}

DEFAULTED = """
measurement_id : int32
---
value = 1.5 : float32   # the value
"""

UNDEFAULTED = """
measurement_id : int32
---
value : float32   # the value
"""

NATIVE = """
measurement_id : int32
---
value : float   # the value
"""

NATIVE_RENAMED = """
measurement_id : int32
---
reading = null : double   # {value} the value
"""

# What alter() produced for MySQL before #1625, statement by statement.
MYSQL_BEFORE = {
    "type": 'ALTER TABLE `djtest_alter`.`measurement`\n\tMODIFY `value` double NOT NULL COMMENT ":float64:the value" ',
    "nullable": 'ALTER TABLE `djtest_alter`.`measurement`\n\tMODIFY `value` float DEFAULT NULL COMMENT ":float32:the value" ',
    "default": (
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        "\tMODIFY `value` float NOT NULL DEFAULT '1.5' COMMENT \":float32:the value\" ,\n"
        "\tMODIFY `label` varchar(16) NOT NULL DEFAULT 'none' COMMENT \":varchar(16):a label\" "
    ),
    "comment": 'ALTER TABLE `djtest_alter`.`measurement`\n\tMODIFY `value` float NOT NULL COMMENT ":float32:another comment" ',
    "combined": (
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        "\tMODIFY `value` double NOT NULL DEFAULT '2.5' COMMENT \":float64:a wider value\" ,\n"
        '\tMODIFY `label` varchar(32) DEFAULT NULL COMMENT ":varchar(32):a longer label" '
    ),
    "add": (
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        '\tADD `extra` smallint DEFAULT NULL COMMENT ":int16:an added attribute" AFTER `value`,\n'
        '\tMODIFY `label` varchar(16) NOT NULL COMMENT ":varchar(16):a label" AFTER `extra`'
    ),
    "drop": "ALTER TABLE `djtest_alter`.`measurement`\n\tDROP `value`",
    "reorder": (
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        '\tMODIFY `label` varchar(16) NOT NULL COMMENT ":varchar(16):a label" AFTER `measurement_id`,\n'
        '\tMODIFY `value` float NOT NULL COMMENT ":float32:the value" AFTER `label`'
    ),
    "table_comment": 'ALTER TABLE `djtest_alter`.`measurement`\n\tCOMMENT="measurements"',
    "enum": (
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        "\tADD `status` enum('active', 'retired') NOT NULL DEFAULT 'active' "
        "COMMENT \":enum('active', 'retired'):status\" AFTER `label`"
    ),
}


@pytest.fixture(scope="module")
def mysql():
    return get_adapter("mysql")


@pytest.fixture(scope="module")
def postgres():
    pytest.importorskip("psycopg2")
    return get_adapter("postgresql")


@pytest.mark.parametrize("change", sorted(MYSQL_BEFORE))
def test_mysql_statement_unchanged(mysql, change):
    sql, _, pre_ddl = alter(CHANGES[change], BASE, {}, mysql, full_table_name=MYSQL_TABLE)
    assert sql == [MYSQL_BEFORE[change]]
    assert pre_ddl == []


def test_mysql_default_drop_unchanged(mysql):
    sql, _, _ = alter(UNDEFAULTED, DEFAULTED, {}, mysql, full_table_name=MYSQL_TABLE)
    assert sql == ['ALTER TABLE `djtest_alter`.`measurement`\n\tMODIFY `value` float NOT NULL COMMENT ":float32:the value" ']


def test_mysql_native_rename_unchanged(mysql):
    sql, _, _ = alter(NATIVE_RENAMED, NATIVE, {}, mysql, full_table_name=MYSQL_TABLE)
    assert sql == [
        'ALTER TABLE `djtest_alter`.`measurement`\n\tCHANGE `value` `reading` double DEFAULT NULL COMMENT "{value} the value" '
    ]


def test_mysql_rename_of_core_type_keeps_the_column(mysql):
    # Before #1625 the `:float32:` marker hid the rename, and this became
    # DROP `value`, ADD `reading`: the values were lost.
    sql, _, _ = alter(CHANGES["rename_and_change"], BASE, {}, mysql, full_table_name=MYSQL_TABLE)
    assert sql == [
        "ALTER TABLE `djtest_alter`.`measurement`\n"
        '\tCHANGE `value` `reading` double DEFAULT NULL COMMENT ":float64:{value} the value" '
    ]


def test_mysql_nothing_to_alter(mysql):
    assert alter(BASE, BASE, {}, mysql, full_table_name=MYSQL_TABLE)[0] == []


def comment(column, text):
    escaped = text.replace("'", "''")
    return f"COMMENT ON COLUMN {PG_TABLE}.\"{column}\" IS '{escaped}'"


PG_EXPECTED = {
    "type": [
        f'ALTER TABLE {PG_TABLE}\n\tALTER COLUMN "value" TYPE double precision',
        comment("value", ":float64:the value"),
    ],
    "nullable": [f'ALTER TABLE {PG_TABLE}\n\tALTER COLUMN "value" DROP NOT NULL'],
    "default": [
        f"ALTER TABLE {PG_TABLE}\n\tALTER COLUMN \"value\" SET DEFAULT '1.5',\n\tALTER COLUMN \"label\" SET DEFAULT 'none'"
    ],
    "comment": [comment("value", ":float32:another comment")],
    "combined": [
        f"ALTER TABLE {PG_TABLE}\n"
        '\tALTER COLUMN "value" TYPE double precision,\n'
        "\tALTER COLUMN \"value\" SET DEFAULT '2.5',\n"
        '\tALTER COLUMN "label" TYPE varchar(32),\n'
        '\tALTER COLUMN "label" DROP NOT NULL',
        comment("value", ":float64:a wider value"),
        comment("label", ":varchar(32):a longer label"),
    ],
    "add": [
        f'ALTER TABLE {PG_TABLE}\n\tADD COLUMN "extra" smallint DEFAULT NULL',
        comment("extra", ":int16:an added attribute"),
    ],
    "drop": [f'ALTER TABLE {PG_TABLE}\n\tDROP COLUMN "value"'],
    "reorder": [],  # PostgreSQL cannot position columns
    "table_comment": [f"COMMENT ON TABLE {PG_TABLE} IS 'measurements'"],
    "rename": [
        f'ALTER TABLE {PG_TABLE} RENAME COLUMN "value" TO "reading"',
        comment("reading", ":float32:{value} the value"),
    ],
    "rename_and_change": [
        f'ALTER TABLE {PG_TABLE} RENAME COLUMN "value" TO "reading"',
        f'ALTER TABLE {PG_TABLE}\n\tALTER COLUMN "reading" TYPE double precision,\n\tALTER COLUMN "reading" DROP NOT NULL',
        comment("reading", ":float64:{value} the value"),
    ],
}


@pytest.mark.parametrize("change", sorted(PG_EXPECTED))
def test_postgres_statements(postgres, change):
    sql, _, pre_ddl = alter(CHANGES[change], BASE, {}, postgres, full_table_name=PG_TABLE)
    assert sql == PG_EXPECTED[change]
    assert pre_ddl == []


def test_postgres_add_enum(postgres):
    sql, _, pre_ddl = alter(CHANGES["enum"], BASE, {}, postgres, full_table_name=PG_TABLE)
    (type_ddl,) = pre_ddl
    assert type_ddl.startswith('CREATE TYPE "djtest_alter"."enum_') and type_ddl.endswith("AS ENUM ('active', 'retired')")
    enum_type = '"{database}".' + type_ddl.split(".")[1].split()[0]
    assert sql == [
        f"ALTER TABLE {PG_TABLE}\n\tADD COLUMN \"status\" {enum_type} NOT NULL DEFAULT 'active'",
        comment("status", ":enum('active', 'retired'):status"),
    ]


def test_postgres_drop_default(postgres):
    sql, _, _ = alter(UNDEFAULTED, DEFAULTED, {}, postgres, full_table_name=PG_TABLE)
    assert sql == [f'ALTER TABLE {PG_TABLE}\n\tALTER COLUMN "value" DROP DEFAULT']


def test_postgres_type_change_resets_default(postgres):
    """A default is dropped before the type changes, then set again."""
    old = """
    measurement_id : int32
    ---
    code = "7" : varchar(8)   # a code
    """
    new = """
    measurement_id : int32
    ---
    code = 7 : int32   # a code
    """
    sql, _, _ = alter(new, old, {}, postgres, full_table_name=PG_TABLE)
    assert sql == [
        f"ALTER TABLE {PG_TABLE}\n"
        '\tALTER COLUMN "code" DROP DEFAULT,\n'
        '\tALTER COLUMN "code" TYPE integer USING "code"::integer,\n'
        "\tALTER COLUMN \"code\" SET DEFAULT '7'",
        comment("code", ":int32:a code"),
    ]


def test_postgres_change_to_enum(postgres):
    old = """
    measurement_id : int32
    ---
    species : varchar(16)   # the species
    """
    new = """
    measurement_id : int32
    ---
    species : enum('mouse', 'rat')   # the species
    """
    sql, _, pre_ddl = alter(new, old, {}, postgres, full_table_name=PG_TABLE)
    (type_ddl,) = pre_ddl
    enum_type = '"{database}".' + type_ddl.split(".")[1].split()[0]
    assert sql == [
        f'ALTER TABLE {PG_TABLE}\n\tALTER COLUMN "species" TYPE {enum_type} USING "species"::text::{enum_type}',
        comment("species", ":enum('mouse', 'rat'):the species"),
    ]


def test_postgres_unconvertible_type_is_refused(postgres):
    old = """
    measurement_id : int32
    ---
    taken : date   # when it was taken
    """
    new = """
    measurement_id : int32
    ---
    taken : int32   # when it was taken
    """
    with pytest.raises(DataJointError, match="cannot convert attribute `taken` from date to integer"):
        alter(new, old, {}, postgres, full_table_name=PG_TABLE)


ENUM = '"{database}"."enum_0123abcd"'


@pytest.mark.parametrize(
    "old_type, new_type, expected",
    [
        # an assignment conversion exists
        ("real", "double precision", None),
        ("integer", "smallint", None),
        ("double precision", "numeric(8,2)", None),
        ("varchar(16)", "varchar(64)", None),
        ("varchar(64)", "char(8)", None),
        ("integer", "varchar(8)", None),
        ("timestamp", "varchar(32)", None),
        ("boolean", "varchar(8)", None),
        (ENUM, "varchar(16)", None),
        ("date", "timestamp(3)", None),
        ("timestamp", "date", None),
        # an explicit conversion is needed
        ("varchar(8)", "integer", '"x"::integer'),
        ("varchar(8)", "boolean", '"x"::boolean'),
        ("varchar(36)", "uuid", '"x"::uuid'),
        ("varchar(32)", "date", '"x"::date'),
        ("varchar(64)", "jsonb", '"x"::jsonb'),
        ("varchar(16)", ENUM, f'"x"::text::{ENUM}'),
        ('"other"."enum_99999999"', ENUM, f'"x"::text::{ENUM}'),
        (ENUM, "integer", '"x"::text::integer'),
        ("smallint", "boolean", '"x" <> 0'),
        ("boolean", "bigint", '"x"::integer'),
        ("jsonb", "double precision", '"x"::double precision'),
        ("integer", "jsonb", 'to_jsonb("x")'),
        # a type this conversion does not know: left to PostgreSQL
        ("interval", "time", None),
    ],
)
def test_postgres_type_conversion(postgres, old_type, new_type, expected):
    assert postgres._type_conversion("x", old_type, new_type) == expected


@pytest.mark.parametrize(
    "old_type, new_type",
    [("date", "integer"), ("integer", "timestamp"), ("uuid", "bigint"), ("bytea", "uuid"), ("boolean", "date")],
)
def test_postgres_type_conversion_refused(postgres, old_type, new_type):
    with pytest.raises(DataJointError, match="cannot convert"):
        postgres._type_conversion("x", old_type, new_type)


@pytest.mark.parametrize(
    "reported, default",
    [
        ("'1.5'::real", "1.5"),
        ("'abc'::character varying", "abc"),
        ("'it''s'::character varying", "it's"),
        ("''::character varying", ""),
        ("'a'::\"some_schema\".enum_0123abcd", "a"),
        ("'2020-01-01 00:00:00'::timestamp without time zone", "2020-01-01 00:00:00"),
        ("5", "5"),
        ("true", "true"),
        ("CURRENT_TIMESTAMP", "CURRENT_TIMESTAMP"),
        ("nextval('t_id_seq'::regclass)", "nextval('t_id_seq'::regclass)"),
        (None, None),
    ],
)
def test_postgres_reports_default_without_cast(postgres, reported, default):
    row = dict(column_name="x", data_type="text", udt_name="text", is_nullable="NO", column_default=reported)
    assert postgres.parse_column_info(row)["default"] == default


@pytest.mark.parametrize(
    "comment, name",
    [
        ("{value} the value", "value"),
        ("{ value } the value", "value"),
        ("{old_name}", "old_name"),
        ("the value {value}", None),
        ("the value", None),
        ("", None),
    ],
)
def test_renamed_from(comment, name):
    assert _renamed_from(comment) == name
