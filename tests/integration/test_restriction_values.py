"""
Mapping restrictions with values that need quoting, on both backends.

Values in dict restrictions are compared as literals: quote characters, backslashes,
``%``, and SQL keywords inside a value are part of the value.
"""

import numpy as np
import pytest

import datajoint as dj

ROWS = [
    dict(item_id=1, name="alpha", data={"label": "alpha", "tags": [1, 2], "sub": {"k": 1}, "size": 3}),
    dict(item_id=2, name="o'brien", data={"label": "o'brien", "tags": [3], "sub": {"k": 2}, "size": 4}),
    dict(item_id=3, name="50% off", data={"label": "50% off", "tags": [], "sub": {"k": 3}, "size": 5}),
    dict(item_id=4, name='say "hi"', data={"label": 'say "hi"', "tags": [4], "sub": {"k": "x"}, "size": 6}),
]

SPECIAL_CHARACTER_VALUES = [
    "' OR 1=1 --",
    "' OR '1'='1",
    "x' OR 'a'='a",
    "\\' OR 1=1 -- ",
    "') OR ('1'='1",
    '" OR ""="',
    "'; DELETE FROM item; --",
    "%' OR 1=1 --",
]


@pytest.fixture
def item(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_restriction_values_{backend}", connection=connection_by_backend)

    @schema
    class Item(dj.Manual):
        definition = """
        item_id : int32
        ---
        name : varchar(64)
        data = null : json
        """

    Item.insert(ROWS)
    yield Item()
    schema.drop(prompt=False)


def ids(query):
    return sorted(query.to_arrays("item_id").tolist())


@pytest.mark.parametrize("value", SPECIAL_CHARACTER_VALUES)
def test_values_with_quote_characters_match_nothing(item, value):
    assert ids(item & {"name": value}) == []
    assert ids(item & {"data.label": value}) == []
    assert ids(item & {"data.sub": {"k": value}}) == []
    assert ids(item & {"name": [value, 'a"b', "c'd"]}) == []
    assert ids(item & [{"name": value}, {"item_id": 1}]) == [1]
    assert ids(item - {"name": value}) == [1, 2, 3, 4]
    assert len(item & {"name": value}) == 0


@pytest.mark.parametrize("value", SPECIAL_CHARACTER_VALUES)
def test_values_with_quote_characters_delete_nothing(item, value):
    (item & {"name": value}).delete_quick()
    (item & {"data.label": value}).delete_quick()
    (item & {"name": [value]}).delete_quick()
    (item & {"data.sub": {"k": value}}).delete_quick()
    assert len(item) == len(ROWS)


@pytest.mark.parametrize(
    "key",
    [
        "data.x') OR 1=1 -- ",
        "data.x` OR 1=1 OR `y",
        'data.x" OR 1=1 OR "y',
        "data.label'] OR 1=1 --",
    ],
)
def test_invalid_json_keys_rejected(item, key):
    with pytest.raises(dj.DataJointError):
        item & {key: 1}


@pytest.mark.parametrize("type_spec", ["unsigned) OR (1", "char(20)) OR (1", "signed or 1", "nonsense"])
def test_invalid_json_types_rejected(item, type_spec):
    with pytest.raises(dj.DataJointError):
        item & {f"data.size:{type_spec}": 1}


def test_unsupported_value_types_rejected(item):
    with pytest.raises(dj.DataJointError):
        item & {"name": b"' OR 1=1 --"}
    with pytest.raises(dj.DataJointError):
        item & {"name": {"a": "' OR 1=1 --"}}


def test_ordinary_values_still_match(item):
    assert ids(item & {"item_id": 2}) == [2]
    assert ids(item & {"item_id": np.int64(3)}) == [3]
    assert ids(item & {"name": "alpha"}) == [1]
    assert ids(item & {"name": "o'brien"}) == [2]
    assert ids(item & {"name": "50% off"}) == [3]
    assert ids(item & {"name": 'say "hi"'}) == [4]
    assert ids(item & [{"name": "alpha"}, {"name": "o'brien"}]) == [1, 2]
    assert ids(item - {"name": "o'brien"}) == [1, 3, 4]
    assert ids(item & {"data.label": "o'brien"}) == [2]
    assert ids(item & {"data.label": "50% off"}) == [3]
    assert ids(item & {"data.tags": [1, 2]}) == [1]
    assert ids(item & {"data.sub": {"k": 2}}) == [2]
    assert ids(item & {"data.sub": {"k": "x"}}) == [4]
    assert ids(item & {"data.size:unsigned": 5}) == [3]
    assert ids(item & {"data.size:decimal(4, 1)": 6}) == [4]
    assert ids(item & {"data": None}) == []


def test_string_with_backslash(item, backend):
    """On MySQL a backslash in a string value matches itself."""
    item.insert1(dict(item_id=5, name="back\\slash", data={"label": "back\\slash", "sub": {"k": "a\\b"}}))
    assert ids(item & {"data.sub": {"k": "a\\b"}}) == [5]
    if backend == "mysql":
        assert ids(item & {"name": "back\\slash"}) == [5]
        assert ids(item & {"data.label": "back\\slash"}) == [5]


def test_mysql_values_without_backslash_escapes(item, backend):
    """On MySQL, string values stay closed literals when the session enables NO_BACKSLASH_ESCAPES."""
    if backend != "mysql":
        pytest.skip("sql_mode is MySQL-specific")
    connection = item.connection
    sql_mode = connection.query("SELECT @@SESSION.sql_mode").fetchone()[0]
    try:
        connection.query("SET SESSION sql_mode = %s", (",".join(filter(None, [sql_mode, "NO_BACKSLASH_ESCAPES"])),))
        assert ids(item & {"name": "' OR 1=1 --"}) == []
        assert ids(item & {"name": "o'brien"}) == [2]
    finally:
        connection.query("SET SESSION sql_mode = %s", (sql_mode,))


def test_postgresql_list_values_without_standard_conforming_strings(item, backend):
    """On PostgreSQL, list values stay closed literals when standard_conforming_strings is off."""
    if backend != "postgresql":
        pytest.skip("standard_conforming_strings is PostgreSQL-specific")
    item.insert1(dict(item_id=5, name=str(["back\\slash"])))
    connection = item.connection
    setting = connection.query("SHOW standard_conforming_strings").fetchone()[0]
    try:
        connection.query("SET standard_conforming_strings = off")
        assert ids(item & {"name": ["'OR 1=1 -- \""]}) == []
        assert ids(item & {"name": ["back\\slash"]}) == [5]
    finally:
        connection.query(f"SET standard_conforming_strings = {setting}")


QUOTED_STRING_VALUES = ["a\\'b", "x\\", "\\", "it's", 'he said "no"', 'a\\"b', "$$ c $$"]


@pytest.mark.parametrize("value", QUOTED_STRING_VALUES)
def test_string_restriction_with_quoted_literal(item, backend, value):
    """A string restriction built with quote_string runs and matches the value (#1697)."""
    item.insert1(dict(item_id=5, name=value))
    adapter = item.connection.adapter
    literal = adapter.quote_string(value)
    name = adapter.quote_identifier("name")
    assert ids(item & f"{name} = {literal}") == [5]
    assert ids(item & f"{name} = {literal} AND item_id > 1") == [5]
    assert ids(item - f"{name} = {literal}") == [1, 2, 3, 4]
    assert ids(item & f"{name} <> {literal} AND item_id < 3") == [1, 2]
    flags = item.proj(same=f"{name} = {literal}").to_arrays("same", order_by="item_id")
    assert [bool(f) for f in flags] == [False] * 4 + [True]


def test_string_restriction_with_backend_literals(item, backend):
    """Literals in the backend's other quoting forms are skipped when reading column names."""
    item.insert1(dict(item_id=5, name="x\\"))
    if backend == "mysql":
        assert ids(item & "`name` = 'x\\\\' AND item_id > 1") == [5]
        assert ids(item & 'name = "say \\"hi\\"" AND item_id > 1') == [4]
        assert ids(item & 'name = "say ""hi""" AND item_id > 1') == [4]
        assert ids(item & "name = 'o\\'brien' AND item_id > 1") == [2]
    else:
        assert ids(item & "name = 'x\\' AND item_id > 1") == [5]
        assert ids(item & "name = E'x\\\\' AND item_id > 1") == [5]
        assert ids(item & "name = E'o\\'brien' AND item_id > 1") == [2]
        assert ids(item & "name = $$o'brien$$ AND item_id > 1") == [2]


NUMERIC_JSON_TYPES = {
    "mysql": ["unsigned", "signed", "decimal(4, 1)", "double", "float"],
    "postgresql": ["integer", "int", "bigint", "numeric(10,2)", "double precision", "real", "unsigned", "double"],
}
TEXT_JSON_TYPES = {
    "mysql": ["char(20)", "char(20) character set utf8mb4"],
    "postgresql": ["text", "varchar(32)", "character varying(32)"],
}


def test_json_types_in_restriction_and_projection(item, backend):
    for type_spec in NUMERIC_JSON_TYPES[backend]:
        assert ids(item & {f"data.size:{type_spec}": 5}) == [3], type_spec
        values = item.proj(size=f"data.size:{type_spec}").to_arrays("size", order_by="item_id")
        assert [float(v) for v in values] == [3, 4, 5, 6], type_spec
    for type_spec in TEXT_JSON_TYPES[backend]:
        assert ids(item & {f"data.label:{type_spec}": "o'brien"}) == [2], type_spec
        values = item.proj(label=f"data.label:{type_spec}").to_arrays("label", order_by="item_id")
        assert list(values) == [row["data"]["label"] for row in ROWS], type_spec


def test_json_type_in_index_declaration(connection_by_backend, backend, prefix):
    if backend == "postgresql":
        pytest.skip("Indexes on JSON paths are not created on PostgreSQL (index name is derived from the expression)")
    schema = dj.Schema(f"{prefix}_restriction_values_index_{backend}", connection=connection_by_backend)
    numeric, text = NUMERIC_JSON_TYPES[backend][0], TEXT_JSON_TYPES[backend][0]
    try:

        @schema
        class Indexed(dj.Manual):
            definition = f"""
            item_id : int32
            ---
            data = null : json
            index(data.size:{numeric})
            index(data.label:{text})
            """

        Indexed.insert(dict(item_id=r["item_id"], data=r["data"]) for r in ROWS)
        assert sorted((Indexed & {f"data.size:{numeric}": 4}).to_arrays("item_id").tolist()) == [2]
    finally:
        schema.drop(prompt=False)


def test_non_finite_numbers_rejected(item):
    for value in (float("nan"), float("inf"), np.float64("-inf")):
        with pytest.raises(dj.DataJointError):
            item & {"item_id": value}
