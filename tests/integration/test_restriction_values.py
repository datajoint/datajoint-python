"""
Mapping restrictions with values that need quoting, on both backends.

Values in dict restrictions are compared as literals: quote characters, backslashes,
``%``, and SQL keywords inside a value are part of the value.
"""

import uuid

import numpy as np
import pandas as pd
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


def row_ids(query):
    """item_id of each row, fetched with all attributes (a restricted aggregation needs them)."""
    return sorted(row["item_id"] for row in query.to_dicts())


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


NON_ASCII_ROW = dict(item_id=5, name="Zoë", data={"label": "Zoë", "sub": {"k": "Zoë 名前"}, "size": 7})


def test_json_object_value_with_non_ascii_text(item):
    """A dict value for a JSON path matches stored JSON that holds non-ASCII text."""
    item.insert1(NON_ASCII_ROW)
    assert ids(item & {"data.sub": {"k": "Zoë 名前"}}) == [5]
    assert ids(item & {"data.sub": {"k": "Zoe 名前"}}) == []
    assert ids(item & {"data.label": "Zoë"}) == [5]
    assert ids(item & {"name": "Zoë"}) == [5]


def test_uuid_restricted_by_non_uuid_value(connection_by_backend, backend, prefix):
    """A uuid attribute restricted or inserted with bytes raises DataJointError, not TypeError."""
    schema = dj.Schema(f"{prefix}_restriction_values_uuid_{backend}", connection=connection_by_backend)
    try:

        @schema
        class Tagged(dj.Manual):
            definition = """
            tagged_id : int32
            ---
            token = null : uuid
            """

        for value in (uuid.UUID(int=1).bytes, b"' OR 1=1 --", 1):
            with pytest.raises(dj.DataJointError, match="Badly formed UUID"):
                Tagged & {"token": value}
        with pytest.raises(dj.DataJointError):
            Tagged.insert1(dict(tagged_id=1, token=uuid.UUID(int=1).bytes))
        if backend == "mysql":
            Tagged.insert1(dict(tagged_id=1, token=uuid.UUID(int=1)))
            assert len(Tagged & {"token": str(uuid.UUID(int=1))}) == 1
            assert len(Tagged & {"token": uuid.UUID(int=2)}) == 0
    finally:
        schema.drop(prompt=False)


@pytest.fixture
def item_with_tags(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_restriction_values_tags_{backend}", connection=connection_by_backend)

    @schema
    class Item(dj.Manual):
        definition = """
        item_id : int32
        ---
        name : varchar(64)
        data = null : json
        """

    @schema
    class Tag(dj.Manual):
        definition = """
        -> Item
        tag : varchar(32)
        """

    Item.insert(ROWS + [NON_ASCII_ROW])
    Tag.insert(
        [
            dict(item_id=1, tag="a"),
            dict(item_id=2, tag="o'brien"),
            dict(item_id=2, tag="50% off"),
            dict(item_id=3, tag="b"),
            dict(item_id=5, tag="Zoë"),
        ]
    )
    yield Item(), Tag()
    schema.drop(prompt=False)


@pytest.mark.parametrize("value", SPECIAL_CHARACTER_VALUES)
def test_quoted_values_in_derived_expressions(item_with_tags, value):
    """Restrictions on joins, renamed projections, aggregations, and dj.Top match only literally."""
    item, tag = item_with_tags
    assert ids(item * tag & {"name": value}) == []
    assert ids(item * tag & {"tag": value}) == []
    assert ids(item.proj(label="name") & {"label": value}) == []
    assert row_ids(item.aggr(tag, "name", n="count(*)") & {"name": value}) == []
    assert ids(item & dj.Top(10, order_by="item_id") & {"name": value}) == []
    assert ids(item * tag - {"tag": value}) == [1, 2, 2, 3, 5]


def test_ordinary_values_in_derived_expressions(item_with_tags):
    item, tag = item_with_tags
    assert ids(item * tag & {"name": "o'brien"}) == [2, 2]
    assert ids(item * tag & {"tag": "50% off"}) == [2]
    assert ids(item * tag & {"tag": "Zoë"}) == [5]
    assert ids(item.proj(label="name") & {"label": "o'brien"}) == [2]
    assert ids(item.proj(label="name") & {"label": "Zoë"}) == [5]
    assert row_ids(item.aggr(tag, "name", n="count(*)") & {"name": "o'brien"}) == [2]
    assert row_ids(item.aggr(tag, "name", n="count(*)") & {"n": 2}) == [2]
    assert ids(item & dj.Top(3, order_by="item_id") & {"name": "50% off"}) == [3]
    assert ids(item & dj.Top(2, order_by="item_id") & {"name": "50% off"}) == []
    assert ids(item & {"data.sub": {"k": "Zoë 名前"}}) == [5]


@pytest.mark.parametrize("value", ["o'brien", "Zoë"])
def test_values_from_records_and_dataframes(item_with_tags, value):
    """Restrictions by record arrays, numpy.void rows, and DataFrame rows quote their values."""
    item, _ = item_with_tags
    expected = ids(item & {"name": value})
    assert len(expected) == 1
    records = np.array([(value,), ("' OR 1=1 --",)], dtype=[("name", object)])
    assert ids(item & records) == expected
    assert ids(item & records[0]) == expected
    assert ids(item & records[1]) == []
    frame = pd.DataFrame({"name": [value, "' OR 1=1 --"]})
    assert ids(item & frame) == expected
    assert ids(item & frame.iloc[[1]]) == []
    fetched = item.proj("name").to_arrays()
    assert ids(item & fetched) == [1, 2, 3, 4, 5]
    assert ids(item & fetched[fetched["name"] == value][0]) == expected


@pytest.mark.parametrize("value", SPECIAL_CHARACTER_VALUES[:3])
def test_cascading_delete_with_quoted_values(item_with_tags, value):
    item, tag = item_with_tags
    (item & {"name": value}).delete(prompt=False)
    assert len(item) == len(ROWS) + 1
    assert len(tag) == 5
    (item & {"name": "o'brien"}).delete(prompt=False)
    assert ids(item) == [1, 3, 4, 5]
    assert ids(tag) == [1, 3, 5]


def test_update1_with_quoted_values(item_with_tags, backend):
    item, _ = item_with_tags
    for value in SPECIAL_CHARACTER_VALUES + ["Zoë Ünal"]:
        item.update1(dict(item_id=2, name=value, data={"label": value, "sub": {"k": value}}))
        assert (item & {"item_id": 2}).fetch1("name") == value
        assert ids(item & {"data.sub": {"k": value}}) == [2]
        if backend == "mysql" or "\\" not in value:
            # PostgreSQL doubles backslashes in string values (see test_string_with_backslash)
            assert ids(item & {"name": value}) == [2]
            assert ids(item & {"data.label": value}) == [2]
    assert ids(item & {"name": "o'brien"}) == []
