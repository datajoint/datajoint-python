"""
Mapping restrictions with values that need quoting, on both backends.

Values in dict restrictions are compared as literals: quote characters, backslashes,
``%``, and SQL keywords inside a value are part of the value.
"""

import datetime

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


@pytest.fixture
def shift(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_restriction_values_time_{backend}", connection=connection_by_backend)

    @schema
    class Shift(dj.Manual):
        definition = """
        start_time : time
        """

    @schema
    class Slot(dj.Manual):
        definition = """
        -> Shift
        slot : int32
        """

    @schema
    class Duty(dj.Computed):
        definition = """
        -> Shift
        ---
        matches : int32
        """

        def make(self, key):
            self.insert1(dict(key, matches=len(Shift & key)))

    @schema
    class PreciseShift(dj.Manual):
        definition = """
        start_time : time(6)
        """

    yield Shift(), Slot(), Duty(), PreciseShift()
    schema.drop(prompt=False)


def test_time_key_restricts_its_own_table(shift, backend):
    """A key fetched from a table with a time attribute restricts that table."""
    Shift, _, _, PreciseShift = shift
    Shift.insert([{"start_time": "01:02:03"}, {"start_time": "04:05:06"}, {"start_time": "23:59:59"}])
    PreciseShift.insert([{"start_time": "01:02:03"}, {"start_time": "01:02:03.500000"}, {"start_time": "23:59:59"}])
    expected_type = datetime.timedelta if backend == "mysql" else datetime.time
    for table in (Shift, PreciseShift):
        keys = table.keys()
        assert all(isinstance(key["start_time"], expected_type) for key in keys)
        for key in keys:
            assert len(table & key) == 1
            assert (table & key).fetch1("KEY") == key
            assert len(table - key) == 2
        assert len(table & keys) == 3
        assert len(table & table.to_pandas()) == 3


def test_delete_by_time_key(shift):
    Shift, Slot, _, _ = shift
    Shift.insert([{"start_time": "01:02:03"}, {"start_time": "04:05:06"}])
    Slot.insert([{"start_time": "01:02:03", "slot": 1}, {"start_time": "04:05:06", "slot": 1}])
    key = Shift.keys(order_by="start_time", limit=1)[0]
    (Shift & key).delete(prompt=False)
    assert len(Shift) == 1 and len(Slot) == 1
    assert len(Shift & key) == 0 and len(Slot & key) == 0


@pytest.mark.parametrize("reserve_jobs", [False, True])
def test_populate_below_time_key(shift, backend, reserve_jobs):
    Shift, _, Duty, _ = shift
    if reserve_jobs and backend == "postgresql":
        pytest.skip("the jobs table cannot yet be declared below a PostgreSQL time key (#1763)")
    Shift.insert([{"start_time": "01:02:03"}, {"start_time": "04:05:06"}])
    Duty.populate(reserve_jobs=reserve_jobs)
    assert Duty.keys(order_by="start_time") == Shift.keys(order_by="start_time")
    assert Duty.to_arrays("matches").tolist() == [1, 1]


def test_timedelta_values_on_postgresql_time(shift, backend):
    """On PostgreSQL a time attribute also matches a timedelta within one day."""
    if backend != "postgresql":
        pytest.skip("PostgreSQL time attributes are fetched as datetime.time")
    _, _, _, PreciseShift = shift
    PreciseShift.insert([{"start_time": datetime.time(1, 2, 3)}, {"start_time": datetime.time(1, 2, 3, 500000)}])
    assert len(PreciseShift & {"start_time": datetime.timedelta(hours=1, minutes=2, seconds=3)}) == 1
    assert len(PreciseShift & {"start_time": pd.Timedelta("01:02:03.5")}) == 1


MYSQL_TIME_VALUES = [
    ("-00:00:01", datetime.timedelta(seconds=-1)),
    ("-01:02:03.250000", -datetime.timedelta(hours=1, minutes=2, seconds=3, microseconds=250000)),
    ("26:00:00", datetime.timedelta(days=1, hours=2)),
    ("838:59:59", datetime.timedelta(hours=838, minutes=59, seconds=59)),
    ("-838:59:59", -datetime.timedelta(hours=838, minutes=59, seconds=59)),
    ("00:00:00", datetime.timedelta(0)),
]


def test_mysql_negative_and_long_time_keys(shift, backend):
    """MySQL time values range over -838:59:59 to 838:59:59."""
    if backend != "mysql":
        pytest.skip("PostgreSQL time values lie within one day")
    _, _, _, PreciseShift = shift
    PreciseShift.insert({"start_time": text} for text, _ in MYSQL_TIME_VALUES)
    for _, value in MYSQL_TIME_VALUES:
        key = (PreciseShift & {"start_time": value}).fetch1("KEY")
        assert key == {"start_time": value}
        assert len(PreciseShift & {"start_time": pd.Timedelta(value)}) == 1
        assert len(PreciseShift & {"start_time": np.timedelta64(value)}) == 1
    assert len(PreciseShift & PreciseShift.to_pandas()) == len(MYSQL_TIME_VALUES)
    (PreciseShift & {"start_time": datetime.timedelta(days=1, hours=2)}).delete(prompt=False)
    assert len(PreciseShift) == len(MYSQL_TIME_VALUES) - 1


@pytest.mark.parametrize("value", [np.timedelta64(5), np.timedelta64(1, "Y"), np.timedelta64(2, "M")])
def test_timedelta64_without_fixed_unit_rejected(shift, value):
    Shift, _, _, _ = shift
    with pytest.raises(dj.DataJointError, match="no fixed unit"):
        Shift & {"start_time": value}
