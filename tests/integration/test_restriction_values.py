"""
Mapping restrictions with values that need quoting, on both backends.

Values in dict restrictions are compared as literals: quotes, backslashes, ``%``,
and SQL fragments inside them select no extra rows.
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

HOSTILE = [
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


@pytest.mark.parametrize("value", HOSTILE)
def test_hostile_string_values_match_nothing(item, value):
    assert ids(item & {"name": value}) == []
    assert ids(item & {"data.label": value}) == []
    assert ids(item & {"data.sub": {"k": value}}) == []
    assert ids(item & {"name": [value, 'a"b', "c'd"]}) == []
    assert ids(item & [{"name": value}, {"item_id": 1}]) == [1]
    assert ids(item - {"name": value}) == [1, 2, 3, 4]
    assert len(item & {"name": value}) == 0


@pytest.mark.parametrize("value", HOSTILE)
def test_hostile_values_do_not_delete(item, value):
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
