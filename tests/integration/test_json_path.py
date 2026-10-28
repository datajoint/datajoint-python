"""
JSON path restrictions and type annotations give the same result on MySQL and
PostgreSQL.

- A mapping restriction on a JSON path compares bool and number values as JSON
  values, so ``True`` and ``64`` work on both backends and ``64`` matches a
  stored ``64.0`` (#1564).
- A ``attr.path:type`` annotation accepts the same type spellings on both
  backends (#1563).
"""

import datetime
import decimal

import numpy as np
import pytest

import datajoint as dj


@pytest.fixture
def Device(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_json_path_{backend}", connection=connection_by_backend)

    @schema
    class Device(dj.Manual):
        definition = """
        name : varchar(32)
        ---
        s : json
        """

    Device.insert(
        [
            dict(
                name="a",
                s=dict(vendor="Acme", ch=64, gain=2.5, cal=True, n=7, made="2024-01-02 03:04:05", pos=[1, 2]),
            ),
            dict(
                name="b",
                s=dict(vendor="Other", ch=16.0, gain=0.1, cal=False, n=3, made="2025-06-07 08:09:10", pos=[3, 4]),
            ),
            # Strings where the other rows hold numbers
            dict(name="c", s=dict(vendor="64", ch="sixty-four")),
        ],
        skip_duplicates=True,
    )
    yield Device
    schema.drop()


def names(query):
    return sorted(query.to_arrays("name"))


@pytest.mark.parametrize(
    "restriction, expected",
    [
        # strings compare as text, as before
        ({"s.vendor": "Acme"}, ["a"]),
        ({"s.ch": "64"}, ["a"]),
        ({"s.cal": "true"}, ["a"]),
        # bool
        ({"s.cal": True}, ["a"]),
        ({"s.cal": False}, ["b"]),
        ({"s.cal": np.bool_(True)}, ["a"]),
        # int, including against a stored float and a stored non-numeric string
        ({"s.ch": 64}, ["a"]),
        ({"s.ch": 16}, ["b"]),
        ({"s.ch": np.int64(64)}, ["a"]),
        ({"s.n": 7}, ["a"]),
        # float, including against a stored int
        ({"s.ch": 64.0}, ["a"]),
        ({"s.gain": 2.5}, ["a"]),
        ({"s.gain": 0.1}, ["b"]),
        ({"s.gain": np.float64(2.5)}, ["a"]),
        ({"s.gain": decimal.Decimal("2.50")}, ["a"]),
        # a number or bool never matches a JSON string, a missing key never matches
        ({"s.vendor": 64}, []),
        ({"s.vendor": True}, []),
        ({"s.missing": 1}, []),
        # array element
        ({"s.pos[1]": 4}, ["b"]),
        # combined with a regular attribute
        ({"name": "a", "s.cal": True, "s.ch": 64}, ["a"]),
        ({"name": "b", "s.cal": True}, []),
    ],
)
def test_restrict_by_value(Device, restriction, expected):
    assert names(Device & restriction) == expected


def test_anti_restrict(Device):
    # Row c holds a string at s.ch, which is unequal to 64 rather than an error
    assert names(Device - {"s.ch": 64}) == ["b", "c"]
    # Row c has no s.cal, so it matches neither the restriction nor its complement
    assert names(Device - {"s.cal": True}) == ["b"]


@pytest.mark.parametrize(
    "restriction, expected",
    [
        ({"s.n:int": 7}, ["a"]),
        ({"s.n:signed": 3}, ["b"]),
        ({"s.n:unsigned": 7}, ["a"]),
        ({"s.cal:boolean": True}, ["a"]),
        ({"s.cal:bool": False}, ["b"]),
        ({"s.made:datetime": datetime.datetime(2024, 1, 2, 3, 4, 5)}, ["a"]),
        ({"s.made:timestamp": datetime.datetime(2025, 6, 7, 8, 9, 10)}, ["b"]),
    ],
)
def test_restrict_by_annotated_path(Device, restriction, expected):
    assert names(Device & restriction) == expected


@pytest.mark.parametrize(
    "annotation, expected",
    [
        ("int", [7, 3]),
        ("integer", [7, 3]),
        ("bigint", [7, 3]),
        ("smallint", [7, 3]),
        ("signed", [7, 3]),
        ("unsigned", [7, 3]),
    ],
)
def test_project_integer_annotation(Device, annotation, expected):
    rows = (Device & "name in ('a', 'b')").proj(n=f"s.n:{annotation}").to_dicts(order_by="name")
    assert [row["n"] for row in rows] == expected
    assert all(isinstance(row["n"], (int, np.integer)) for row in rows)


@pytest.mark.parametrize("annotation", ["decimal(4,1)", "numeric(4,1)"])
def test_project_decimal_annotation(Device, annotation):
    rows = Device.proj(g=f"s.gain:{annotation}").to_dicts(order_by="name")
    assert [row["g"] for row in rows[:2]] == [decimal.Decimal("2.5"), decimal.Decimal("0.1")]


@pytest.mark.parametrize("annotation", ["double", "double precision"])
def test_project_double_annotation(Device, annotation):
    rows = (Device & "name in ('a', 'b')").proj(g=f"s.gain:{annotation}").to_dicts(order_by="name")
    assert [row["g"] for row in rows] == [2.5, 0.1]


@pytest.mark.parametrize("annotation", ["varchar(16)", "text"])
def test_project_string_annotation(Device, annotation):
    rows = Device.proj(v=f"s.vendor:{annotation}").to_dicts(order_by="name")
    assert [row["v"] for row in rows] == ["Acme", "Other", "64"]


@pytest.mark.parametrize("annotation", ["bool", "boolean"])
def test_project_boolean_annotation(Device, annotation):
    rows = (Device & "name in ('a', 'b')").proj(c=f"s.cal:{annotation}").to_dicts(order_by="name")
    # PostgreSQL returns True/False, MySQL 1/0
    assert [row["c"] for row in rows] == [True, False]


@pytest.mark.parametrize("annotation", ["datetime", "timestamp"])
def test_project_datetime_annotation(Device, annotation):
    rows = (Device & "name in ('a', 'b')").proj(m=f"s.made:{annotation}").to_dicts(order_by="name")
    assert [row["m"] for row in rows] == [datetime.datetime(2024, 1, 2, 3, 4, 5), datetime.datetime(2025, 6, 7, 8, 9, 10)]
