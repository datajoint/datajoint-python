"""Unit tests for condition.py - Top class and merge logic."""

import pytest
from datajoint.condition import Top


class TestTopMerge:
    """Tests for Top.merge() method."""

    def test_merge_inherits_order(self):
        """When other.order_by is None, ordering is inherited."""
        top1 = Top(limit=10, order_by="score desc")
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.order_by == ["score desc"]
        assert merged.limit == 5
        assert merged.offset == 0

    def test_merge_limits_take_min(self):
        """Merged limit is minimum of both."""
        top1 = Top(limit=10, order_by="id")
        top2 = Top(limit=3, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 3

        # Reverse order
        top1 = Top(limit=3, order_by="id")
        top2 = Top(limit=10, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 3

    def test_merge_none_limit_preserved(self):
        """None limit (unlimited) is handled correctly."""
        top1 = Top(limit=None, order_by="id")
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 5

        top1 = Top(limit=5, order_by="id")
        top2 = Top(limit=None, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 5

        top1 = Top(limit=None, order_by="id")
        top2 = Top(limit=None, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit is None

    def test_merge_offsets_add(self):
        """Offsets are added together."""
        top1 = Top(limit=10, order_by="id", offset=5)
        top2 = Top(limit=3, order_by=None, offset=2)
        merged = top1.merge(top2)
        assert merged.offset == 7

    def test_merge_preserves_existing_order(self):
        """Merged Top preserves first Top's ordering."""
        top1 = Top(limit=10, order_by=["col1 desc", "col2 asc"])
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.order_by == ["col1 desc", "col2 asc"]


class TestTopValidation:
    """Tests for Top validation."""

    def test_order_by_none_allowed(self):
        """order_by=None is valid (means inherit)."""
        top = Top(limit=5, order_by=None)
        assert top.order_by is None

    def test_order_by_string_converted_to_list(self):
        """Single string order_by is converted to list."""
        top = Top(order_by="id desc")
        assert top.order_by == ["id desc"]

    def test_order_by_list_preserved(self):
        """List order_by is preserved."""
        top = Top(order_by=["col1", "col2 desc"])
        assert top.order_by == ["col1", "col2 desc"]

    def test_invalid_limit_type_raises(self):
        """Non-integer limit raises TypeError."""
        with pytest.raises(TypeError):
            Top(limit="5")

    def test_invalid_order_by_type_raises(self):
        """Non-string order_by raises TypeError."""
        with pytest.raises(TypeError):
            Top(order_by=123)

    def test_invalid_offset_type_raises(self):
        """Non-integer offset raises TypeError."""
        with pytest.raises(TypeError):
            Top(offset="1")


# ---------------------------------------------------------------------------
# Rendering of values in mapping restrictions
# ---------------------------------------------------------------------------

import datetime  # noqa: E402
import decimal  # noqa: E402
import importlib.util  # noqa: E402
import json  # noqa: E402
import uuid  # noqa: E402
from types import SimpleNamespace  # noqa: E402

import numpy as np  # noqa: E402

from datajoint.adapters import get_adapter  # noqa: E402
from datajoint.condition import make_condition, translate_attribute  # noqa: E402
from datajoint.errors import DataJointError  # noqa: E402

BACKENDS = [
    "mysql",
    pytest.param(
        "postgresql",
        marks=pytest.mark.skipif(importlib.util.find_spec("psycopg2") is None, reason="psycopg2 not installed"),
    ),
]

HOSTILE_STRINGS = [
    "' OR 1=1 --",
    "x' OR '1'='1",
    "\\' OR 1=1 -- ",
    "\\",
    "a\\'b",
    "''",
    'it\'s "quoted"',
    "50% off",
    "%s",
    "line\nbreak; DROP TABLE x; --",
    "\x00\x1a",
]


def _fake_expression(backend, names=("name", "car", "uid")):
    """Stand-in for a query expression: an adapter and a heading."""
    attributes = {n: SimpleNamespace(uuid=(n == "uid")) for n in names}
    heading = type("Heading", (), {"names": list(names), "__getitem__": lambda self, k: attributes[k]})()
    return SimpleNamespace(connection=SimpleNamespace(adapter=get_adapter(backend)), heading=heading)


def _condition(backend, restriction):
    return make_condition(_fake_expression(backend), restriction, set())


def _read_literal(backend, sql):
    """
    Read one quoted string literal at the start of ``sql`` using the backend's rules
    (MySQL with backslash escapes; PostgreSQL with standard_conforming_strings on).

    Returns the literal's value and the text that follows it.
    """
    assert sql[0] == "'", sql
    out, i = [], 1
    while i < len(sql):
        c = sql[i]
        if c == "\\" and backend == "mysql":
            nxt = sql[i + 1]
            out.append({"n": "\n", "r": "\r", "0": "\x00", "Z": "\x1a", "t": "\t", "b": "\b"}.get(nxt, nxt))
            i += 2
        elif c == "'":
            if sql[i + 1 : i + 2] == "'":
                out.append("'")
                i += 2
            else:
                return "".join(out), sql[i + 1 :]
        else:
            out.append(c)
            i += 1
    raise AssertionError(f"unterminated literal in {sql!r}")


def _literal_value(backend, condition, lhs):
    """Value of the single literal compared with ``lhs`` in a one-term condition."""
    sql = condition % ()  # the driver's parameter substitution turns %% into %
    prefix = f" ({lhs}="
    assert sql.startswith(prefix), sql
    value, rest = _read_literal(backend, sql[len(prefix) :])
    assert rest == ")", f"text after the literal: {rest!r}"
    return value


def _quoted(backend, name):
    return get_adapter(backend).quote_identifier(name)


def _json_lhs(backend, path):
    return translate_attribute(f"car.{path}", get_adapter(backend))[1]


@pytest.mark.parametrize("backend", BACKENDS)
class TestRestrictionLiterals:
    """Values in mapping restrictions are rendered as single, complete literals."""

    @pytest.mark.parametrize("value", HOSTILE_STRINGS)
    def test_string_value(self, backend, value):
        cond = _condition(backend, {"name": value})
        expected = value if backend == "mysql" else value.replace("\\", "\\\\")
        assert _literal_value(backend, cond, _quoted(backend, "name")) == expected

    @pytest.mark.parametrize("value", HOSTILE_STRINGS)
    def test_json_path_string_value(self, backend, value):
        cond = _condition(backend, {"car.name": value})
        expected = value if backend == "mysql" else value.replace("\\", "\\\\")
        assert _literal_value(backend, cond, _json_lhs(backend, "name")) == expected

    @pytest.mark.parametrize("value", HOSTILE_STRINGS)
    def test_list_value(self, backend, value):
        items = [value, 1, 'a"b', "c'd"]
        cond = _condition(backend, {"car.tags": items})
        assert _literal_value(backend, cond, _json_lhs(backend, "tags")) == str(items)

    @pytest.mark.parametrize("value", HOSTILE_STRINGS)
    def test_json_dict_value(self, backend, value):
        obj = {"side": value, value: True, "nested": {"k": [value]}}
        cond = _condition(backend, {"car.headlights[1]": obj})
        assert _literal_value(backend, cond, _json_lhs(backend, "headlights[1]")) == json.dumps(obj)

    @pytest.mark.parametrize(
        "key",
        [
            "car.na'me",
            'car.na"me',
            "car.x`) OR 1=1 -- ",
            'car.x") OR 1=1 -- ',
            "car.a\\b",
            "car.name'] OR 1=1 --",
            "car.name OR 1=1",
            "car.",
        ],
    )
    def test_invalid_json_key_rejected(self, backend, key):
        with pytest.raises(DataJointError):
            _condition(backend, {key: 1})

    @pytest.mark.parametrize(
        "type_spec",
        [
            "unsigned) OR (1",
            "char(20)) OR (1",
            "unsigned) OR sleep(5",
            "signed or 1",
            "text, 1",
            "decimal(4, 1) or 1",
            "nonsense",
        ],
    )
    def test_invalid_json_type_rejected(self, backend, type_spec):
        with pytest.raises(DataJointError):
            _condition(backend, {f"car.length:{type_spec}": 1})

    @pytest.mark.parametrize(
        "type_spec", ["unsigned", "signed", "float", "double", "decimal(4, 1)", "decimal(10,2)", "char(20)", "DATETIME"]
    )
    def test_valid_json_type_accepted(self, backend, type_spec):
        cond = _condition(backend, {f"car.length:{type_spec}": 1})
        assert cond.endswith("=1)")

    @pytest.mark.parametrize("value", [b"x' OR 1=1 --", object(), {"a": 1}, (1, 2), np.datetime64("2020-01-01"), 1 + 2j])
    def test_unsupported_value_type_rejected(self, backend, value):
        with pytest.raises(DataJointError):
            _condition(backend, {"name": value})

    def test_negated_hostile_value(self, backend):
        from datajoint.condition import Not

        cond = make_condition(_fake_expression(backend), Not({"name": "' OR 1=1 --"}), set())
        assert cond.startswith("NOT ")
        assert _literal_value(backend, cond[4:], _quoted(backend, "name")) == "' OR 1=1 --"


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "restriction, expected",
    [
        ({"name": "alpha"}, " ({name}='alpha')"),
        ({"name": 5}, " ({name}=5)"),
        ({"name": -2.5}, " ({name}=-2.5)"),
        ({"name": 1e-20}, " ({name}=1e-20)"),
        ({"name": True}, " ({name}=True)"),
        ({"name": np.int64(7)}, " ({name}=7)"),
        ({"name": np.float64(0.25)}, " ({name}=0.25)"),
        ({"name": np.bool_(False)}, " ({name}=False)"),
        ({"name": None}, " ({name} IS NULL)"),
        ({"name": decimal.Decimal("1.50")}, " ({name}='1.50')"),
        ({"name": datetime.date(2020, 1, 2)}, " ({name}='2020-01-02')"),
        ({"name": datetime.datetime(2020, 1, 2, 3, 4, 5)}, " ({name}='2020-01-02 03:04:05')"),
        ({"name": datetime.time(3, 4, 5)}, " ({name}='03:04:05')"),
        ({"car.tire_pressure": [34, 30, 27, 32]}, " ({tire_pressure}='[34, 30, 27, 32]')"),
        ({"uid": uuid.UUID(int=255)}, " ({uid}=X'000000000000000000000000000000ff')"),
    ],
)
def test_ordinary_values_render_as_before(backend, restriction, expected):
    """Values that do not need escaping are rendered exactly as in previous releases."""
    adapter = get_adapter(backend)
    names = {
        "name": adapter.quote_identifier("name"),
        "uid": adapter.quote_identifier("uid"),
        "tire_pressure": _json_lhs(backend, "tire_pressure"),
    }
    assert _condition(backend, restriction) == expected.format(**names)


@pytest.mark.parametrize("backend", BACKENDS)
def test_json_path_expression_unchanged(backend):
    """JSON paths render as in previous releases."""
    adapter = get_adapter(backend)
    if backend == "mysql":
        assert translate_attribute("car.headlights[0].side:char(20)", adapter)[1] == (
            "json_value(`car`, _utf8mb4'$.headlights[0].side' returning char(20))"
        )
    else:
        assert translate_attribute("car.headlights[0].side", adapter)[1] == (
            "jsonb_extract_path_text(\"car\", 'headlights', '0', 'side')"
        )
