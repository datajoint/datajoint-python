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

    @pytest.mark.parametrize(
        "first, second, limit, offset",
        [
            (Top(limit=2, order_by="id"), Top(limit=None, order_by=None, offset=1), 1, 1),
            (Top(limit=2, order_by="id"), Top(limit=5, order_by=None, offset=1), 1, 1),
            (Top(limit=2, order_by="id", offset=3), Top(limit=1, order_by=None, offset=1), 1, 4),
            (Top(limit=2, order_by="id"), Top(limit=1, order_by=None, offset=5), 0, 5),
            (Top(limit=None, order_by="id"), Top(limit=None, order_by=None, offset=2), 999999999999, 2),
        ],
    )
    def test_merge_offset_stays_within_first_limit(self, first, second, limit, offset):
        """The second Top skips rows of the first Top's window and never reaches past its limit."""
        merged = first.merge(second)
        assert (merged.limit, merged.offset) == (limit, offset)

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

SPECIAL_CHARACTER_STRINGS = [
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


# JSON return types accepted by previous releases on MySQL or PostgreSQL
VALID_JSON_TYPES = [
    # MySQL JSON_VALUE ... RETURNING
    "unsigned",
    "signed",
    "UNSIGNED INTEGER",
    "float",
    "double",
    "decimal(4, 1)",
    "decimal(10,2)",
    "char(20)",
    "char(20) character set utf8mb4",
    "DATETIME",
    "datetime(6)",
    "date",
    "time",
    "year",
    "json",
    # PostgreSQL casts
    "integer",
    "int",
    "int4",
    "bigint",
    "smallint",
    "numeric",
    "numeric(10,2)",
    "double precision",
    "real",
    "text",
    "varchar(32)",
    "character varying(32)",
    "boolean",
    "bool",
    "jsonb",
    "timestamp",
    "timestamptz",
    "timestamp with time zone",
    "timestamp(3) without time zone",
    "uuid",
]


def _fake_expression(backend, names=("name", "car", "uid")):
    """Stand-in for a query expression: an adapter and a heading."""
    attributes = {n: SimpleNamespace(uuid=(n == "uid")) for n in names}
    heading = type("Heading", (), {"names": list(names), "__getitem__": lambda self, k: attributes[k]})()
    return SimpleNamespace(connection=SimpleNamespace(adapter=get_adapter(backend)), heading=heading)


def _condition(backend, restriction):
    return make_condition(_fake_expression(backend), restriction, set())


def _read_literal(backend, sql, backslash_escapes=None):
    """
    Read one quoted string literal at the start of ``sql`` using the backend's rules
    (MySQL with backslash escapes; PostgreSQL with standard_conforming_strings on,
    where backslashes are escapes only in an E'...' literal).

    ``backslash_escapes=False`` reads a MySQL literal as under NO_BACKSLASH_ESCAPES;
    ``backslash_escapes=True`` reads a PostgreSQL literal as with standard_conforming_strings off.

    Returns the literal's value and the text that follows it.
    """
    if backslash_escapes is None:
        backslash_escapes = backend == "mysql"
    if backend == "postgresql" and sql.startswith("E'"):
        sql, backslash_escapes = sql[1:], True
    assert sql[0] == "'", sql
    out, i = [], 1
    while i < len(sql):
        c = sql[i]
        if c == "\\" and backslash_escapes:
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


def _literal_value(backend, condition, lhs, backslash_escapes=None):
    """Value of the single literal compared with ``lhs`` in a one-term condition."""
    sql = condition % ()  # the driver's parameter substitution turns %% into %
    prefix = f" ({lhs}="
    assert sql.startswith(prefix), sql
    value, rest = _read_literal(backend, sql[len(prefix) :], backslash_escapes)
    assert rest == ")", f"text after the literal: {rest!r}"
    return value


def _quoted(backend, name):
    return get_adapter(backend).quote_identifier(name)


def _json_lhs(backend, path):
    return translate_attribute(f"car.{path}", get_adapter(backend))[1]


@pytest.mark.parametrize("backend", BACKENDS)
class TestRestrictionLiterals:
    """Values in mapping restrictions are rendered as single, complete literals."""

    @pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
    def test_string_value(self, backend, value):
        cond = _condition(backend, {"name": value})
        expected = value if backend == "mysql" else value.replace("\\", "\\\\")
        assert _literal_value(backend, cond, _quoted(backend, "name")) == expected

    @pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
    def test_json_path_string_value(self, backend, value):
        cond = _condition(backend, {"car.name": value})
        expected = value if backend == "mysql" else value.replace("\\", "\\\\")
        assert _literal_value(backend, cond, _json_lhs(backend, "name")) == expected

    @pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
    def test_list_value(self, backend, value):
        items = [value, 1, 'a"b', "c'd"]
        cond = _condition(backend, {"car.tags": items})
        assert _literal_value(backend, cond, _json_lhs(backend, "tags")) == str(items)

    @pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
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

    @pytest.mark.parametrize("type_spec", VALID_JSON_TYPES)
    def test_valid_json_type_accepted(self, backend, type_spec):
        cond = _condition(backend, {f"car.length:{type_spec}": 1})
        assert cond.endswith("=1)")

    @pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), np.float64("nan"), np.float32("inf")])
    def test_non_finite_number_rejected(self, backend, value):
        with pytest.raises(DataJointError, match="non-finite"):
            _condition(backend, {"name": value})

    def test_invalid_json_path_message(self, backend):
        with pytest.raises(DataJointError, match="Invalid JSON path `car.na'me`"):
            _condition(backend, {"car.na'me": 1})

    @pytest.mark.parametrize("value", [b"x' OR 1=1 --", object(), {"a": 1}, (1, 2), np.datetime64("2020-01-01"), 1 + 2j])
    def test_unsupported_value_type_rejected(self, backend, value):
        with pytest.raises(DataJointError):
            _condition(backend, {"name": value})

    def test_negated_value_with_quote_characters(self, backend):
        from datajoint.condition import Not

        cond = make_condition(_fake_expression(backend), Not({"name": "' OR 1=1 --"}), set())
        assert cond.startswith("NOT ")
        assert _literal_value(backend, cond[4:], _quoted(backend, "name")) == "' OR 1=1 --"


@pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
@pytest.mark.parametrize("key", ["name", "car.name", "car.tags", "car.headlights[1]"])
def test_mysql_literal_closed_without_backslash_escapes(key, value):
    """MySQL literals stay closed when read with NO_BACKSLASH_ESCAPES in the session's sql_mode."""
    restriction = {"car.tags": [value, 'a"b', "c'd"], "car.headlights[1]": {"side": value}}.get(key, value)
    lhs = _quoted("mysql", key) if key == "name" else _json_lhs("mysql", key.split(".", 1)[1])
    # _literal_value asserts that the literal's closing quote is followed only by ")"
    _literal_value("mysql", _condition("mysql", {key: restriction}), lhs, backslash_escapes=False)


@pytest.mark.skipif(importlib.util.find_spec("psycopg2") is None, reason="psycopg2 not installed")
@pytest.mark.parametrize("value", SPECIAL_CHARACTER_STRINGS)
def test_postgresql_list_value_without_standard_conforming_strings(value):
    """PostgreSQL list values read back exactly when standard_conforming_strings is off."""
    items = [value, 1, 'a"b', "c'd"]
    cond = _condition("postgresql", {"car.tags": items})
    assert _literal_value("postgresql", cond, _json_lhs("postgresql", "tags"), backslash_escapes=True) == str(items)


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


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("type_spec", VALID_JSON_TYPES)
def test_json_type_in_projection_and_index(backend, type_spec):
    """JSON return types accepted in restrictions are also accepted in projections and index declarations."""
    from datajoint.declare import compile_index

    adapter = get_adapter(backend)
    expr = translate_attribute(f"car.length:{type_spec}", adapter)[1]
    assert expr.startswith("json_value(" if backend == "mysql" else "(jsonb_extract_path_text(")
    index_sql = []
    compile_index(f"index(car.length:{type_spec})", index_sql, adapter)
    assert index_sql == [f"index (({expr}))"]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("type_spec", ["unsigned) OR (1", "nonsense"])
def test_invalid_json_type_in_projection_and_index(backend, type_spec):
    from datajoint.declare import compile_index

    adapter = get_adapter(backend)
    with pytest.raises(DataJointError):
        translate_attribute(f"car.length:{type_spec}", adapter)
    with pytest.raises(DataJointError):
        compile_index(f"index(car.length:{type_spec})", [], adapter)


# ---------------------------------------------------------------------------
# Column names in string restrictions: string literals are skipped
# ---------------------------------------------------------------------------

from datajoint.condition import extract_column_names, strip_literals  # noqa: E402

LITERAL_VALUES = [
    "a\\'b",  # a backslash right before a quote
    "x\\",  # ends in a backslash
    "\\\\",
    "\\",
    "'",
    "''",
    "it's",
    'say "hi"',
    'a\\"b',
    "and b or c",
    "$$ d $$",
    "%s",
]


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("value", LITERAL_VALUES)
def test_column_names_skip_quoted_values(backend, value):
    """A literal made by the backend's quote_string never yields column names."""
    literal = get_adapter(backend).quote_string(value)
    name = _quoted(backend, "name") if backend == "mysql" else "name"
    condition = f"{name} > {literal} AND age < 3 AND {literal} <> zip"
    assert extract_column_names(condition, backend) == {"name", "age", "zip"}


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("value", LITERAL_VALUES)
def test_column_names_skip_mapping_restriction_values(backend, value):
    """Literals in compiled mapping restrictions never yield column names."""
    for restriction in ({"name": value}, {"car.tags": [value, "c'd"]}, {"car.side": value}):
        condition = _condition(backend, restriction)
        assert extract_column_names(condition, backend) <= {"name", "car"}, condition


@pytest.mark.parametrize(
    "condition, expected",
    [
        # the value a\'b as written by MySQLAdapter.quote_string
        ("`name` > 'a\\\\''b'", {"name"}),
        ("`name` > 'x\\\\'", {"name"}),
        ("`name` > 'x\\\\' AND `age` < 3", {"name", "age"}),
        # a quote escaped by a backslash, and by doubling
        ("name = 'o\\'brien' AND age > 1", {"name", "age"}),
        ("name = 'o''brien' AND age > 1", {"name", "age"}),
        ("name = '' AND age > 1", {"name", "age"}),
        ("name = 'it''s' 'b' AND age > 1", {"name", "age"}),
        # double-quoted strings
        ('name = "say \\"hi\\" b" AND age > 1', {"name", "age"}),
        ('name = "say ""hi"" b" AND age > 1', {"name", "age"}),
        ('name = "it\'s" AND age > 1', {"name", "age"}),
        ("name = 'say \"hi\" b' AND age > 1", {"name", "age"}),
        # literal prefixes
        ("name = _utf8mb4'b c' AND age > 1", {"name", "age"}),
        ("uid = X'0a0b' AND age > 1", {"uid", "age"}),
        ("uid = x'0a0b' AND flag = b'01' AND label = N'c d'", {"uid", "flag", "label"}),
        # functions and reserved words
        ("concat(name, 'a b') LIKE 'c%' AND age IS NOT NULL", {"name", "age"}),
    ],
)
def test_column_names_mysql(condition, expected):
    assert extract_column_names(condition, "mysql") == expected
    assert extract_column_names(condition) == expected  # MySQL rules by default


@pytest.mark.parametrize(
    "condition, expected",
    [
        # with standard_conforming_strings on, a backslash is an ordinary character
        ("name > 'a\\''b'", {"name"}),
        ("name > 'x\\'", {"name"}),
        ("name > 'x\\' AND age < 3", {"name", "age"}),
        ("name > 'x\\\\' AND age < 3", {"name", "age"}),
        ("name = 'o''brien' AND age > 1", {"name", "age"}),
        # E'' literals read backslash escapes
        ("name = E'o\\'brien b' AND age > 1", {"name", "age"}),
        ("name = e'x\\\\' AND age > 1", {"name", "age"}),
        ("name = E'a\\\\''b' AND age > 1", {"name", "age"}),
        ("name = E'' AND age > 1", {"name", "age"}),
        # a column ending in e directly before a literal is not an E prefix
        ("name = 'e' AND age > 1", {"name", "age"}),
        # other prefixes and dollar quoting
        ("flag = B'01' AND label = U&'d\\0061' AND age > 1", {"flag", "label", "age"}),
        ("name = $$it's b$$ AND age > 1", {"name", "age"}),
        ("name = $q$a $$ b$q$ AND age > 1", {"name", "age"}),
        # double quotes delimit identifiers, which are skipped as before
        ("\"name\" = 'b' AND age > 1", {"age"}),
        ('"na""me" = 1 AND age > 1', {"age"}),
    ],
)
def test_column_names_postgresql(condition, expected):
    assert extract_column_names(condition, "postgresql") == expected


def test_column_names_unterminated_literal_keeps_names():
    """Names after a quote that is never closed are still reported."""
    assert extract_column_names("name = 'b AND age > 1", "mysql") == {"name", "b", "age"}
    assert extract_column_names("name = 'x\\' AND age > 1", "mysql") == {"name", "x", "age"}


def test_strip_literals_unknown_backend():
    with pytest.raises(DataJointError, match="Unknown database backend"):
        strip_literals("a = 'b'", "oracle")


@pytest.mark.parametrize(
    "backend, condition, expected",
    [
        ("mysql", "`name` > 'x\\\\' AND `id` IN (SELECT `id` FROM `t` WHERE `a` = 'q')", True),
        ("mysql", "`name` = 'select b'", False),
        ("mysql", '`name` = "select b"', False),
        ("mysql", "`select` = 1", False),
        ("postgresql", "name > 'x\\' AND id IN (SELECT id FROM t WHERE a = 'q')", True),
        ("postgresql", "name = E'a\\' select b'", False),
        ("postgresql", '"select" = 1', False),
    ],
)
def test_diagram_subquery_detection(backend, condition, expected):
    """A subquery after a literal that ends in a backslash is detected; text in literals is not a subquery."""
    from datajoint.diagram import _has_subquery

    assert _has_subquery(condition, backend) is expected
