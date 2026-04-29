"""Unit tests for hidden attribute names (leading underscore) in table declarations.

Regression coverage for issue #1433: the declaration parser previously rejected
attribute names starting with ``_``, even though hidden-attribute semantics
(``is_hidden = name.startswith("_")``) were already implemented in ``Heading``.
"""

import pytest

from datajoint.declare import attribute_parser


@pytest.mark.parametrize(
    "line",
    [
        "_hidden: bool",
        "_params_hash: varchar(32)",
        "_job_start_time=null: datetime(3)",
        "_a: int",
    ],
)
def test_parser_accepts_leading_underscore(line):
    match = attribute_parser.parse_string(line + "#", parse_all=True)
    assert match["name"].startswith("_")


def test_parser_still_accepts_plain_names():
    match = attribute_parser.parse_string("name: varchar(40)#", parse_all=True)
    assert match["name"] == "name"


def test_parser_rejects_digit_start():
    """Numeric leading char remains invalid (preserved behavior)."""
    import pyparsing as pp

    with pytest.raises(pp.ParseException):
        attribute_parser.parse_string("1bad: int#", parse_all=True)


def test_parser_extracts_hidden_name_for_is_hidden_dispatch():
    """The parsed name is what Heading uses to set is_hidden via name.startswith('_')."""
    match = attribute_parser.parse_string("_secret: int#", parse_all=True)
    name = match["name"]
    assert name.startswith("_")
