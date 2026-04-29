"""Unit tests for the leading-underscore guard in attribute declarations.

Regression coverage for issue #1433: declarations like ``_hidden: bool``
previously failed with a cryptic ``pyparsing.ParseException``. The framework
intentionally does not support user-defined hidden attributes — those names
are reserved for platform-managed columns (e.g. ``_job_start_time``,
``_singleton``) which DataJoint injects programmatically after parsing.

This test ensures the user gets a clear ``DataJointError`` pointing to the
right alternative, not a parser-internals error.
"""

import pytest

from datajoint.declare import attribute_parser, compile_attribute
from datajoint.errors import DataJointError


@pytest.mark.parametrize(
    "line",
    [
        "_hidden: bool",
        "_params_hash: varchar(32)",
        "  _leading_whitespace: int32",
    ],
)
def test_compile_attribute_rejects_leading_underscore(line):
    """The leading-underscore guard fires before the parser, so adapter is unused."""
    with pytest.raises(DataJointError, match="reserved for platform-managed"):
        compile_attribute(line, in_key=False, foreign_key_sql=[], context={}, adapter=None)


def test_parser_still_rejects_leading_underscore():
    """Parser regex itself remains strict; the helpful error fires before the parser."""
    import pyparsing as pp

    with pytest.raises(pp.ParseException):
        attribute_parser.parse_string("_hidden: bool#", parse_all=True)


def test_parser_still_accepts_plain_names():
    match = attribute_parser.parse_string("name: varchar(40)#", parse_all=True)
    assert match["name"] == "name"


def test_parser_rejects_digit_start():
    """Numeric leading char remains invalid (preserved behavior)."""
    import pyparsing as pp

    with pytest.raises(pp.ParseException):
        attribute_parser.parse_string("1bad: int32#", parse_all=True)
