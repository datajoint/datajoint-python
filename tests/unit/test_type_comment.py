"""
Unit tests for declared types read back from column comments (#1711).

``declare`` records a core or codec type at the start of the column comment as
``:type:comment``. ``Heading`` reads it back, and ``Job`` writes it into the jobs
table definition, one attribute per line.
"""

from types import SimpleNamespace

import pytest

from datajoint.errors import DataJointError
from datajoint.heading import Heading, split_type_comment
from datajoint.jobs import Job


@pytest.mark.parametrize(
    "comment, expected",
    [
        (None, (None, "")),
        ("", (None, "")),
        ("plain comment", (None, "plain comment")),
        ("ratio 1:2 or 3:4", (None, "ratio 1:2 or 3:4")),
        (":int32:", ("int32", "")),
        (":int32:subject number", ("int32", "subject number")),
        (":uuid:", ("uuid", "")),
        (":float64:a: b: c", ("float64", "a: b: c")),
        (":varchar(255):name", ("varchar(255)", "name")),
        (":decimal(8, 3):", ("decimal(8, 3)", "")),
        (":datetime(3):", ("datetime(3)", "")),
        (":enum('a', 'b'):", ("enum('a', 'b')", "")),
        (":json:", ("json", "")),
        (":<blob>:", ("<blob>", "")),
        (":<blob@store>:data", ("<blob@store>", "data")),
        (":<attach>:", ("<attach>", "")),
    ],
)
def test_split_type_comment(comment, expected):
    assert split_type_comment(comment) == expected


@pytest.mark.parametrize(
    "comment",
    [
        ":int32\n-> Other:",
        ":<blob>\n-> Other:",
        ":int32\r\n-> Other:",
        ":int32\r-> Other:",
        ":uuid\n:",
        ":notatype:comment",
        ":int32 , other : int32:",
    ],
)
def test_comment_without_known_type_is_plain(comment):
    """A marker that is not a single-line core or codec type is kept as the comment."""
    assert split_type_comment(comment) == (None, comment)


def test_legacy_external_type_is_passed_on():
    """Pre-2.0 `external` types are passed on so that reading the heading reports them."""
    assert split_type_comment(":external-store:data") == ("external-store", "data")


def _attribute(name, sql_type, comment):
    """Heading attribute for an inherited primary-key column, as Heading builds it."""
    declared_type, comment = split_type_comment(comment)
    original_type = None
    if declared_type is not None and declared_type.startswith("<"):
        sql_type = declared_type
    elif declared_type is not None:
        original_type = declared_type
    return dict(
        name=name,
        type=sql_type,
        original_type=original_type,
        in_key=True,
        nullable=False,
        default=None,
        comment=comment,
        autoincrement=False,
        numeric=True,
        string=False,
        uuid=False,
        json=False,
        is_blob=False,
        is_hidden=False,
        codec=None,
        store=None,
        unsupported=False,
        attribute_expression=None,
        dtype=object,
        lineage=None,
    )


def _job(attributes):
    """A Job for a target table whose primary key is inherited from one parent."""
    heading = Heading(attributes)
    attr_map = {a["name"]: a["name"] for a in attributes}
    target = SimpleNamespace(
        full_table_name="`schema`.`result`",
        heading=heading,
        parents=lambda primary, foreign_key_info: [("`schema`.`item`", {"attr_map": attr_map})],
    )
    job = Job.__new__(Job)
    job._target = target
    job._connection = SimpleNamespace(dependencies=SimpleNamespace(load=lambda: None))
    return job


def _attribute_lines(definition):
    """Lines of a jobs table definition above the primary-key divider."""
    lines = [line.strip() for line in definition.strip().splitlines()]
    return [line for line in lines[: lines.index("---")] if not line.startswith("#")]


@pytest.mark.parametrize("comment", [":int32\n-> Other:", ":<blob>\n-> Other:", ":int32\r\n-> Other:"])
def test_line_break_in_comment_adds_no_line_to_jobs_definition(comment):
    job = _job([_attribute("item", "int", comment)])
    definition = job._generate_definition()
    assert _attribute_lines(definition) == ["item : int"]
    assert "Other" not in definition


def test_jobs_definition_keeps_declared_types():
    job = _job(
        [
            _attribute("subject_id", "int", ":int32:subject"),
            _attribute("item", "binary(16)", ":uuid:"),
            _attribute("label", "varchar(16)", ":varchar(16):"),
        ]
    )
    assert _attribute_lines(job._generate_definition()) == [
        "subject_id : int32",
        "item : uuid",
        "label : varchar(16)",
    ]


@pytest.mark.parametrize("field, comment", [("type", ""), ("original_type", ":int32:"), ("name", "")])
def test_jobs_definition_refuses_line_breaks(field, comment):
    """A name or type that spans lines is refused, naming the table and the attribute."""
    attribute = _attribute("item", "int", comment)
    attribute[field] = attribute[field] + "\n-> Other"
    job = _job([attribute])
    with pytest.raises(DataJointError, match=r"`schema`\.`result`.*line break") as error:
        job._generate_definition()
    assert repr(attribute["name"]) in str(error.value)
