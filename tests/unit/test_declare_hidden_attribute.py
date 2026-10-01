"""Grammar permits a leading underscore; policy forbids a user declaring one.

Issue #1433 asked that `_hidden: bool` fail with a clear error rather than a
parser-internals exception. That is still the contract, but it now lives in a
different place.

The framework declares its own hidden columns -- `_job_start_time`,
`_singleton`, `_prov` -- in the same DataJoint notation a user writes, so the
grammar has to be able to spell them. What must stay forbidden is a *user*
declaring one, and that check is a branch of `prepare_declare`'s line loop,
which reaches it only after blanks, comments, `---`, foreign keys and indexes
have each been dispatched -- and which iterates nothing but the user's lines,
the framework's own being added after the loop.

Keeping the two separate is what lets platform columns go through one path while
`heading`'s visible/hidden split keeps meaning what it says.
"""

import pytest

from datajoint.adapters.mysql import MySQLAdapter
from datajoint.declare import attribute_parser, compile_attribute, prepare_declare
from datajoint.errors import DataJointError


def parse(definition, table_name=None, config=None):
    """Run a definition through the real entry point."""
    return prepare_declare(definition, {}, MySQLAdapter(), table_name=table_name, config=config)


def attribute_names(definition, table_name=None, config=None):
    _comment, _pk, attribute_sql, *_rest = parse(definition, table_name, config)
    return [sql.split()[0].strip("`") for sql in attribute_sql]


@pytest.mark.parametrize(
    "line",
    [
        "_hidden: bool",
        "_params_hash: varchar(32)",
        "  _leading_whitespace: int32",
    ],
)
def test_user_definition_rejects_leading_underscore(line):
    """The user-facing guarantee, checked on the path a user actually takes."""
    with pytest.raises(DataJointError, match="reserved for platform-managed"):
        parse(f"id : int32\n---\n{line}")


def test_rejection_message_is_unchanged():
    """#1433's point was the message, not only the failure."""
    with pytest.raises(DataJointError) as exc:
        parse("id : int32\n---\n_hidden : bool")
    message = str(exc.value)
    assert "starts with an underscore" in message
    assert "_job_start_time" in message and "_singleton" in message
    assert "proj()" in message


def test_indented_declarations_are_refused_too():
    """Definitions arrive indented from a class body; lines are stripped first."""
    with pytest.raises(DataJointError, match="starts with an underscore"):
        parse("        id : int32\n        ---\n        _indented : int32")


def test_ordinary_definitions_pass():
    """A name with an interior underscore is ordinary and must not trip the check."""
    assert attribute_names("subject_id : int32\n---\nspecies_name : varchar(32)") == [
        "subject_id",
        "species_name",
    ]


def test_comments_and_indexes_are_not_attribute_lines():
    assert attribute_names("# _not_an_attribute\nid : int32\n---\nvalue : int32\nindex (value)") == [
        "id",
        "value",
    ]


def test_grammar_accepts_what_policy_forbids():
    """The parser must spell a hidden name; refusing is policy, applied later."""
    parsed = attribute_parser.parse_string("_prov = null : json#", parse_all=True)
    assert parsed["name"] == "_prov"


@pytest.mark.parametrize(
    "line, expect_in_sql",
    [
        ("_job_start_time = null : datetime(3) # began", ":datetime(3):"),
        ("_prov = null : json # extrinsic provenance", ":json:"),
    ],
)
def test_framework_can_compile_its_own_columns(line, expect_in_sql):
    """The framework's own declarations go through the ordinary compile path."""
    name, sql, _store, comment = compile_attribute(line, in_key=False, foreign_key_sql=[], context={}, adapter=MySQLAdapter())
    assert name.startswith("_")
    assert expect_in_sql in sql
    assert expect_in_sql in comment


class _Config:
    """Minimal stand-in for the settings object the helper reads."""

    class jobs:
        add_job_metadata = True

    class provenance:
        capture = True


@pytest.mark.parametrize(
    "table_name, expected, unexpected",
    [
        ("subject", ["_prov"], ["_job_start_time"]),  # Entry
        ("__analysis", ["_job_start_time"], ["_prov"]),  # Computed
        ("_ingest", ["_job_start_time"], ["_prov"]),  # Imported
        ("#param", [], ["_prov", "_job_start_time"]),  # Lookup
        ("subject__detail", [], ["_prov", "_job_start_time"]),  # Part of an Entry
        ("~~analysis", [], ["_prov", "_job_start_time"]),  # job table
    ],
)
def test_platform_attributes_added_per_tier(table_name, expected, unexpected):
    """Each tier gets exactly the hidden columns it should, and no others."""
    names = attribute_names("id : int32\n---\nvalue : int32", table_name, _Config)
    for name in expected:
        assert name in names, f"{table_name} should receive {name}"
    for name in unexpected:
        assert name not in names, f"{table_name} should not receive {name}"


def test_platform_attributes_are_secondary():
    """They are nullable, so landing in the key section would be rejected.

    Compiling them after the parse is what settles this: a definition whose
    attributes are all primary key has no separator to sit behind, and does not
    need one.
    """
    _comment, primary_key, *_rest = parse("id : int32", "subject", _Config)
    assert primary_key == ["id"]
    assert attribute_names("id : int32", "subject", _Config) == ["id", "_prov"]


def test_no_tier_means_no_secondary_attributes():
    """`alter` passes no table name, and compares two definitions that declare none."""
    assert attribute_names("id : int32\n---\nvalue : int32") == ["id", "value"]


def test_the_check_never_judges_the_framework_s_own_lines():
    """The guard is inside the loop; platform attributes are added after it."""
    assert "_prov" in attribute_names("id : int32\n---\nvalue : int32", "subject", _Config)
