"""Grammar permits a leading underscore; policy forbids a user declaring one.

Issue #1433 asked that `_hidden: bool` fail with a clear error rather than a
parser-internals exception. That is still the contract, but it now lives in a
different place.

The framework declares its own hidden columns -- `_job_start_time`,
`_singleton`, `_prov` -- in the same DataJoint notation a user writes, so the
grammar has to be able to spell them. What must stay forbidden is a *user*
declaring one, and that check sits where a user's definition enters:
`_reject_user_hidden_attributes`, called from `declare()` before the framework
appends anything of its own.

Keeping the two separate is what lets platform columns go through one path while
`heading`'s visible/hidden split keeps meaning what it says.
"""

import pytest

from datajoint.declare import (
    _append_platform_attributes,
    _reject_user_hidden_attributes,
    attribute_parser,
    compile_attribute,
)
from datajoint.errors import DataJointError


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
    definition = f"id : int32\n---\n{line}"
    with pytest.raises(DataJointError, match="reserved for platform-managed"):
        _reject_user_hidden_attributes(definition)


def test_rejection_message_is_unchanged():
    """#1433's point was the message, not only the failure."""
    with pytest.raises(DataJointError) as exc:
        _reject_user_hidden_attributes("id : int32\n---\n_hidden : bool")
    message = str(exc.value)
    assert "starts with an underscore" in message
    assert "_job_start_time" in message and "_singleton" in message
    assert "proj()" in message


def test_ordinary_definitions_pass():
    """A name with an interior underscore is ordinary and must not trip the check."""
    _reject_user_hidden_attributes("subject_id : int32\n---\nspecies_name : varchar(32)")


def test_foreign_keys_and_comments_are_not_attribute_lines():
    _reject_user_hidden_attributes("# _not_an_attribute\n-> Parent\n---\nvalue : int32")


def test_grammar_accepts_what_policy_forbids():
    """The parser must spell a hidden name; refusing is policy, applied earlier."""
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
    from datajoint.adapters.mysql import MySQLAdapter

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
def test_platform_attributes_appended_per_tier(table_name, expected, unexpected):
    """Each tier gets exactly the hidden columns it should, and no others."""
    augmented = _append_platform_attributes("id : int32\n---\nvalue : int32", table_name, _Config)
    for name in expected:
        assert name in augmented, f"{table_name} should receive {name}"
    for name in unexpected:
        assert name not in augmented, f"{table_name} should not receive {name}"


def test_appended_lines_survive_the_user_check():
    """The guard runs before appending, so the framework's lines are never judged."""
    definition = "id : int32\n---\nvalue : int32"
    _reject_user_hidden_attributes(definition)
    augmented = _append_platform_attributes(definition, "subject", _Config)
    assert "_prov" in augmented
    # And the augmented text would now fail the user check -- which is exactly why
    # the check runs first rather than over the final string.
    with pytest.raises(DataJointError):
        _reject_user_hidden_attributes(augmented)
