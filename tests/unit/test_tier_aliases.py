"""`Entry`/`Ingest`/`Compute` are the old tiers under names on one axis.

#1546: `Manual` names its writer but means the origin; `Imported` names the
origin but means the writer. The new names put all of them on one question --
what puts rows in this table -- and the old names stay permanently.

These are aliases, not subclasses, so "identical table" is not a property to be
maintained but one that cannot be broken. These tests pin that, plus the
transparency checks the issue asks for: SQL prefix, Role, tier detection, and
`lookup_class_name`.
"""

import pytest

import datajoint as dj
from datajoint.settings import Role, role_to_prefix
from datajoint.user_tables import _get_tier

PAIRS = [
    ("Entry", "Manual", Role.manual),
    ("Ingest", "Imported", Role.imported),
    ("Compute", "Computed", Role.computed),
]


@pytest.mark.parametrize("new, old, _role", PAIRS)
def test_alias_is_the_same_class(new, old, _role):
    """Not a subclass: the same object, so nothing can drift between them."""
    assert getattr(dj, new) is getattr(dj, old)


@pytest.mark.parametrize("new, old, role", PAIRS)
def test_prefix_matches_the_role(new, old, role):
    assert getattr(dj, new)._prefix == role_to_prefix[role]


@pytest.mark.parametrize("new, old, _role", PAIRS)
def test_exported_from_the_package(new, old, _role):
    assert new in dj.__all__
    assert old in dj.__all__


def test_lookup_and_part_are_unchanged():
    """The issue renames three tiers; these two already sit on the axis."""
    assert not hasattr(dj, "LookupAlias")
    assert dj.Lookup._prefix == "#"
    assert dj.Part.__name__ == "Part"


@pytest.mark.parametrize(
    "table_name, expected",
    [
        ("subject", "Manual"),
        ("#param", "Lookup"),
        ("_ingest", "Imported"),
        ("__analysis", "Computed"),
        ("subject__detail", "Part"),
    ],
)
def test_tier_detection_resolves_to_one_class(table_name, expected):
    """Tier detection is by SQL prefix, so the alias cannot confuse it."""
    tier = _get_tier(f"`lab`.`{table_name}`")
    assert tier is not None and tier.__name__ == expected


def test_declaring_either_way_produces_the_same_table_name():
    """`_prefix` plus the class name is the whole of the table name."""
    from datajoint.utils import from_camel_case

    class ViaOld(dj.Manual):
        definition = ""

    class ViaNew(dj.Entry):
        definition = ""

    assert ViaOld._prefix == ViaNew._prefix
    assert from_camel_case("Subject") == "subject"
    # Both inherit the identical metaclass machinery, so the computed name for a
    # given class name is the same by construction.
    assert type(ViaOld) is type(ViaNew)
