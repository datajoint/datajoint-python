"""
Type-pattern consistency and strictness.

These tests cover the seam between the three places that name a type: the core type
table in ``declare``, the native passthrough patterns beside it, and the legacy mapping
in ``migrate``. Each of the bugs exercised here was a spelling that one of those three
accepted and another did not.
"""

import pytest

from datajoint.declare import CORE_TYPES, TYPE_PATTERN, match_type
from datajoint.errors import DataJointError
from datajoint.migrate import NATIVE_TO_CORE_TYPE


class TestCrossReferences:
    """Every type named in one module must be recognized by the others."""

    def test_migrate_core_types_are_declarable(self):
        """
        Values in NATIVE_TO_CORE_TYPE are written into column comments as ``:type:``
        markers and read back by Heading. A value that match_type rejects produces a
        marker that cannot be resolved on load.
        """
        for native, core in NATIVE_TO_CORE_TYPE.items():
            if core.startswith("<"):
                continue  # codecs are resolved by the codec registry, not match_type
            assert match_type(core), f"{native!r} maps to {core!r}, which match_type rejects"

    def test_migrate_native_types_are_recognized(self):
        """The keys are types read from a legacy database and must classify too."""
        for native in NATIVE_TO_CORE_TYPE:
            assert match_type(native)

    def test_core_type_names_are_declarable(self):
        """Each core type's own canonical spelling must classify as that core type."""
        canonical = {
            "float32": "float32",
            "float64": "float64",
            "int64": "int64",
            "int32": "int32",
            "int16": "int16",
            "int8": "int8",
            "bool": "bool",
            "uuid": "uuid",
            "json": "json",
            "bytes": "bytes",
            "date": "date",
            "datetime": "datetime",
            "char": "char(8)",
            "varchar": "varchar(8)",
            "enum": "enum('a','b')",
            "decimal": "decimal(6,2)",
        }
        assert set(canonical) == set(CORE_TYPES), "CORE_TYPES changed; update this map"
        for name, spelling in canonical.items():
            assert match_type(spelling) == name.upper()


class TestStrictness:
    """A declared type must be consumed in its entirety, not merely prefixed."""

    @pytest.mark.parametrize(
        "spelling",
        [
            "int24",  # plausible-looking, and formerly emitted by migrate.py
            "intbanana",
            "integerish",
            "tinyinteger",
            "uint32",  # unsigned core types are deliberately not provided
            "uint8",
            "float64x",
            "completely_invalid_type_xyz",
        ],
    )
    def test_near_miss_spellings_are_rejected(self, spelling):
        with pytest.raises(DataJointError, match="Unsupported attribute type"):
            match_type(spelling)

    def test_serial_alternative_stays_anchored(self):
        """`serial` is an alternative inside the INTEGER pattern; it must not leak."""
        assert match_type("serial") == "INTEGER"
        with pytest.raises(DataJointError):
            match_type("serialize")


class TestNumericAliases:
    """decimal/numeric/dec/fixed are one SQL type and must be treated alike."""

    def test_canonical_decimal_is_a_core_type(self):
        assert match_type("decimal(6,2)") == "DECIMAL"
        assert match_type("decimal(2, 2)") == "DECIMAL"

    @pytest.mark.parametrize(
        "spelling",
        [
            "decimal(2,2) unsigned",
            "decimal(2, 2) unsigned",
            "DECIMAL(2,2) UNSIGNED",
            "decimal(2,2) zerofill",
            "decimal(5)",
            "decimal",
            "dec(2,2)",
            "fixed(2,2)",
            "numeric(2,2)",
            "numeric(2,2) unsigned",
        ],
    )
    def test_modified_and_aliased_forms_pass_as_native(self, spelling):
        """
        These were valid in 0.14.x and must remain declarable. They pass as native
        types with a warning rather than as the core decimal type.
        """
        assert match_type(spelling) == "NUMERIC"

    def test_numeric_is_recognized_as_numeric_by_heading(self):
        """
        Heading derives its `numeric` flag from the same patterns. A decimal carrying a
        modifier must not fall through as neither numeric nor string.
        """
        for spelling in ("decimal(2,2) unsigned", "numeric(6,2)", "decimal(5)"):
            assert any(TYPE_PATTERN[t].match(spelling) for t in ("DECIMAL", "NUMERIC", "INTEGER", "FLOAT"))


class TestNativeSpellingsStillAccepted:
    """Guard against the strictness change rejecting legitimate native types."""

    @pytest.mark.parametrize(
        "spelling,category",
        [
            ("int", "INTEGER"),
            ("int(11)", "INTEGER"),
            ("int unsigned", "INTEGER"),
            ("int(11) unsigned", "INTEGER"),
            ("bigint unsigned auto_increment", "INTEGER"),
            ("integer", "INTEGER"),
            ("tinyint", "INTEGER"),
            ("smallint", "INTEGER"),
            ("double", "FLOAT"),
            ("float", "FLOAT"),
            ("real", "FLOAT"),
            ("double unsigned", "FLOAT"),
            ("varchar(255)", "VARCHAR"),
            ("char(4)", "CHAR"),
            ("timestamp", "TEMPORAL"),
            ("time", "TEMPORAL"),
            ("year", "TEMPORAL"),
            ("longblob", "NATIVE_BLOB"),
            ("mediumblob", "NATIVE_BLOB"),
            ("blob", "NATIVE_BLOB"),  # bare blob is a MySQL type too
            ("longtext", "NATIVE_TEXT"),
            ("text", "NATIVE_TEXT"),
            ("<blob>", "CODEC"),
            ("<attach>", "CODEC"),
            ("<blob@store>", "CODEC"),
        ],
    )
    def test_spelling_classifies(self, spelling, category):
        assert match_type(spelling) == category
