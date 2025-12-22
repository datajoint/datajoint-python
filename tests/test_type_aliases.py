"""
Tests for numeric type aliases (float32, float64, int8, int16, int32, int64, etc.)
"""

import pytest

from datajoint.declare import TYPE_ALIASES, SPECIAL_TYPES, match_type

from .schema_type_aliases import TypeAliasTable, TypeAliasPrimaryKey, TypeAliasNullable


class TestTypeAliasPatterns:
    """Test that type alias patterns are correctly defined and matched."""

    @pytest.mark.parametrize(
        "alias,expected_category",
        [
            ("float32", "FLOAT32"),
            ("float64", "FLOAT64"),
            ("int64", "INT64"),
            ("uint64", "UINT64"),
            ("int32", "INT32"),
            ("uint32", "UINT32"),
            ("int16", "INT16"),
            ("uint16", "UINT16"),
            ("int8", "INT8"),
            ("uint8", "UINT8"),
            ("bool", "BOOL"),
            ("boolean", "BOOL"),
        ],
    )
    def test_type_alias_pattern_matching(self, alias, expected_category):
        """Test that type aliases are matched to correct categories."""
        category = match_type(alias)
        assert category == expected_category
        assert category in SPECIAL_TYPES
        assert category in TYPE_ALIASES

    @pytest.mark.parametrize(
        "alias,expected_mysql_type",
        [
            ("float32", "float"),
            ("float64", "double"),
            ("int64", "bigint"),
            ("uint64", "bigint unsigned"),
            ("int32", "int"),
            ("uint32", "int unsigned"),
            ("int16", "smallint"),
            ("uint16", "smallint unsigned"),
            ("int8", "tinyint"),
            ("uint8", "tinyint unsigned"),
            ("bool", "tinyint"),
            ("boolean", "tinyint"),
        ],
    )
    def test_type_alias_mysql_mapping(self, alias, expected_mysql_type):
        """Test that type aliases map to correct MySQL types."""
        category = match_type(alias)
        mysql_type = TYPE_ALIASES[category]
        assert mysql_type == expected_mysql_type

    @pytest.mark.parametrize(
        "native_type,expected_category",
        [
            ("int", "INTEGER"),
            ("bigint", "INTEGER"),
            ("smallint", "INTEGER"),
            ("tinyint", "INTEGER"),
            ("float", "FLOAT"),
            ("double", "FLOAT"),
        ],
    )
    def test_native_types_still_work(self, native_type, expected_category):
        """Test that native MySQL types still match correctly."""
        category = match_type(native_type)
        assert category == expected_category


class TestTypeAliasTableCreation:
    """Test table creation with type aliases."""

    def test_create_table_with_all_aliases(self, schema_type_aliases):
        """Test that tables with all type aliases can be created."""
        assert TypeAliasTable().full_table_name is not None

    def test_create_table_with_alias_primary_key(self, schema_type_aliases):
        """Test that tables with type aliases in primary key can be created."""
        assert TypeAliasPrimaryKey().full_table_name is not None

    def test_create_table_with_nullable_aliases(self, schema_type_aliases):
        """Test that tables with nullable type alias columns can be created."""
        assert TypeAliasNullable().full_table_name is not None


class TestTypeAliasHeading:
    """Test that headings correctly preserve type alias information."""

    def test_heading_preserves_type_aliases(self, schema_type_aliases):
        """Test that heading shows original type aliases."""
        heading = TypeAliasTable().heading
        heading_str = repr(heading)

        # Check that type aliases appear in the heading representation
        assert "float32" in heading_str
        assert "float64" in heading_str
        assert "int64" in heading_str
        assert "uint64" in heading_str
        assert "int32" in heading_str
        assert "uint32" in heading_str
        assert "int16" in heading_str
        assert "uint16" in heading_str
        assert "int8" in heading_str
        assert "uint8" in heading_str
        assert "bool" in heading_str


class TestTypeAliasInsertFetch:
    """Test inserting and fetching data with type aliases."""

    def test_insert_and_fetch(self, schema_type_aliases):
        """Test inserting and fetching values with type aliases."""
        table = TypeAliasTable()
        table.delete()

        test_data = dict(
            id=1,
            val_float32=3.14,
            val_float64=2.718281828,
            val_int64=9223372036854775807,  # max int64
            val_uint64=18446744073709551615,  # max uint64
            val_int32=2147483647,  # max int32
            val_uint32=4294967295,  # max uint32
            val_int16=32767,  # max int16
            val_uint16=65535,  # max uint16
            val_int8=127,  # max int8
            val_uint8=255,  # max uint8
            val_bool=1,  # boolean true
        )

        table.insert1(test_data)
        fetched = table.fetch1()

        assert fetched["id"] == test_data["id"]
        assert abs(fetched["val_float32"] - test_data["val_float32"]) < 0.001
        assert abs(fetched["val_float64"] - test_data["val_float64"]) < 1e-9
        assert fetched["val_int64"] == test_data["val_int64"]
        assert fetched["val_uint64"] == test_data["val_uint64"]
        assert fetched["val_int32"] == test_data["val_int32"]
        assert fetched["val_uint32"] == test_data["val_uint32"]
        assert fetched["val_int16"] == test_data["val_int16"]
        assert fetched["val_uint16"] == test_data["val_uint16"]
        assert fetched["val_int8"] == test_data["val_int8"]
        assert fetched["val_uint8"] == test_data["val_uint8"]
        assert fetched["val_bool"] == test_data["val_bool"]

    def test_insert_primary_key_with_aliases(self, schema_type_aliases):
        """Test using type aliases in primary key."""
        table = TypeAliasPrimaryKey()
        table.delete()

        table.insert1(dict(pk_int32=100, pk_uint16=200, value="test"))
        fetched = (table & dict(pk_int32=100, pk_uint16=200)).fetch1()

        assert fetched["pk_int32"] == 100
        assert fetched["pk_uint16"] == 200
        assert fetched["value"] == "test"

    def test_nullable_type_aliases(self, schema_type_aliases):
        """Test nullable columns with type aliases."""
        table = TypeAliasNullable()
        table.delete()

        # Insert with NULL values
        table.insert1(dict(id=1, nullable_float32=None, nullable_int64=None))
        fetched = table.fetch1()

        assert fetched["id"] == 1
        assert fetched["nullable_float32"] is None
        assert fetched["nullable_int64"] is None

        # Insert with actual values
        table.insert1(dict(id=2, nullable_float32=1.5, nullable_int64=999))
        fetched = (table & dict(id=2)).fetch1()

        assert fetched["nullable_float32"] == 1.5
        assert fetched["nullable_int64"] == 999
