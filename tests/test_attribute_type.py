"""
Tests for the new AttributeType system.
"""

import pytest

import datajoint as dj
from datajoint.attribute_type import (
    AttributeType,
    _type_registry,
    get_type,
    is_type_registered,
    list_types,
    register_type,
    resolve_dtype,
    unregister_type,
)
from datajoint.errors import DataJointError


class TestAttributeTypeRegistry:
    """Tests for the type registry functionality."""

    def setup_method(self):
        """Clear any test types from registry before each test."""
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def test_register_type_decorator(self):
        """Test registering a type using the decorator."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_decorator"
            dtype = "longblob"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_type_registered("test_decorator")
        assert get_type("test_decorator").type_name == "test_decorator"

    def test_register_type_direct(self):
        """Test registering a type by calling register_type directly."""

        class TestType(AttributeType):
            type_name = "test_direct"
            dtype = "varchar(255)"

            def encode(self, value, *, key=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        register_type(TestType)
        assert is_type_registered("test_direct")

    def test_register_type_idempotent(self):
        """Test that registering the same type twice is idempotent."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_idempotent"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        # Second registration should not raise
        register_type(TestType)
        assert is_type_registered("test_idempotent")

    def test_register_duplicate_name_different_class(self):
        """Test that registering different classes with same name raises error."""

        @register_type
        class TestType1(AttributeType):
            type_name = "test_duplicate"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TestType2(AttributeType):
            type_name = "test_duplicate"
            dtype = "varchar(100)"

            def encode(self, value, *, key=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        with pytest.raises(DataJointError, match="already registered"):
            register_type(TestType2)

    def test_unregister_type(self):
        """Test unregistering a type."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_unregister"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_type_registered("test_unregister")
        unregister_type("test_unregister")
        assert not is_type_registered("test_unregister")

    def test_get_type_not_found(self):
        """Test that getting an unregistered type raises error."""
        with pytest.raises(DataJointError, match="Unknown attribute type"):
            get_type("nonexistent_type")

    def test_list_types(self):
        """Test listing registered types."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_list"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        types = list_types()
        assert "test_list" in types
        assert types == sorted(types)  # Should be sorted

    def test_get_type_strips_brackets(self):
        """Test that get_type accepts names with or without angle brackets."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_brackets"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert get_type("test_brackets") is get_type("<test_brackets>")


class TestAttributeTypeValidation:
    """Tests for the validate method."""

    def setup_method(self):
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def teardown_method(self):
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def test_validate_called_default(self):
        """Test that default validate accepts any value."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_validate_default"
            dtype = "longblob"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        t = get_type("test_validate_default")
        # Default validate should not raise for any value
        t.validate(None)
        t.validate(42)
        t.validate("string")
        t.validate([1, 2, 3])

    def test_validate_custom(self):
        """Test custom validation logic."""

        @register_type
        class PositiveIntType(AttributeType):
            type_name = "test_positive_int"
            dtype = "int"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

            def validate(self, value):
                if not isinstance(value, int):
                    raise TypeError(f"Expected int, got {type(value).__name__}")
                if value < 0:
                    raise ValueError("Value must be positive")

        t = get_type("test_positive_int")
        t.validate(42)  # Should pass

        with pytest.raises(TypeError):
            t.validate("not an int")

        with pytest.raises(ValueError):
            t.validate(-1)


class TestTypeChaining:
    """Tests for type chaining (dtype referencing another custom type)."""

    def setup_method(self):
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def teardown_method(self):
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def test_resolve_native_dtype(self):
        """Test resolving a native dtype."""
        final_dtype, chain = resolve_dtype("longblob")
        assert final_dtype == "longblob"
        assert chain == []

    def test_resolve_custom_dtype(self):
        """Test resolving a custom dtype."""

        @register_type
        class TestType(AttributeType):
            type_name = "test_resolve"
            dtype = "varchar(100)"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain = resolve_dtype("<test_resolve>")
        assert final_dtype == "varchar(100)"
        assert len(chain) == 1
        assert chain[0].type_name == "test_resolve"

    def test_resolve_chained_dtype(self):
        """Test resolving a chained dtype."""

        @register_type
        class InnerType(AttributeType):
            type_name = "test_inner"
            dtype = "longblob"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class OuterType(AttributeType):
            type_name = "test_outer"
            dtype = "<test_inner>"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain = resolve_dtype("<test_outer>")
        assert final_dtype == "longblob"
        assert len(chain) == 2
        assert chain[0].type_name == "test_outer"
        assert chain[1].type_name == "test_inner"

    def test_circular_reference_detection(self):
        """Test that circular type references are detected."""

        @register_type
        class TypeA(AttributeType):
            type_name = "test_circular_a"
            dtype = "<test_circular_b>"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class TypeB(AttributeType):
            type_name = "test_circular_b"
            dtype = "<test_circular_a>"

            def encode(self, value, *, key=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        with pytest.raises(DataJointError, match="Circular type reference"):
            resolve_dtype("<test_circular_a>")


class TestExportsAndAPI:
    """Test that the public API is properly exported."""

    def test_exports_from_datajoint(self):
        """Test that AttributeType and helpers are exported from datajoint."""
        assert hasattr(dj, "AttributeType")
        assert hasattr(dj, "register_type")
        assert hasattr(dj, "list_types")


class TestDJBlobType:
    """Tests for the built-in DJBlobType."""

    def test_djblob_is_registered(self):
        """Test that djblob is automatically registered."""
        assert is_type_registered("djblob")

    def test_djblob_properties(self):
        """Test DJBlobType properties."""
        blob_type = get_type("djblob")
        assert blob_type.type_name == "djblob"
        assert blob_type.dtype == "longblob"

    def test_djblob_encode_decode_roundtrip(self):
        """Test that encode/decode is a proper roundtrip."""
        import numpy as np

        blob_type = get_type("djblob")

        # Test with various data types
        test_data = [
            {"key": "value", "number": 42},
            [1, 2, 3, 4, 5],
            np.array([1.0, 2.0, 3.0]),
            "simple string",
            (1, 2, 3),
            None,
        ]

        for original in test_data:
            encoded = blob_type.encode(original)
            assert isinstance(encoded, bytes)
            decoded = blob_type.decode(encoded)
            if isinstance(original, np.ndarray):
                np.testing.assert_array_equal(decoded, original)
            else:
                assert decoded == original

    def test_djblob_encode_produces_valid_blob_format(self):
        """Test that encoded data has valid blob protocol header."""
        blob_type = get_type("djblob")
        encoded = blob_type.encode({"test": "data"})

        # Should start with compression prefix or protocol header
        valid_prefixes = (b"ZL123\0", b"mYm\0", b"dj0\0")
        assert any(encoded.startswith(p) for p in valid_prefixes)

    def test_djblob_in_list_types(self):
        """Test that djblob appears in list_types."""
        types = list_types()
        assert "djblob" in types

    def test_djblob_handles_serialization(self):
        """Test that DJBlobType handles serialization internally.

        With the new design:
        - Plain longblob columns store/return raw bytes (no serialization)
        - <djblob> handles pack/unpack in encode/decode
        """
        blob_type = get_type("djblob")

        # DJBlobType.encode() should produce packed bytes
        data = {"key": "value"}
        encoded = blob_type.encode(data)
        assert isinstance(encoded, bytes)

        # DJBlobType.decode() should unpack back to original
        decoded = blob_type.decode(encoded)
        assert decoded == data
