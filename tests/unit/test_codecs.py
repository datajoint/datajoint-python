"""
Tests for the Codec system.
"""

import pytest

import datajoint as dj
from datajoint.codecs import (
    AttributeType,
    _codec_registry,
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
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_register_type_auto(self):
        """Test auto-registration via __init_subclass__."""

        class TestType(AttributeType):
            name = "test_decorator"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_type_registered("test_decorator")
        assert get_type("test_decorator").type_name == "test_decorator"

    def test_register_type_direct(self):
        """Test registering a type by calling register_type directly."""

        class TestType(AttributeType, register=False):
            name = "test_direct"

            def get_dtype(self, is_external: bool) -> str:
                return "varchar(255)"

            def encode(self, value, *, key=None, store_name=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        register_type(TestType)
        assert is_type_registered("test_direct")

    def test_register_type_idempotent(self):
        """Test that registering the same type twice is idempotent."""

        class TestType(AttributeType):
            name = "test_idempotent"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        # Second registration should not raise
        register_type(TestType)
        assert is_type_registered("test_idempotent")

    def test_register_duplicate_name_different_class(self):
        """Test that registering different classes with same name raises error."""

        class TestType1(AttributeType):
            name = "test_duplicate"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TestType2(AttributeType, register=False):
            name = "test_duplicate"

            def get_dtype(self, is_external: bool) -> str:
                return "varchar(100)"

            def encode(self, value, *, key=None, store_name=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        with pytest.raises(DataJointError, match="already registered"):
            register_type(TestType2)

    def test_unregister_type(self):
        """Test unregistering a type."""

        class TestType(AttributeType):
            name = "test_unregister"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_type_registered("test_unregister")
        unregister_type("test_unregister")
        assert not is_type_registered("test_unregister")

    def test_get_type_not_found(self):
        """Test that getting an unregistered type raises error."""
        with pytest.raises(DataJointError, match="Unknown codec"):
            get_type("nonexistent_type")

    def test_list_types(self):
        """Test listing registered types."""

        class TestType(AttributeType):
            name = "test_list"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        types = list_types()
        assert "test_list" in types
        assert types == sorted(types)  # Should be sorted

    def test_get_type_strips_brackets(self):
        """Test that get_type accepts names with or without angle brackets."""

        class TestType(AttributeType):
            name = "test_brackets"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert get_type("test_brackets") is get_type("<test_brackets>")


class TestAttributeTypeValidation:
    """Tests for the validate method."""

    def setup_method(self):
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_validate_called_default(self):
        """Test that default validate accepts any value."""

        class TestType(AttributeType):
            name = "test_validate_default"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
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

        class PositiveIntType(AttributeType):
            name = "test_positive_int"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
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
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_resolve_native_dtype(self):
        """Test resolving a native dtype."""
        final_dtype, chain, store = resolve_dtype("bytes")
        assert final_dtype == "bytes"
        assert chain == []
        assert store is None

    def test_resolve_custom_dtype(self):
        """Test resolving a custom dtype."""

        class TestType(AttributeType):
            name = "test_resolve"

            def get_dtype(self, is_external: bool) -> str:
                return "varchar(100)"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain, store = resolve_dtype("<test_resolve>")
        assert final_dtype == "varchar(100)"
        assert len(chain) == 1
        assert chain[0].type_name == "test_resolve"
        assert store is None

    def test_resolve_chained_dtype(self):
        """Test resolving a chained dtype."""

        class InnerType(AttributeType):
            name = "test_inner"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class OuterType(AttributeType):
            name = "test_outer"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_inner>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain, store = resolve_dtype("<test_outer>")
        assert final_dtype == "bytes"
        assert len(chain) == 2
        assert chain[0].type_name == "test_outer"
        assert chain[1].type_name == "test_inner"
        assert store is None

    def test_circular_reference_detection(self):
        """Test that circular type references are detected."""

        class TypeA(AttributeType):
            name = "test_circular_a"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_circular_b>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TypeB(AttributeType):
            name = "test_circular_b"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_circular_a>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        with pytest.raises(DataJointError, match="Circular codec reference"):
            resolve_dtype("<test_circular_a>")


class TestExportsAndAPI:
    """Test that the public API is properly exported."""

    def test_exports_from_datajoint(self):
        """Test that AttributeType and helpers are exported from datajoint."""
        assert hasattr(dj, "AttributeType")
        assert hasattr(dj, "register_type")
        assert hasattr(dj, "list_types")


class TestBlobCodec:
    """Tests for the built-in BlobCodec."""

    def test_blob_is_registered(self):
        """Test that blob is automatically registered."""
        assert is_type_registered("blob")

    def test_blob_properties(self):
        """Test BlobCodec properties."""
        blob_type = get_type("blob")
        assert blob_type.type_name == "blob"
        assert blob_type.get_dtype(is_external=False) == "bytes"
        assert blob_type.get_dtype(is_external=True) == "<hash>"

    def test_blob_encode_decode_roundtrip(self):
        """Test that encode/decode is a proper roundtrip."""
        import numpy as np

        blob_type = get_type("blob")

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

    def test_blob_encode_produces_valid_blob_format(self):
        """Test that encoded data has valid blob protocol header."""
        blob_type = get_type("blob")
        encoded = blob_type.encode({"test": "data"})

        # Should start with compression prefix or protocol header
        valid_prefixes = (b"ZL123\0", b"mYm\0", b"dj0\0")
        assert any(encoded.startswith(p) for p in valid_prefixes)

    def test_blob_in_list_types(self):
        """Test that blob appears in list_types."""
        types = list_types()
        assert "blob" in types

    def test_blob_handles_serialization(self):
        """Test that BlobCodec handles serialization internally.

        With the new design:
        - Plain bytes columns store/return raw bytes (no serialization)
        - <blob> handles pack/unpack in encode/decode
        """
        blob_type = get_type("blob")

        # BlobCodec.encode() should produce packed bytes
        data = {"key": "value"}
        encoded = blob_type.encode(data)
        assert isinstance(encoded, bytes)

        # BlobCodec.decode() should unpack back to original
        decoded = blob_type.decode(encoded)
        assert decoded == data
