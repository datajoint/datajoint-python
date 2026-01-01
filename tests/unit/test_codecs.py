"""
Tests for the Codec system.
"""

import pytest

import datajoint as dj
from datajoint.codecs import (
    Codec,
    _codec_registry,
    get_codec,
    is_codec_registered,
    list_codecs,
    resolve_dtype,
    unregister_codec,
)
from datajoint.errors import DataJointError


class TestCodecRegistry:
    """Tests for the codec registry functionality."""

    def setup_method(self):
        """Clear any test codecs from registry before each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        """Clean up test codecs after each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_register_codec_auto(self):
        """Test auto-registration via __init_subclass__."""

        class TestCodec(Codec):
            name = "test_decorator"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_codec_registered("test_decorator")
        assert get_codec("test_decorator").name == "test_decorator"

    def test_register_codec_skip(self):
        """Test skipping registration with register=False."""

        class TestCodec(Codec, register=False):
            name = "test_skip"

            def get_dtype(self, is_external: bool) -> str:
                return "varchar(255)"

            def encode(self, value, *, key=None, store_name=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        assert not is_codec_registered("test_skip")

    def test_register_codec_idempotent(self):
        """Test that defining the same codec class twice is idempotent."""

        class TestCodec(Codec):
            name = "test_idempotent"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        # Redefine the same name should not raise (same class)
        assert is_codec_registered("test_idempotent")

    def test_register_duplicate_name_different_class(self):
        """Test that registering different classes with same name raises error."""

        class TestCodec1(Codec):
            name = "test_duplicate"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        with pytest.raises(DataJointError, match="already registered"):

            class TestCodec2(Codec):
                name = "test_duplicate"

                def get_dtype(self, is_external: bool) -> str:
                    return "varchar(100)"

                def encode(self, value, *, key=None, store_name=None):
                    return str(value)

                def decode(self, stored, *, key=None):
                    return stored

    def test_unregister_codec(self):
        """Test unregistering a codec."""

        class TestCodec(Codec):
            name = "test_unregister"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert is_codec_registered("test_unregister")
        unregister_codec("test_unregister")
        assert not is_codec_registered("test_unregister")

    def test_get_codec_not_found(self):
        """Test that getting an unregistered codec raises error."""
        with pytest.raises(DataJointError, match="Unknown codec"):
            get_codec("nonexistent_codec")

    def test_list_codecs(self):
        """Test listing registered codecs."""

        class TestCodec(Codec):
            name = "test_list"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        codecs = list_codecs()
        assert "test_list" in codecs
        assert codecs == sorted(codecs)  # Should be sorted

    def test_get_codec_strips_brackets(self):
        """Test that get_codec accepts names with or without angle brackets."""

        class TestCodec(Codec):
            name = "test_brackets"

            def get_dtype(self, is_external: bool) -> str:
                return "int32"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        assert get_codec("test_brackets") is get_codec("<test_brackets>")


class TestCodecValidation:
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

        class TestCodec(Codec):
            name = "test_validate_default"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        t = get_codec("test_validate_default")
        # Default validate should not raise for any value
        t.validate(None)
        t.validate(42)
        t.validate("string")
        t.validate([1, 2, 3])

    def test_validate_custom(self):
        """Test custom validation logic."""

        class PositiveIntCodec(Codec):
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

        t = get_codec("test_positive_int")
        t.validate(42)  # Should pass

        with pytest.raises(TypeError):
            t.validate("not an int")

        with pytest.raises(ValueError):
            t.validate(-1)


class TestCodecChaining:
    """Tests for codec chaining (dtype referencing another codec)."""

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

        class TestCodec(Codec):
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
        assert chain[0].name == "test_resolve"
        assert store is None

    def test_resolve_chained_dtype(self):
        """Test resolving a chained dtype."""

        class InnerCodec(Codec):
            name = "test_inner"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class OuterCodec(Codec):
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
        assert chain[0].name == "test_outer"
        assert chain[1].name == "test_inner"
        assert store is None

    def test_circular_reference_detection(self):
        """Test that circular codec references are detected."""

        class CodecA(Codec):
            name = "test_circular_a"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_circular_b>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class CodecB(Codec):
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
        """Test that Codec and helpers are exported from datajoint."""
        assert hasattr(dj, "Codec")
        assert hasattr(dj, "get_codec")
        assert hasattr(dj, "list_codecs")


class TestBlobCodec:
    """Tests for the built-in BlobCodec."""

    def test_blob_is_registered(self):
        """Test that blob is automatically registered."""
        assert is_codec_registered("blob")

    def test_blob_properties(self):
        """Test BlobCodec properties."""
        blob_codec = get_codec("blob")
        assert blob_codec.name == "blob"
        assert blob_codec.get_dtype(is_external=False) == "bytes"
        assert blob_codec.get_dtype(is_external=True) == "<hash>"

    def test_blob_encode_decode_roundtrip(self):
        """Test that encode/decode is a proper roundtrip."""
        import numpy as np

        blob_codec = get_codec("blob")

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
            encoded = blob_codec.encode(original)
            assert isinstance(encoded, bytes)
            decoded = blob_codec.decode(encoded)
            if isinstance(original, np.ndarray):
                np.testing.assert_array_equal(decoded, original)
            else:
                assert decoded == original

    def test_blob_encode_produces_valid_blob_format(self):
        """Test that encoded data has valid blob protocol header."""
        blob_codec = get_codec("blob")
        encoded = blob_codec.encode({"test": "data"})

        # Should start with compression prefix or protocol header
        valid_prefixes = (b"ZL123\0", b"mYm\0", b"dj0\0")
        assert any(encoded.startswith(p) for p in valid_prefixes)

    def test_blob_in_list_codecs(self):
        """Test that blob appears in list_codecs."""
        codecs = list_codecs()
        assert "blob" in codecs

    def test_blob_handles_serialization(self):
        """Test that BlobCodec handles serialization internally.

        With the new design:
        - Plain bytes columns store/return raw bytes (no serialization)
        - <blob> handles pack/unpack in encode/decode
        """
        blob_codec = get_codec("blob")

        # BlobCodec.encode() should produce packed bytes
        data = {"key": "value"}
        encoded = blob_codec.encode(data)
        assert isinstance(encoded, bytes)

        # BlobCodec.decode() should unpack back to original
        decoded = blob_codec.decode(encoded)
        assert decoded == data
