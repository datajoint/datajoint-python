"""
Tests for type composition (type chain encoding/decoding).

This tests the <blob@> → <hash> → json composition pattern
and similar type chains.
"""

from datajoint.codecs import (
    AttributeType,
    _codec_registry,
    resolve_dtype,
)


class TestTypeChainResolution:
    """Tests for resolving type chains."""

    def setup_method(self):
        """Clear test types from registry before each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_single_type_chain(self):
        """Test resolving a single-type chain."""

        class TestSingle(AttributeType):
            name = "test_single"

            def get_dtype(self, is_external: bool) -> str:
                return "varchar(100)"

            def encode(self, value, *, key=None, store_name=None):
                return str(value)

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain, store = resolve_dtype("<test_single>")

        assert final_dtype == "varchar(100)"
        assert len(chain) == 1
        assert chain[0].type_name == "test_single"
        assert store is None

    def test_two_type_chain(self):
        """Test resolving a two-type chain."""

        class TestInner(AttributeType):
            name = "test_inner"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TestOuter(AttributeType):
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

    def test_three_type_chain(self):
        """Test resolving a three-type chain."""

        class TestBase(AttributeType):
            name = "test_base"

            def get_dtype(self, is_external: bool) -> str:
                return "json"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TestMiddle(AttributeType):
            name = "test_middle"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_base>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        class TestTop(AttributeType):
            name = "test_top"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_middle>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain, store = resolve_dtype("<test_top>")

        assert final_dtype == "json"
        assert len(chain) == 3
        assert chain[0].type_name == "test_top"
        assert chain[1].type_name == "test_middle"
        assert chain[2].type_name == "test_base"


class TestTypeChainEncodeDecode:
    """Tests for encode/decode through type chains."""

    def setup_method(self):
        """Clear test types from registry before each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_codec_registry.keys()):
            if name.startswith("test_"):
                del _codec_registry[name]

    def test_encode_order(self):
        """Test that encode is applied outer → inner."""
        encode_order = []

        class TestInnerEnc(AttributeType):
            name = "test_inner_enc"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                encode_order.append("inner")
                return value + b"_inner"

            def decode(self, stored, *, key=None):
                return stored

        class TestOuterEnc(AttributeType):
            name = "test_outer_enc"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_inner_enc>"

            def encode(self, value, *, key=None, store_name=None):
                encode_order.append("outer")
                return value + b"_outer"

            def decode(self, stored, *, key=None):
                return stored

        _, chain, _ = resolve_dtype("<test_outer_enc>")

        # Apply encode in order: outer first, then inner
        value = b"start"
        for attr_type in chain:
            value = attr_type.encode(value)

        assert encode_order == ["outer", "inner"]
        assert value == b"start_outer_inner"

    def test_decode_order(self):
        """Test that decode is applied inner → outer (reverse of encode)."""
        decode_order = []

        class TestInnerDec(AttributeType):
            name = "test_inner_dec"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                decode_order.append("inner")
                return stored.replace(b"_inner", b"")

        class TestOuterDec(AttributeType):
            name = "test_outer_dec"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_inner_dec>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                decode_order.append("outer")
                return stored.replace(b"_outer", b"")

        _, chain, _ = resolve_dtype("<test_outer_dec>")

        # Apply decode in reverse order: inner first, then outer
        value = b"start_outer_inner"
        for attr_type in reversed(chain):
            value = attr_type.decode(value)

        assert decode_order == ["inner", "outer"]
        assert value == b"start"

    def test_roundtrip(self):
        """Test encode/decode roundtrip through a type chain."""

        class TestInnerRt(AttributeType):
            name = "test_inner_rt"

            def get_dtype(self, is_external: bool) -> str:
                return "bytes"

            def encode(self, value, *, key=None, store_name=None):
                # Compress (just add prefix for testing)
                return b"COMPRESSED:" + value

            def decode(self, stored, *, key=None):
                # Decompress
                return stored.replace(b"COMPRESSED:", b"")

        class TestOuterRt(AttributeType):
            name = "test_outer_rt"

            def get_dtype(self, is_external: bool) -> str:
                return "<test_inner_rt>"

            def encode(self, value, *, key=None, store_name=None):
                # Serialize (just encode string for testing)
                return str(value).encode("utf-8")

            def decode(self, stored, *, key=None):
                # Deserialize
                return stored.decode("utf-8")

        _, chain, _ = resolve_dtype("<test_outer_rt>")

        # Original value
        original = "test data"

        # Encode: outer → inner
        encoded = original
        for attr_type in chain:
            encoded = attr_type.encode(encoded)

        assert encoded == b"COMPRESSED:test data"

        # Decode: inner → outer (reversed)
        decoded = encoded
        for attr_type in reversed(chain):
            decoded = attr_type.decode(decoded)

        assert decoded == original


class TestBuiltinTypeComposition:
    """Tests for built-in type composition."""

    def test_blob_internal_resolves_to_bytes(self):
        """Test that <blob> (internal) → bytes."""
        final_dtype, chain, _ = resolve_dtype("<blob>")

        assert final_dtype == "bytes"
        assert len(chain) == 1
        assert chain[0].type_name == "blob"

    def test_blob_external_resolves_to_json(self):
        """Test that <blob@store> → <hash> → json."""
        final_dtype, chain, store = resolve_dtype("<blob@store>")

        assert final_dtype == "json"
        assert len(chain) == 2
        assert chain[0].type_name == "blob"
        assert chain[1].type_name == "hash"
        assert store == "store"

    def test_attach_internal_resolves_to_bytes(self):
        """Test that <attach> (internal) → bytes."""
        final_dtype, chain, _ = resolve_dtype("<attach>")

        assert final_dtype == "bytes"
        assert len(chain) == 1
        assert chain[0].type_name == "attach"

    def test_attach_external_resolves_to_json(self):
        """Test that <attach@store> → <hash> → json."""
        final_dtype, chain, store = resolve_dtype("<attach@store>")

        assert final_dtype == "json"
        assert len(chain) == 2
        assert chain[0].type_name == "attach"
        assert chain[1].type_name == "hash"
        assert store == "store"

    def test_hash_external_resolves_to_json(self):
        """Test that <hash@store> → json (external only)."""
        final_dtype, chain, store = resolve_dtype("<hash@store>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "hash"
        assert store == "store"

    def test_object_external_resolves_to_json(self):
        """Test that <object@> → json (external only)."""
        final_dtype, chain, store = resolve_dtype("<object@store>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "object"
        assert store == "store"

    def test_filepath_external_resolves_to_json(self):
        """Test that <filepath@> → json (external only)."""
        final_dtype, chain, store = resolve_dtype("<filepath@store>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "filepath"
        assert store == "store"


class TestStoreNameParsing:
    """Tests for store name parsing in type specs."""

    def test_type_with_store(self):
        """Test parsing type with store name."""
        final_dtype, chain, store = resolve_dtype("<blob@mystore>")

        assert final_dtype == "json"
        assert store == "mystore"

    def test_type_without_store(self):
        """Test parsing type without store name."""
        final_dtype, chain, store = resolve_dtype("<blob>")

        assert store is None

    def test_filepath_with_store(self):
        """Test parsing filepath with store name."""
        final_dtype, chain, store = resolve_dtype("<filepath@s3store>")

        assert final_dtype == "json"
        assert store == "s3store"
