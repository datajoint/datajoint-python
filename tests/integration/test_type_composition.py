"""
Tests for type composition (type chain encoding/decoding).

This tests the <xblob> → <content> → json composition pattern
and similar type chains.
"""

from datajoint.attribute_type import (
    AttributeType,
    _type_registry,
    register_type,
    resolve_dtype,
)


class TestTypeChainResolution:
    """Tests for resolving type chains."""

    def setup_method(self):
        """Clear test types from registry before each test."""
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def test_single_type_chain(self):
        """Test resolving a single-type chain."""

        @register_type
        class TestSingle(AttributeType):
            type_name = "test_single"
            dtype = "varchar(100)"

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

        @register_type
        class TestInner(AttributeType):
            type_name = "test_inner"
            dtype = "longblob"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class TestOuter(AttributeType):
            type_name = "test_outer"
            dtype = "<test_inner>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        final_dtype, chain, store = resolve_dtype("<test_outer>")

        assert final_dtype == "longblob"
        assert len(chain) == 2
        assert chain[0].type_name == "test_outer"
        assert chain[1].type_name == "test_inner"

    def test_three_type_chain(self):
        """Test resolving a three-type chain."""

        @register_type
        class TestBase(AttributeType):
            type_name = "test_base"
            dtype = "json"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class TestMiddle(AttributeType):
            type_name = "test_middle"
            dtype = "<test_base>"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class TestTop(AttributeType):
            type_name = "test_top"
            dtype = "<test_middle>"

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
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def teardown_method(self):
        """Clean up test types after each test."""
        for name in list(_type_registry.keys()):
            if name.startswith("test_"):
                del _type_registry[name]

    def test_encode_order(self):
        """Test that encode is applied outer → inner."""
        encode_order = []

        @register_type
        class TestInnerEnc(AttributeType):
            type_name = "test_inner_enc"
            dtype = "longblob"

            def encode(self, value, *, key=None, store_name=None):
                encode_order.append("inner")
                return value + b"_inner"

            def decode(self, stored, *, key=None):
                return stored

        @register_type
        class TestOuterEnc(AttributeType):
            type_name = "test_outer_enc"
            dtype = "<test_inner_enc>"

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

        @register_type
        class TestInnerDec(AttributeType):
            type_name = "test_inner_dec"
            dtype = "longblob"

            def encode(self, value, *, key=None, store_name=None):
                return value

            def decode(self, stored, *, key=None):
                decode_order.append("inner")
                return stored.replace(b"_inner", b"")

        @register_type
        class TestOuterDec(AttributeType):
            type_name = "test_outer_dec"
            dtype = "<test_inner_dec>"

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

        @register_type
        class TestInnerRt(AttributeType):
            type_name = "test_inner_rt"
            dtype = "longblob"

            def encode(self, value, *, key=None, store_name=None):
                # Compress (just add prefix for testing)
                return b"COMPRESSED:" + value

            def decode(self, stored, *, key=None):
                # Decompress
                return stored.replace(b"COMPRESSED:", b"")

        @register_type
        class TestOuterRt(AttributeType):
            type_name = "test_outer_rt"
            dtype = "<test_inner_rt>"

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

    def test_xblob_resolves_to_json(self):
        """Test that <xblob> → <content> → json."""
        final_dtype, chain, _ = resolve_dtype("<xblob>")

        assert final_dtype == "json"
        assert len(chain) == 2
        assert chain[0].type_name == "xblob"
        assert chain[1].type_name == "content"

    def test_xattach_resolves_to_json(self):
        """Test that <xattach> → <content> → json."""
        final_dtype, chain, _ = resolve_dtype("<xattach>")

        assert final_dtype == "json"
        assert len(chain) == 2
        assert chain[0].type_name == "xattach"
        assert chain[1].type_name == "content"

    def test_djblob_resolves_to_longblob(self):
        """Test that <djblob> → longblob (no chain)."""
        final_dtype, chain, _ = resolve_dtype("<djblob>")

        assert final_dtype == "longblob"
        assert len(chain) == 1
        assert chain[0].type_name == "djblob"

    def test_content_resolves_to_json(self):
        """Test that <content> → json."""
        final_dtype, chain, _ = resolve_dtype("<content>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "content"

    def test_object_resolves_to_json(self):
        """Test that <object> → json."""
        final_dtype, chain, _ = resolve_dtype("<object>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "object"

    def test_attach_resolves_to_longblob(self):
        """Test that <attach> → longblob."""
        final_dtype, chain, _ = resolve_dtype("<attach>")

        assert final_dtype == "longblob"
        assert len(chain) == 1
        assert chain[0].type_name == "attach"

    def test_filepath_resolves_to_json(self):
        """Test that <filepath> → json."""
        final_dtype, chain, _ = resolve_dtype("<filepath>")

        assert final_dtype == "json"
        assert len(chain) == 1
        assert chain[0].type_name == "filepath"


class TestStoreNameParsing:
    """Tests for store name parsing in type specs."""

    def test_type_with_store(self):
        """Test parsing type with store name."""
        final_dtype, chain, store = resolve_dtype("<xblob@mystore>")

        assert final_dtype == "json"
        assert store == "mystore"

    def test_type_without_store(self):
        """Test parsing type without store name."""
        final_dtype, chain, store = resolve_dtype("<xblob>")

        assert store is None

    def test_filepath_with_store(self):
        """Test parsing filepath with store name."""
        final_dtype, chain, store = resolve_dtype("<filepath@s3store>")

        assert final_dtype == "json"
        assert store == "s3store"
