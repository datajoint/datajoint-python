"""The `context` argument is additive: a codec written before it keeps working.

DataJoint passes `context` only to codecs whose signature declares it, and keeps
populating the pre-2.3.4 underscore keys in `key`. These tests pin both halves of
that contract, since breaking either would break every third-party codec.
"""

import inspect
import warnings

import pytest

from datajoint.builtin_codecs.schema import SchemaCodec
from datajoint.codecs import Codec


class LegacyCodec(SchemaCodec):
    """A codec written against the pre-2.3.4 signature. Must not need changing."""

    name = "legacy_ctx_test"

    def encode(self, value, *, key=None, store_name=None):
        schema, table, field, pk = self._extract_context(key)
        return {"schema": schema, "table": table, "field": field, "pk": pk, "config": self._codec_config(key)}

    def decode(self, stored, *, key=None):
        return self._codec_config(key)

    def validate(self, value):
        pass


class ModernCodec(SchemaCodec):
    """A codec that declares `context`."""

    name = "modern_ctx_test"

    def encode(self, value, *, key=None, context=None, store_name=None):
        schema, table, field, pk = self._extract_context(key, context)
        return {"schema": schema, "table": table, "field": field, "pk": pk, "config": self._codec_config(key, context)}

    def decode(self, stored, *, key=None, context=None):
        return self._codec_config(key, context)

    def validate(self, value):
        pass


LEGACY_KEY = {"_schema": "lab", "_table": "subject", "_field": "data", "_config": "CFG", "subject_id": 7}
CONTEXT = {"schema": "lab", "table": "subject", "field": "data", "config": "CFG"}


def test_legacy_codec_signature_is_not_asked_for_context():
    """The framework introspects before passing, so the old signature is safe."""
    assert "context" not in inspect.signature(LegacyCodec().encode).parameters
    assert "context" in inspect.signature(ModernCodec().encode).parameters


def test_legacy_codec_still_resolves_everything_from_key():
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        out = LegacyCodec().encode(b"x", key=dict(LEGACY_KEY))
    assert out == {"schema": "lab", "table": "subject", "field": "data", "pk": {"subject_id": 7}, "config": "CFG"}


def test_modern_codec_resolves_everything_from_context():
    out = ModernCodec().encode(b"x", key={"subject_id": 7}, context=CONTEXT)
    assert out == {"schema": "lab", "table": "subject", "field": "data", "pk": {"subject_id": 7}, "config": "CFG"}


def test_context_takes_precedence_over_the_legacy_keys():
    """Both are populated during the deprecation window; context wins."""
    out = ModernCodec().encode(b"x", key=dict(LEGACY_KEY), context=CONTEXT)
    assert out["config"] == "CFG"
    assert out["pk"] == {"subject_id": 7}


def test_reading_context_from_key_warns():
    """The deprecated path works and says so."""
    with pytest.warns(DeprecationWarning, match="1550"):
        LegacyCodec()._extract_context(dict(LEGACY_KEY))


def test_no_warning_when_context_is_supplied():
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        ModernCodec()._extract_context(dict(LEGACY_KEY), CONTEXT)


def test_no_warning_for_a_plain_primary_key():
    """A codec that never needed context must not be nagged."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        assert LegacyCodec()._extract_context({"subject_id": 7}) == ("unknown", "unknown", "data", {"subject_id": 7})


@pytest.mark.parametrize(
    "key, context, expected",
    [
        ({"_config": "FROM_KEY"}, None, "FROM_KEY"),
        (None, {"config": "FROM_CONTEXT"}, "FROM_CONTEXT"),
        ({"_config": "FROM_KEY"}, {"config": "FROM_CONTEXT"}, "FROM_CONTEXT"),
        ({"_config": "FROM_KEY"}, {}, "FROM_KEY"),
        (None, None, None),
    ],
)
def test_codec_config_resolution(key, context, expected):
    assert Codec._codec_config(key, context) == expected


def test_builtin_codecs_declare_context():
    """Every built-in accepts the new argument, so none falls back silently."""
    from datajoint.builtin_codecs import attach, filepath, hash as hash_codec, npy, object as object_codec

    for cls in (
        attach.AttachCodec,
        filepath.FilepathCodec,
        hash_codec.HashCodec,
        npy.NpyCodec,
        object_codec.ObjectCodec,
    ):
        for method in ("encode", "decode"):
            params = inspect.signature(getattr(cls, method)).parameters
            assert "context" in params, f"{cls.__name__}.{method} does not accept context"


def test_capability_check_is_cached_per_class():
    """inspect.signature costs more than a small encode; it must not run per row."""
    from datajoint.codecs import _accepts_kwarg

    _accepts_kwarg.cache_clear()
    assert _accepts_kwarg(ModernCodec.encode, "context") is True
    assert _accepts_kwarg(LegacyCodec.encode, "context") is False
    before = _accepts_kwarg.cache_info()
    for _ in range(1000):
        _accepts_kwarg(ModernCodec.encode, "context")
        _accepts_kwarg(LegacyCodec.encode, "context")
    after = _accepts_kwarg.cache_info()
    assert after.misses == before.misses, "signature was re-inspected after caching"
    assert after.hits - before.hits == 2000


def test_builtin_codecs_read_the_schema_from_context():
    """A built-in must not reach past `context` for something `context` carries.

    `HashCodec.encode` read the schema from `key["_schema"]` while reading the
    config from `context`. A caller that passes `context` with a key holding only
    primary-key values -- which is every caller once 2.4 removes the underscore
    keys -- stored its content under `unknown/`, silently and in the wrong place.
    """
    from datajoint.builtin_codecs.hash import HashCodec

    stored = {}

    def fake_put_hash(value, *, schema_name, store_name=None, config=None):
        stored["schema_name"] = schema_name
        return {"hash": "h", "path": f"{schema_name}/h", "schema": schema_name, "store": store_name, "size": 0}

    import datajoint.hash_registry as hash_registry

    original = hash_registry.put_hash
    hash_registry.put_hash = fake_put_hash
    try:
        HashCodec().encode(b"x", key={"subject_id": 1}, context={"schema": "lab", "config": None})
        assert stored["schema_name"] == "lab", "context['schema'] must win over the key"

        # and the pre-2.3.4 caller is unaffected
        stored.clear()
        HashCodec().encode(b"x", key={"_schema": "legacy_lab", "subject_id": 1})
        assert stored["schema_name"] == "legacy_lab"
    finally:
        hash_registry.put_hash = original
