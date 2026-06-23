"""
Unit tests for the Renderable Codec Protocol (#1458).

The Protocol is a structural-typing contract — codecs opt in by
implementing ``render_spark`` and consumers detect support via
``isinstance(codec, Renderable)``. These tests cover the detection
behavior, not specific rendering implementations (which live downstream).
"""

from __future__ import annotations

import datajoint as dj
from datajoint.rendering import Renderable


class _RenderableCodec:
    """A minimal codec-like object that opts into the protocol."""

    name = "fake_renderable"

    def render_spark(self, decoded, *, key=None):
        return list(decoded) if hasattr(decoded, "__iter__") else decoded


class _NonRenderableCodec:
    """A minimal codec-like object that does NOT opt into the protocol."""

    name = "fake_opaque"

    def encode(self, value, *, key=None, store_name=None):
        return bytes(value)

    def decode(self, stored, *, key=None):
        return stored


def test_renderable_protocol_detects_opt_in():
    """A class implementing ``render_spark`` is detected as Renderable."""
    assert isinstance(_RenderableCodec(), Renderable)


def test_renderable_protocol_rejects_non_opt_in():
    """A class without ``render_spark`` is not detected as Renderable."""
    assert not isinstance(_NonRenderableCodec(), Renderable)


def test_renderable_exported_at_top_level():
    """``dj.Renderable`` is accessible at the top level."""
    assert dj.Renderable is Renderable


def test_renderable_is_runtime_checkable():
    """The Protocol is decorated with @runtime_checkable (the test fixtures
    above rely on this)."""
    # Direct assertion: classes lacking runtime_checkable would raise TypeError
    # on isinstance(). The previous tests would error rather than fail.
    try:
        isinstance(object(), Renderable)
    except TypeError:
        raise AssertionError("Renderable must be @runtime_checkable")


def test_blob_codec_is_not_renderable():
    """The built-in <blob@> codec is intentionally non-renderable per the spec."""
    from datajoint.builtin_codecs.blob import BlobCodec

    assert not isinstance(BlobCodec(), Renderable)


def test_hash_codec_is_not_renderable():
    """The built-in <hash@> codec is intentionally non-renderable per the spec."""
    from datajoint.builtin_codecs.hash import HashCodec

    assert not isinstance(HashCodec(), Renderable)


def test_renderable_invocation_passes_through():
    """A codec implementing the method can be invoked and returns its result."""
    codec = _RenderableCodec()
    assert codec.render_spark([1, 2, 3]) == [1, 2, 3]
    assert codec.render_spark(42) == 42


def test_renderable_method_accepts_key_kwarg():
    """The method signature accepts the optional ``key`` keyword argument."""
    codec = _RenderableCodec()
    # Should not raise
    codec.render_spark([1, 2, 3], key={"some_pk": 1})


def test_subclass_with_render_spark_is_renderable():
    """A subclass of a non-renderable that adds the method becomes renderable."""

    class _OpaqueBase:
        name = "base"

        def encode(self, value, *, key=None, store_name=None):
            return b""

    class _TypedSubclass(_OpaqueBase):
        def render_spark(self, decoded, *, key=None):
            return decoded

    assert not isinstance(_OpaqueBase(), Renderable)
    assert isinstance(_TypedSubclass(), Renderable)
