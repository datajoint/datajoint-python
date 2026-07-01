"""
Unit tests for the SparkAdapter Codec Protocol (#1458).

The Protocol is a structural-typing contract — codecs opt in by
implementing ``to_spark`` and consumers detect support via
``isinstance(codec, SparkAdapter)``. These tests cover the detection
behavior, not specific rendering implementations (which live downstream).
"""

from __future__ import annotations

import datajoint as dj
from datajoint.spark import SparkAdapter


class _SparkAdapterCodec:
    """A minimal codec-like object that opts into the protocol."""

    name = "fake_spark_adapter"

    def to_spark(self, decoded, *, key=None):
        return list(decoded) if hasattr(decoded, "__iter__") else decoded


class _OpaqueCodec:
    """A minimal codec-like object that does NOT opt into the protocol."""

    name = "fake_opaque"

    def encode(self, value, *, key=None, store_name=None):
        return bytes(value)

    def decode(self, stored, *, key=None):
        return stored


def test_protocol_detects_opt_in():
    """A class implementing ``to_spark`` is detected as a SparkAdapter."""
    assert isinstance(_SparkAdapterCodec(), SparkAdapter)


def test_protocol_rejects_non_opt_in():
    """A class without ``to_spark`` is not detected as a SparkAdapter."""
    assert not isinstance(_OpaqueCodec(), SparkAdapter)


def test_protocol_exported_at_top_level():
    """``dj.SparkAdapter`` is accessible at the top level."""
    assert dj.SparkAdapter is SparkAdapter


def test_protocol_is_runtime_checkable():
    """The Protocol is decorated with @runtime_checkable (the test fixtures
    above rely on this)."""
    # Direct assertion: classes lacking runtime_checkable would raise TypeError
    # on isinstance(). The previous tests would error rather than fail.
    try:
        isinstance(object(), SparkAdapter)
    except TypeError:
        raise AssertionError("SparkAdapter must be @runtime_checkable")


def test_blob_codec_is_not_spark_adapter():
    """The built-in <blob@> codec is intentionally non-adapting per the spec."""
    from datajoint.builtin_codecs.blob import BlobCodec

    assert not isinstance(BlobCodec(), SparkAdapter)


def test_hash_codec_is_not_spark_adapter():
    """The built-in <hash@> codec is intentionally non-adapting per the spec."""
    from datajoint.builtin_codecs.hash import HashCodec

    assert not isinstance(HashCodec(), SparkAdapter)


def test_to_spark_invocation_passes_through():
    """A codec implementing the method can be invoked and returns its result."""
    codec = _SparkAdapterCodec()
    assert codec.to_spark([1, 2, 3]) == [1, 2, 3]
    assert codec.to_spark(42) == 42


def test_to_spark_method_accepts_key_kwarg():
    """The method signature accepts the optional ``key`` keyword argument."""
    codec = _SparkAdapterCodec()
    # Should not raise
    codec.to_spark([1, 2, 3], key={"some_pk": 1})


def test_subclass_adding_to_spark_becomes_adapter():
    """A subclass of an opaque codec that adds the method becomes a SparkAdapter."""

    class _OpaqueBase:
        name = "base"

        def encode(self, value, *, key=None, store_name=None):
            return b""

    class _TypedSubclass(_OpaqueBase):
        def to_spark(self, decoded, *, key=None):
            return decoded

    assert not isinstance(_OpaqueBase(), SparkAdapter)
    assert isinstance(_TypedSubclass(), SparkAdapter)
