"""Compressed blobs expand only up to the size their header declares."""

import tracemalloc
import zlib

import numpy as np
import pytest

import datajoint.blob as blob_module
from datajoint.blob import len_u64, pack, serialize_lookup, unpack
from datajoint.builtin_codecs.blob import BlobCodec
from datajoint.errors import DataJointError
from datajoint.settings import Config

PREFIX = b"ZL123\0"


def compressed_blob(payload, declared_size=None):
    size = len(payload) if declared_size is None else declared_size
    return PREFIX + np.uint64(size).tobytes() + zlib.compress(payload)


def test_round_trip_compressed():
    value = np.arange(10_000, dtype=np.float64)
    blob = pack(value)
    assert blob.startswith(PREFIX)
    np.testing.assert_array_equal(unpack(blob), value)


def test_round_trip_uncompressed():
    assert unpack(pack({"a": [1, 2, 3]}, compress=False)) == {"a": [1, 2, 3]}


def test_payload_larger_than_declared_size_is_rejected_without_expanding_it():
    # a short declared size in front of ~100 MB of zeros that compress to ~100 kB
    inner = pack(b"x", compress=False)
    blob = compressed_blob(inner + bytes(100_000_000), declared_size=len(inner))
    tracemalloc.start()
    try:
        with pytest.raises(DataJointError, match="size mismatch"):
            unpack(blob)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 10_000_000


def test_payload_smaller_than_declared_size_is_rejected():
    payload = pack(np.arange(1000), compress=False)
    with pytest.raises(DataJointError, match="size mismatch"):
        unpack(compressed_blob(payload, declared_size=len(payload) + 1))


def test_zero_declared_size_is_rejected():
    payload = pack(np.arange(1000), compress=False)
    with pytest.raises(DataJointError, match="size mismatch"):
        unpack(compressed_blob(payload, declared_size=0))


def test_oversized_declared_size_is_rejected():
    payload = pack(np.arange(1000), compress=False)
    with pytest.raises(DataJointError, match="size mismatch"):
        unpack(compressed_blob(payload, declared_size=2**64 - 1))


def test_truncated_stream_is_rejected():
    payload = pack(np.arange(1000), compress=False)
    blob = compressed_blob(payload)
    with pytest.raises(DataJointError):
        unpack(blob[:-10])


def test_corrupt_stream_is_rejected():
    payload = pack(np.arange(1000), compress=False)
    with pytest.raises(DataJointError, match="Corrupt"):
        unpack(PREFIX + len_u64(payload) + b"not zlib data")


# --------------------------------------------------------------------------- #
# Opt-in cap on the recorded uncompressed size: blob.max_decompressed_size
# --------------------------------------------------------------------------- #
def high_ratio_blob():
    """A blob that records its true size: 50 MB of zeros, about 50 kB compressed."""
    value = np.zeros(50_000_000, dtype=np.uint8)
    blob = pack(value)
    assert blob.startswith(PREFIX) and len(blob) < 1_000_000
    return value, blob, int(np.frombuffer(blob[len(PREFIX) : len(PREFIX) + 8], dtype=np.uint64)[0])


def test_true_size_high_ratio_blob_decodes_without_a_limit():
    value, blob, _ = high_ratio_blob()
    np.testing.assert_array_equal(unpack(blob), value)


def test_limit_refuses_blob_before_decompressing_it():
    _, blob, size = high_ratio_blob()
    tracemalloc.start()
    try:
        with pytest.raises(DataJointError, match="max_decompressed_size"):
            unpack(blob, max_decompressed_size=size - 1)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert peak < 5_000_000


def test_limit_at_the_recorded_size_decodes():
    value, blob, size = high_ratio_blob()
    np.testing.assert_array_equal(unpack(blob, max_decompressed_size=size), value)


def test_limit_does_not_apply_to_uncompressed_blob():
    blob = pack(np.arange(1000), compress=False)
    np.testing.assert_array_equal(unpack(blob, max_decompressed_size=10), np.arange(1000))


def test_setting_is_off_by_default(monkeypatch):
    monkeypatch.delenv("DJ_BLOB_MAX_DECOMPRESSED_SIZE", raising=False)
    assert Config().blob.max_decompressed_size is None


def test_setting_from_environment_and_validation(monkeypatch):
    monkeypatch.setenv("DJ_BLOB_MAX_DECOMPRESSED_SIZE", "1000")
    cfg = Config()
    assert cfg["blob.max_decompressed_size"] == 1000
    with pytest.raises(ValueError):
        cfg.blob.max_decompressed_size = -1


def test_codec_applies_the_setting_from_its_config():
    value, blob, size = high_ratio_blob()
    cfg = Config()
    np.testing.assert_array_equal(BlobCodec().decode(blob, context={"config": cfg}), value)
    cfg.blob.max_decompressed_size = size - 1
    with pytest.raises(DataJointError, match="max_decompressed_size"):
        BlobCodec().decode(blob, context={"config": cfg})


def _mym_double_array(values, dims_dtype):
    """A MATLAB mYm blob holding a column of doubles, with dimensions of type ``dims_dtype``."""
    values = np.asarray(values, dtype=np.float64)
    type_id = serialize_lookup[np.dtype("float64")]["type_id"]
    return (
        b"mYm\0"
        + b"A"
        + np.array(2, dtype=dims_dtype).tobytes()  # number of dimensions
        + np.array([1, len(values)], dtype=dims_dtype).tobytes()  # shape
        + np.array([type_id, 0], dtype=np.uint32).tobytes()  # type, not complex
        + values.tobytes()
    )


@pytest.mark.parametrize("use_32bit_dims", [False, True], ids=["64bit", "32bit"])
def test_matlab_blob_through_size_checks(monkeypatch, use_32bit_dims):
    """mYm blobs from MATLAB: the recorded size is read with the configured width and checked."""
    dims_dtype = np.uint32 if use_32bit_dims else np.uint64
    monkeypatch.setattr(blob_module, "use_32bit_dims", use_32bit_dims)
    values = np.arange(100_000, dtype=np.float64) % 7
    payload = _mym_double_array(values, dims_dtype)
    blob = PREFIX + np.array(len(payload), dtype=dims_dtype).tobytes() + zlib.compress(payload)
    np.testing.assert_array_equal(unpack(blob).ravel(), values)
    np.testing.assert_array_equal(unpack(blob, max_decompressed_size=len(payload)).ravel(), values)
    with pytest.raises(DataJointError, match="max_decompressed_size"):
        unpack(blob, max_decompressed_size=len(payload) - 1)
    for declared in (len(payload) - 1, len(payload) + 1):
        wrong = PREFIX + np.array(declared, dtype=dims_dtype).tobytes() + zlib.compress(payload)
        with pytest.raises(DataJointError, match="size mismatch"):
            unpack(wrong)
