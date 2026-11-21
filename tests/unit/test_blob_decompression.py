"""Compressed blobs expand only up to the size their header declares."""

import tracemalloc
import zlib

import numpy as np
import pytest

from datajoint.blob import len_u64, pack, unpack
from datajoint.errors import DataJointError

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


@pytest.mark.parametrize(
    "payload",
    [b"", b"garbage\0data", b"no terminator here", b"mYm", b"dj0", b"\xff\xfe\0data"],
)
def test_unknown_blob_format_is_rejected(payload):
    with pytest.raises(DataJointError, match="Unknown blob format"):
        unpack(payload)


def test_unknown_blob_format_in_compressed_payload_is_rejected():
    with pytest.raises(DataJointError, match="Unknown blob format"):
        unpack(compressed_blob(b"garbage\0data"))


def test_unknown_blob_format_from_memoryview_is_rejected():
    with pytest.raises(DataJointError, match="Unknown blob format"):
        unpack(memoryview(b"garbage\0data"))


def test_blob_codec_rejects_unknown_format():
    from datajoint.builtin_codecs.blob import BlobCodec

    with pytest.raises(DataJointError, match="Unknown blob format"):
        BlobCodec().decode(b"garbage\0data")


def test_serialized_none_is_still_valid():
    assert unpack(pack(None, compress=False)) is None
