"""Regression tests for MATLAB-compatible character-array blob serialization."""

import numpy as np
import pytest

from datajoint.blob import pack, unpack


@pytest.mark.parametrize(
    "characters",
    [
        [[b"a", b"b", b"c"], [b"d", b"e", b"f"]],
        [[b"a", b"b"], [b"c", b"d"], [b"e", b"f"]],
        [[b"a", b"b"], [b"c", b"d"]],
    ],
    ids=["two-by-three", "three-by-two", "two-by-two"],
)
def test_char_matrix_round_trip(characters):
    matrix = np.asarray(characters, dtype="S1")
    actual = unpack(pack(matrix, compress=False))
    np.testing.assert_array_equal(actual, matrix.astype("U1"))


def test_char_matrix_uses_matlab_column_major_bytes():
    matrix = np.array([[b"a", b"b", b"c"], [b"d", b"e", b"f"]], dtype="S1")
    encoded = pack(matrix, compress=False)
    expected_data = matrix.view(np.uint8).astype(np.uint16).tobytes(order="F")

    assert encoded.startswith(b"mYm\\0")
    assert encoded.endswith(expected_data)
    np.testing.assert_array_equal(unpack(encoded), matrix.astype("U1"))


def test_compressed_char_matrix_round_trip():
    small = np.array([[b"a", b"b", b"c"], [b"d", b"e", b"f"]], dtype="S1")
    matrix = np.tile(small, (50, 30))
    encoded = pack(matrix)

    assert encoded.startswith(b"ZL123\\0")
    np.testing.assert_array_equal(unpack(encoded), matrix.astype("U1"))
