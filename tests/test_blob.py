import numpy as np
from datajoint.blob import pack, unpack
from numpy.testing import assert_array_equal, raises


def test_pack():
    x = np.random.randn(8, 10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.random.randn(10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")


def test_complex():
    z = np.random.randn(8, 10) + 1j*np.random.randn(8,10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    z = np.random.randn(10) + 1j*np.random.randn(10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5)) + 1j*np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3)) + 1j*np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")
