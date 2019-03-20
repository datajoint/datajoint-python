import numpy as np
from decimal import Decimal
from datetime import datetime
from datajoint.blob import pack, unpack
from numpy.testing import assert_array_equal
from nose.tools import assert_equal, assert_true


def test_pack():
    x = 32
    assert_equal(x, unpack(pack(x)), "Numbers don't match!")

    x = 32.0
    assert_equal(x, unpack(pack(x)), "Numbers don't match!")

    x = np.float32(32.0)
    assert_equal(x, unpack(pack(x)), "Numbers don't match!")

    x = np.random.randn(8, 10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.random.randn(10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = {'name': 'Anonymous', 'age': 15}
    assert_true(x == unpack(pack(x), as_dict=True), "Dict do not match!")

    x = [1, 2, 3, 4]
    assert_array_equal(x, unpack(pack(x)), "List did not pack/unpack correctly")

    x = [1, 2, 3, 4]
    assert_array_equal(x, unpack(pack(x.__iter__())), "Iterator did not pack/unpack correctly")

    x = Decimal('1.24')
    assert_true(float(x) == unpack(pack(x)), "Decimal object did not pack/unpack correctly")

    x = datetime.now()
    assert_true(str(x) == unpack(pack(x)), "Datetime object did not pack/unpack correctly")



def test_complex():
    z = np.random.randn(8, 10) + 1j*np.random.randn(8,10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    z = np.random.randn(10) + 1j*np.random.randn(10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5)) + 1j*np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3)) + 1j*np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")
