import numpy as np
from decimal import Decimal
from datetime import datetime
from datajoint.blob import pack, unpack
from numpy.testing import assert_array_equal
from nose.tools import assert_equal, assert_true, assert_list_equal, assert_set_equal, assert_tuple_equal


def test_pack():

    for x in (32, -3.7e-2, np.float64(3e31), -np.inf, np.int8(-3), np.uint8(-1),
              np.int16(-33), np.uint16(-33), np.int32(-3), np.uint32(-1), np.int64(373), np.uint64(-3)):
        assert_equal(x, unpack(pack(x)), "Scalars don't match!")

    x = np.nan
    assert_true(np.isnan(unpack(pack(x))), "nan scalar did not match!")

    x = np.random.randn(8, 10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.random.randn(10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = {'name': 'Anonymous', 'age': 15}
    assert_true(x == unpack(pack(x)), "Dict do not match!")

    x = [1, datetime.now(), {1: "one", "two": 2}, (1, 2)]
    assert_list_equal(x, list(unpack(pack(x))), "List did not pack/unpack correctly")

    x = (1, {datetime.now().date(): "today", "now": datetime.now().date()}, "yes!", (1, 2, (3, 4)))
    assert_tuple_equal(x, unpack(pack(x)), "Tuple did not pack/unpack correctly")

    x = {'elephant'}
    assert_set_equal(x, set(unpack(pack(x))), "Set did not pack/unpack correctly")

    x = tuple(range(10))
    assert_tuple_equal(x, unpack(pack(range(10))), "Iterator did not pack/unpack correctly")

    x = Decimal('1.24')
    assert_true(float(x) == unpack(pack(x)), "Decimal object did not pack/unpack correctly")

    x = datetime.now()
    assert_true(x == unpack(pack(x)), "Datetime object did not pack/unpack correctly")


def test_complex():
    z = np.random.randn(8, 10) + 1j*np.random.randn(8,10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    z = np.random.randn(10) + 1j*np.random.randn(10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5)) + 1j*np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3)) + 1j*np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")
