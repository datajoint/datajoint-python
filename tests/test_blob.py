__author__ = 'fabee'
import numpy as np
from datajoint.blob import pack, unpack
from numpy.testing import assert_array_equal


def test_pack():
    x = np.random.randn(10, 10)

    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")
