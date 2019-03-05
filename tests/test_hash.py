from nose.tools import assert_equal
from datajoint import hash


def test_hash():
    assert_equal(hash.uuid_from_buffer(b'abc').hex, '900150983cd24fb0d6963f7d28e17f72')
    assert_equal(hash.uuid_from_buffer(b'').hex, 'd41d8cd98f00b204e9800998ecf8427e')
