from nose.tools import assert_true
from datajoint import hash


def test_hash():
    assert_true(hash.long_hash(b'string') == 'RzKH-CmNunFjqJeQiVj3wOrnM-JdLgJ5kuou3JvtL6g')
    assert_true(hash.short_hash(b'string') == '7LJSBEte')
