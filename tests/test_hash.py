from nose.tools import assert_equal
from datajoint import hash


def test_hash():
    assert_equal(hash.long_hash(b'abc'), 'ungWv48Bz-pBQUDeXa4iI7ADYaOWF3qctBD_YfIAFa0')
    assert_equal(hash.long_hash(b''), '47DEQpj8HBSa-_TImW-5JCeuQeRkm5NMpJWZG3hSuFU')
    assert_equal(hash.short_hash(b'abc'), 'qZk-NkcG')
    assert_equal(hash.short_hash(b''), '2jmj7l5r')
