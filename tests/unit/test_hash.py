from datajoint import hash


def test_key_hash():
    """Test that key_hash produces consistent MD5 hex digests."""
    assert hash.key_hash({"a": 1, "b": 2}) == hash.key_hash({"b": 2, "a": 1})
    assert hash.key_hash({"x": "hello"}) == "5d41402abc4b2a76b9719d911017c592"
    assert hash.key_hash({}) == "d41d8cd98f00b204e9800998ecf8427e"
