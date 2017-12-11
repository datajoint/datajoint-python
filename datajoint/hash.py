import hashlib
import base64


def key_hash(key):
    """
    32-byte hash used for lookup of primary keys of jobs
    """
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def to_ascii(byte_string):
    """
    :param byte_string: a binary string
    :return:   web-safe 64-bit ASCII encoding of binary strings
    """
    return base64.b64encode(byte_string, b'-_').decode()


def long_hash(*buffers):
    """
    :param buffer: a binary buffer (e.g. serialized blob)
    :return: 43-character base64 ASCII rendition SHA-256
    """
    hashed = hashlib.sha256()
    for buffer in buffers:
        hashed.update(buffer)
    return to_ascii(hashed.digest())[0:43]


def short_hash(*buffers):
    """
    :param buffer: a binary buffer (e.g. serialized blob)
    :return: the first 8 characters of base64 ASCII rendition SHA-1
    """
    hashed = hashlib.sha1()
    for buffer in buffers:
        hashed.update(buffer)
    return to_ascii(hashed.digest())[:8]

