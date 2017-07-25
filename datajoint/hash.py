import hashlib
import base64


def to_ascii(byte_string):
    """
    :param byte_string: a binary string
    :return:   web-safe 64-bit ASCII encoding of binary strings
    """
    return base64.b64encode(byte_string, b'-_').decode()


def long_hash(buffer):
    """
    :param buffer: a binary buffer (e.g. serialized blob)
    :return: 43-character base64 ASCII rendition SHA-256
    """
    return to_ascii(hashlib.sha256(buffer).digest())[0:43]


def short_hash(buffer):
    """
    :param buffer: a binary buffer (e.g. serialized blob)
    :return: the first 8 characters of base64 ASCII rendition SHA-1
    """
    return to_ascii(hashlib.sha1(buffer).digest())[:8]


# def filehash(filename):
#     s = hashlib.sha256()
#     with open(filename, 'rb') as f:
#         for block in iter(lambda: f.read(65536), b''):
#             s.update(block)
#     return to_ascii(s.digest())
