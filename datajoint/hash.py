import hashlib
import uuid


def key_hash(key):
    """
    32-byte hash used for lookup of primary keys of jobs
    """
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_buffer(*buffers):
    """
    :param buffers: any number of binary buffers (e.g. serialized blobs)
    :return: 16-byte digest SHA-1
    """
    hashed = hashlib.md5()
    for buffer in buffers:
        hashed.update(buffer)
    return uuid.UUID(bytes=hashed.digest())
