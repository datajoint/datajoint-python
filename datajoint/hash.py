import hashlib
import uuid
import os


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
    :return: UUID converted from the MD5 hash over the buffers.
    """
    hashed = hashlib.md5()
    for buffer in buffers:
        hashed.update(buffer)
    return uuid.UUID(bytes=hashed.digest())


def uuid_from_file(filepath, filename):
    """
    :return: 16-byte digest SH1
    """
    hashed = hashlib.md5()
    hashed.update(filename.encode() + b'\0')
    with open(os.path.join(filepath, filename), 'br') as f:
        chunk = True
        chunk_size = 1 << 16
        while chunk:
            chunk = f.read(chunk_size)
            hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())
