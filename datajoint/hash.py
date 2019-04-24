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


def uuid_from_file(filepath, filename=None):
    """
    :return: 16-byte digest of the file at filepath
    :filepath: path to the file or folder if filename is provided.
    :filename: if provided separately, then include in the checksum and join to filepath
    """
    hashed = hashlib.md5()
    if filename is not None:
        hashed.update(filename.encode() + b'\0')
        filepath = os.path.join(filepath, filename)
    with open(filepath, 'br') as f:
        chunk = True
        chunk_size = 1 << 16
        while chunk:
            chunk = f.read(chunk_size)
            hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())
