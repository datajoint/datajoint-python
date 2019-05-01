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
    :return: UUID converted from the MD5 hash over the buffers.
    """
    hashed = hashlib.md5()
    for buffer in buffers:
        hashed.update(buffer)
    return uuid.UUID(bytes=hashed.digest())


def uuid_from_file(filepath, init_string=""):
    """
    :return: 16-byte digest of the file at filepath
    :filepath: path to the file or folder if filename is provided.
    :init_string: string to initialize the checksum
    """
    hashed = hashlib.md5(init_string.encode())
    with open(filepath, 'br') as f:
        chunk = True
        chunk_size = 1 << 16
        while chunk:
            chunk = f.read(chunk_size)
            hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())
