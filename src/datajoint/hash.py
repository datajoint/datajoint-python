import hashlib
import uuid


def key_hash(mapping):
    """
    32-byte hash of the mapping's key values sorted by the key name.
    This is often used to convert a long primary key value into a shorter hash.
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_buffer(buffer=b"", *, init_string=""):
    """
    Compute MD5 hash of buffer data, returned as UUID.

    :param buffer: bytes to hash
    :param init_string: string to initialize the checksum (for namespacing)
    :return: UUID based on MD5 digest
    """
    hashed = hashlib.md5(init_string.encode())
    hashed.update(buffer)
    return uuid.UUID(bytes=hashed.digest())
