import hashlib
import uuid
import io
from pathlib import Path


def hash_key_values(mapping):
    """
    32-byte hash of the mapping's key values sorted by the key name.
    This is often used to convert a long primary key value into a shorter hash.
    For example, the JobTable in datajoint.jobs uses this function to hash the primary key of autopopulated tables.
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_stream(stream, *, init_string=""):
    """
    :return: 16-byte digest of stream data
    :stream: stream object or open file handle
    :init_string: string to initialize the checksum
    """
    hashed = hashlib.md5(init_string.encode())
    chunk = True
    chunk_size = 1 << 14
    while chunk:
        chunk = stream.read(chunk_size)
        hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())


def uuid_from_buffer(buffer=b"", *, init_string=""):
    return uuid_from_stream(io.BytesIO(buffer), init_string=init_string)


def uuid_from_file(filepath, *, init_string=""):
    return uuid_from_stream(Path(filepath).open("rb"), init_string=init_string)
