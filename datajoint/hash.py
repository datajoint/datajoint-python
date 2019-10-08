import hashlib
import uuid
import io
from pathlib import Path


def key_hash(key):
    """
    32-byte hash used for lookup of primary keys of jobs
    """
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
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
