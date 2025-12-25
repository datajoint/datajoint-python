"""
Hashing utilities for DataJoint.

This module provides functions for computing hashes of data, streams, and files.
These are used for checksums, content-addressable storage, and primary key hashing.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import IO


def key_hash(mapping: Mapping) -> str:
    """
    Compute a 32-character hex hash of a mapping's values, sorted by key name.

    This is commonly used to convert long primary key values into shorter hashes.
    For example, the JobTable in datajoint.jobs uses this function to hash the
    primary keys of autopopulated tables.

    Args:
        mapping: A dict-like object whose values will be hashed.

    Returns:
        A 32-character hexadecimal MD5 hash string.

    Example:
        >>> key_hash({'subject_id': 1, 'session': 5})
        'a1b2c3d4e5f6...'
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_stream(stream: IO[bytes], *, init_string: str = "") -> uuid.UUID:
    """
    Compute a UUID from the contents of a binary stream.

    Reads the stream in chunks and computes an MD5 hash, returning it as a UUID.

    Args:
        stream: A binary stream object (file handle opened in 'rb' mode or BytesIO).
        init_string: Optional string to initialize the hash (acts as a salt).

    Returns:
        A UUID object derived from the MD5 hash of the stream contents.
    """
    hashed = hashlib.md5(init_string.encode())
    chunk = True
    chunk_size = 1 << 14
    while chunk:
        chunk = stream.read(chunk_size)
        hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())


def uuid_from_buffer(buffer: bytes = b"", *, init_string: str = "") -> uuid.UUID:
    """
    Compute a UUID from a bytes buffer.

    Args:
        buffer: The binary data to hash.
        init_string: Optional string to initialize the hash (acts as a salt).

    Returns:
        A UUID object derived from the MD5 hash of the buffer.
    """
    return uuid_from_stream(io.BytesIO(buffer), init_string=init_string)


def uuid_from_file(filepath: str | Path, *, init_string: str = "") -> uuid.UUID:
    """
    Compute a UUID from the contents of a file.

    Args:
        filepath: Path to the file to hash.
        init_string: Optional string to initialize the hash (acts as a salt).

    Returns:
        A UUID object derived from the MD5 hash of the file contents.
    """
    return uuid_from_stream(Path(filepath).open("rb"), init_string=init_string)
