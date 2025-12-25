"""
Hashing utilities for DataJoint.

This module provides functions for computing hashes of data structures and files.
These are used for content-addressable storage, change detection, and generating
unique identifiers for blob storage.
"""

from __future__ import annotations

import hashlib
import io
import uuid
from pathlib import Path
from typing import IO, Any, Mapping


def key_hash(mapping: Mapping[str, Any]) -> str:
    """
    32-byte hash of the mapping's key values sorted by the key name.
    This is often used to convert a long primary key value into a shorter hash.
    For example, the JobTable in datajoint.jobs uses this function to hash the primary key of autopopulated tables.
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def uuid_from_stream(stream: IO[bytes], *, init_string: str = "") -> uuid.UUID:
    """
    Compute a UUID from stream data.

    :param stream: stream object or open file handle
    :param init_string: string to initialize the checksum
    :return: UUID derived from 16-byte MD5 digest of stream data
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

    :param buffer: bytes data to hash
    :param init_string: string to initialize the checksum
    :return: UUID derived from 16-byte MD5 digest of buffer
    """
    return uuid_from_stream(io.BytesIO(buffer), init_string=init_string)


def uuid_from_file(filepath: str | Path, *, init_string: str = "") -> uuid.UUID:
    """
    Compute a UUID from a file's contents.

    :param filepath: path to the file
    :param init_string: string to initialize the checksum
    :return: UUID derived from 16-byte MD5 digest of file contents
    """
    return uuid_from_stream(Path(filepath).open("rb"), init_string=init_string)
