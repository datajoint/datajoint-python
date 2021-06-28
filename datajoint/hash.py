import hashlib
import uuid
import io
import pathlib
import typing

CHUNK_SIZE_DEFAULT = 1 << 14
CHUNK_COUNT_DEFAULT = 5
INIT_STRING_DEFAULT = ''


def key_hash(mapping):
    """
    32-byte hash of the mapping's key values sorted by the key name.
    This is often used to convert a long primary key value into a shorter hash.
    For example, the JobTable in datajoint.jobs uses this function to hash the primary key of autopopulated tables.
    """
    hashed = hashlib.md5()
    for k, v in sorted(mapping.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def _decompose_stream(stream: io.BytesIO, *, chunk_size: int = CHUNK_SIZE_DEFAULT,
                      chunk_count: int = CHUNK_COUNT_DEFAULT) -> bytes:
    """
    Break up stream into smaller, consumable chunks.

    :param stream: stream object or open file handle
    :type stream: :class:`~io.BytesIO`
    :param chunk_size: max size of individual chunks, defaults to `16` kilobytes
    :type chunk_size: int, optional
    :param chunk_count: number of chunks to linearly seek within stream, defaults to `None`
        i.e. read all of stream as chunks
    :type chunk_count: int, optional
    :return: next individual byte chunk from stream
    :rtype: bytes
    """
    byte_count = stream.getbuffer().nbytes
    if chunk_count is None or byte_count <= chunk_size * chunk_count:
        for _ in range(math.ceil(byte_count / chunk_size)):
            yield stream.read(chunk_size)
    else:
        for _ in range(chunk_count):
            stream.seek(round(byte_count/(chunk_count + 1)) - int(chunk_size / 2.), 1)
            yield stream.read(chunk_size)


def uuid_from_stream(stream: io.BytesIO, *, init_string: str = INIT_STRING_DEFAULT,
                     chunk_size: int = CHUNK_SIZE_DEFAULT,
                     chunk_count: int = CHUNK_COUNT_DEFAULT) -> uuid.UUID:
    """
    Checksum for a stream.

    :param stream: stream object or open file handle
    :type stream: :class:`~io.BytesIO`
    :param init_string: string to initialize the checksum, defaults to `''`
    :type init_string: str, optional
    :param chunk_size: max size of individual chunks, defaults to `16` kilobytes
    :type chunk_size: int, optional
    :param chunk_count: number of chunks to linearly seek within stream, defaults to `None`
        i.e. read all of stream as chunks
    :type chunk_count: int, optional
    :return: 16-byte digest of stream data i.e. checksum
    :rtype: :class:`~uuid.UUID`
    """
    hashed = hashlib.md5(init_string.encode())
    for chunk in _decompose_stream(stream=stream, chunk_size=chunk_size,
                                   chunk_count=chunk_count):
        hashed.update(chunk)
    return uuid.UUID(bytes=hashed.digest())


def uuid_from_buffer(buffer: bytes = b"", *, init_string: str = INIT_STRING_DEFAULT,
                     chunk_size: int = CHUNK_SIZE_DEFAULT,
                     chunk_count: int = CHUNK_COUNT_DEFAULT) -> uuid.UUID:
    """
    Checksum for a buffer i.e. byte string.

    :param stream: buffer for checksum evaluation
    :type stream: bytes
    :param init_string: string to initialize the checksum, defaults to `''`
    :type init_string: str, optional
    :param chunk_size: max size of individual chunks, defaults to `16` kilobytes
    :type chunk_size: int, optional
    :param chunk_count: number of chunks to linearly seek within stream, defaults to `None`
        i.e. read all of stream as chunks
    :type chunk_count: int, optional
    :return: 16-byte digest of stream data i.e. checksum
    :rtype: :class:`~uuid.UUID`
    """
    return uuid_from_stream(stream=io.BytesIO(buffer), init_string=init_string,
                            chunk_size=chunk_size, chunk_count=chunk_count)


def uuid_from_file(filepath: typing.Union(str, pathlib.Path), *,
                   init_string: str = INIT_STRING_DEFAULT,
                   chunk_size: int = CHUNK_SIZE_DEFAULT,
                   chunk_count: int = CHUNK_COUNT_DEFAULT) -> uuid.UUID:
    """
    Checksum for a filepath.

    :param filepath: path to file for checksum evaluation
    :type filepath: str or :class:`~pathlib.Path`
    :param init_string: string to initialize the checksum, defaults to `''`
    :type init_string: str, optional
    :param chunk_size: max size of individual chunks, defaults to `16` kilobytes
    :type chunk_size: int, optional
    :param chunk_count: number of chunks to linearly seek within stream, defaults to `None`
        i.e. read all of stream as chunks
    :type chunk_count: int, optional
    :return: 16-byte digest of stream data i.e. checksum
    :rtype: :class:`~uuid.UUID`
    """
    return uuid_from_stream(stream=pathlib.Path(filepath).open("rb"), init_string=init_string,
                            chunk_size=chunk_size, chunk_count=chunk_count)
