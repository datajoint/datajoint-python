"""
File attachment codec with filename preservation.
"""

from __future__ import annotations

import os
import stat
from pathlib import Path
from typing import Any

from ..codecs import Codec
from ..errors import DataJointError
from ..utils import is_plain_file_name


def _check_file_name(filename: str) -> None:
    """
    Raise unless ``filename`` is a plain file name with no directory part.

    ``encode`` stores only the base name of the attached file. A stored name
    with path separators (``/`` or ``\\``), ``..``, a NUL byte, or (on Windows)
    a drive or stream marker ``:`` is refused.
    """
    if not is_plain_file_name(filename):
        raise DataJointError(f"<attach> stored file name {filename!r} is not a plain file name.")


def _has_contents(path: Path, contents: bytes) -> bool:
    """
    Check whether ``path`` is a regular file holding exactly ``contents``.

    Symbolic links, devices, and pipes are never read, and a file is read only
    when its size matches. The open does not block and does not follow a link,
    in case another file replaces the checked one before it is opened; any
    such file counts as different.
    """
    try:
        st = os.lstat(path)
    except FileNotFoundError:
        return False
    if not stat.S_ISREG(st.st_mode) or st.st_size != len(contents):
        return False
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_BINARY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return False
    with os.fdopen(fd, "rb") as f:
        if not stat.S_ISREG(os.fstat(f.fileno()).st_mode):
            return False
        return f.read(len(contents) + 1) == contents


class AttachCodec(Codec):
    """
    File attachment with filename preserved.

    Supports both in-table and in-store storage:
    - ``<attach>``: Stored in database (bytes → LONGBLOB)
    - ``<attach@>``: Stored in object store via ``<hash@>`` with deduplication
    - ``<attach@store>``: Stored in specific named store

    The filename is preserved and the file is extracted to the configured
    download path on fetch. Only the base name is kept, and fetch refuses a
    stored name that is not a plain file name, so extracted files always land
    directly in the download path.

    Example::

        @schema
        class Documents(dj.Manual):
            definition = '''
            doc_id : int
            ---
            config : <attach>           # in-table (small file in DB)
            dataset : <attach@>         # in-store (default store)
            archive : <attach@cold>     # in-store (specific store)
            '''

        # Insert a file
        table.insert1({'doc_id': 1, 'config': '/path/to/config.json'})

        # Fetch extracts to download_path and returns local path
        local_path = (table & 'doc_id=1').fetch1('config')

    Storage Format (internal):
        The blob contains: ``filename\\0contents``
        - Filename (UTF-8 encoded) + null byte + raw file contents
    """

    name = "attach"

    def get_dtype(self, is_store: bool) -> str:
        """Return bytes for in-table, <hash> for in-store storage."""
        return "<hash>" if is_store else "bytes"

    def encode(
        self, value: Any, *, key: dict | None = None, context: dict | None = None, store_name: str | None = None
    ) -> bytes:
        """
        Read file and encode as filename + contents.

        Parameters
        ----------
        value : str or Path
            Path to file.
        key : dict, optional
            Primary key values (unused).
        store_name : str, optional
            Unused for internal storage.

        Returns
        -------
        bytes
            Filename (UTF-8) + null byte + file contents.
        """
        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"Attachment file not found: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"<attach> does not support directories: {path}")

        filename = path.name
        contents = path.read_bytes()
        return filename.encode("utf-8") + b"\x00" + contents

    def decode(self, stored: bytes, *, key: dict | None = None, context: dict | None = None) -> str:
        """
        Extract file to download path and return local path.

        Parameters
        ----------
        stored : bytes
            Blob containing filename + null + contents.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        str
            Path to extracted file.

        Raises
        ------
        DataJointError
            If the stored file name is not a plain file name.
        """
        # Split on first null byte
        null_pos = stored.index(b"\x00")
        filename = stored[:null_pos].decode("utf-8")
        contents = stored[null_pos + 1 :]
        _check_file_name(filename)

        # Write to download path
        config = self._codec_config(key, context)
        if config is None:
            from ..settings import config  # type: ignore[assignment]
        assert config is not None
        download_path = Path(config.get("download_path", "."))
        download_path.mkdir(parents=True, exist_ok=True)
        local_path = download_path / filename
        if local_path.parent != download_path:
            raise DataJointError(f"<attach> file name {filename!r} leads outside the download path.")

        # Reuse a file with identical content; otherwise add a numeric suffix until
        # a free name is found. New files are created exclusively, never through an
        # existing file or link.
        stem = local_path.stem
        suffix = local_path.suffix
        counter = 0
        while True:
            if _has_contents(local_path, contents):
                return str(local_path)
            if not os.path.lexists(local_path):
                flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_BINARY", 0)
                try:
                    fd = os.open(local_path, flags, 0o666)
                except FileExistsError:
                    continue  # created concurrently; check it again
                with os.fdopen(fd, "wb") as f:
                    f.write(contents)
                return str(local_path)
            counter += 1
            local_path = download_path / f"{stem}_{counter}{suffix}"

    def validate(self, value: Any) -> None:
        """Validate that value is a valid file path."""
        if not isinstance(value, (str, Path)):
            raise TypeError(f"<attach> expects a file path, got {type(value).__name__}")
