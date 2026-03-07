"""
File attachment codec with filename preservation.
"""

from __future__ import annotations

from typing import Any

from ..codecs import Codec


class AttachCodec(Codec):
    """
    File attachment with filename preserved.

    Supports both in-table and in-store storage:
    - ``<attach>``: Stored in database (bytes → LONGBLOB)
    - ``<attach@>``: Stored in object store via ``<hash@>`` with deduplication
    - ``<attach@store>``: Stored in specific named store

    The filename is preserved and the file is extracted to the configured
    download path on fetch.

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

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> bytes:
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
        from pathlib import Path

        path = Path(value)
        if not path.exists():
            raise FileNotFoundError(f"Attachment file not found: {path}")
        if path.is_dir():
            raise IsADirectoryError(f"<attach> does not support directories: {path}")

        filename = path.name
        contents = path.read_bytes()
        return filename.encode("utf-8") + b"\x00" + contents

    def decode(self, stored: bytes, *, key: dict | None = None) -> str:
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
        """
        from pathlib import Path

        # Split on first null byte
        null_pos = stored.index(b"\x00")
        filename = stored[:null_pos].decode("utf-8")
        contents = stored[null_pos + 1 :]

        # Write to download path
        config = (key or {}).get("_config")
        if config is None:
            from ..settings import config
        assert config is not None
        download_path = Path(config.get("download_path", "."))
        download_path.mkdir(parents=True, exist_ok=True)
        local_path = download_path / filename

        # Handle filename collision - if file exists with different content, add suffix
        if local_path.exists():
            existing_contents = local_path.read_bytes()
            if existing_contents != contents:
                # Find unique filename
                stem = local_path.stem
                suffix = local_path.suffix
                counter = 1
                while local_path.exists() and local_path.read_bytes() != contents:
                    local_path = download_path / f"{stem}_{counter}{suffix}"
                    counter += 1

        # Only write if file doesn't exist or has different content
        if not local_path.exists():
            local_path.write_bytes(contents)

        return str(local_path)

    def validate(self, value: Any) -> None:
        """Validate that value is a valid file path."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<attach> expects a file path, got {type(value).__name__}")
