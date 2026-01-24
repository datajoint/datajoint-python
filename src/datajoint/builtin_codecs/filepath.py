"""
Filepath reference codec for existing files in storage.
"""

from __future__ import annotations

from typing import Any

from ..codecs import Codec
from ..errors import DataJointError


class FilepathCodec(Codec):
    """
    Reference to existing file in configured store.

    The ``<filepath@store>`` codec stores a reference to a file that already
    exists in the storage backend. Unlike ``<attach>`` or ``<object@>``, no
    file copying occurs - only the path is recorded.

    External only - requires @store.

    This codec gives users maximum freedom in organizing their files while
    reusing DataJoint's store configuration. Files can be placed anywhere
    in the store EXCEPT the reserved ``_hash/`` and ``_schema/`` sections
    which are managed by DataJoint.

    This is useful when:
    - Files are managed externally (e.g., by acquisition software)
    - Files are too large to copy
    - You want to reference shared datasets
    - You need custom directory structures

    Example::

        @schema
        class Recordings(dj.Manual):
            definition = '''
            recording_id : int
            ---
            raw_data : <filepath@acquisition>
            '''

        # Reference an existing file (no copy)
        # Path is relative to store location
        table.insert1({'recording_id': 1, 'raw_data': 'subject01/session001/data.bin'})

        # Fetch returns ObjectRef for lazy access
        ref = (table & 'recording_id=1').fetch1('raw_data')
        ref.read()      # Read file content
        ref.download()  # Download to local path

    Storage Format:
        JSON metadata: ``{path, store, size, timestamp}``

    Reserved Sections:
        Paths cannot start with ``_hash/`` or ``_schema/`` - these are managed by DataJoint.

    Warning:
        The file must exist in the store at the specified path.
        DataJoint does not manage the lifecycle of referenced files.
    """

    name = "filepath"

    def get_dtype(self, is_store: bool) -> str:
        """Filepath is external only."""
        if not is_store:
            raise DataJointError(
                "<filepath> requires @ symbol. Use <filepath@> for default store or <filepath@store> to specify store."
            )
        return "json"

    def encode(self, value: Any, *, key: dict | None = None, store_name: str | None = None) -> dict:
        """
        Store path reference as JSON metadata.

        Parameters
        ----------
        value : str
            Relative path within the store. Cannot use reserved sections (_hash/, _schema/).
        key : dict, optional
            Primary key values (unused).
        store_name : str, optional
            Store where the file exists.

        Returns
        -------
        dict
            Metadata dict: ``{path, store}``.

        Raises
        ------
        ValueError
            If path uses reserved sections (_hash/ or _schema/).
        FileNotFoundError
            If file does not exist in the store.
        """
        from datetime import datetime, timezone

        from .. import config
        from ..hash_registry import get_store_backend

        path = str(value)

        # Get store spec to check prefix configuration
        # Use filepath_default if no store specified (filepath is not part of OAS)
        spec = config.get_store_spec(store_name, use_filepath_default=True)

        # Validate path doesn't use reserved sections (hash and schema)
        path_normalized = path.lstrip("/")
        reserved_prefixes = []

        hash_prefix = spec.get("hash_prefix")
        if hash_prefix:
            reserved_prefixes.append(("hash_prefix", hash_prefix))

        schema_prefix = spec.get("schema_prefix")
        if schema_prefix:
            reserved_prefixes.append(("schema_prefix", schema_prefix))

        # Check if path starts with any reserved prefix
        for prefix_name, prefix_value in reserved_prefixes:
            prefix_normalized = prefix_value.strip("/") + "/"
            if path_normalized.startswith(prefix_normalized):
                raise ValueError(
                    f"<filepath@> cannot use reserved section '{prefix_value}' ({prefix_name}). "
                    f"This section is managed by DataJoint. "
                    f"Got path: {path}"
                )

        # If filepath_prefix is configured, enforce it
        filepath_prefix = spec.get("filepath_prefix")
        if filepath_prefix:
            filepath_prefix_normalized = filepath_prefix.strip("/") + "/"
            if not path_normalized.startswith(filepath_prefix_normalized):
                raise ValueError(f"<filepath@> must use prefix '{filepath_prefix}' (filepath_prefix). Got path: {path}")

        # Verify file exists
        backend = get_store_backend(store_name)
        if not backend.exists(path):
            raise FileNotFoundError(f"File not found in store '{store_name or 'default'}': {path}")

        # Get file info
        try:
            size = backend.size(path)
        except Exception:
            size = None

        return {
            "path": path,
            "store": store_name,
            "size": size,
            "is_dir": False,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def decode(self, stored: dict, *, key: dict | None = None) -> Any:
        """
        Create ObjectRef handle for lazy access.

        Parameters
        ----------
        stored : dict
            Metadata dict with path and store.
        key : dict, optional
            Primary key values (unused).

        Returns
        -------
        ObjectRef
            Handle for accessing the file.
        """
        from ..objectref import ObjectRef
        from ..hash_registry import get_store_backend

        store_name = stored.get("store")
        backend = get_store_backend(store_name)
        return ObjectRef.from_json(stored, backend=backend)

    def validate(self, value: Any) -> None:
        """Validate that value is a path string or Path object."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<filepath> expects a path string or Path, got {type(value).__name__}")
