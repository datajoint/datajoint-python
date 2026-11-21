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

    Store only - requires @store.

    This codec gives users maximum freedom in organizing their files while
    reusing DataJoint's store configuration. Files can be placed anywhere
    in the store EXCEPT the reserved sections managed by DataJoint (the
    hash-addressed and schema-addressed sections — from each store's
    ``hash_prefix`` and ``schema_prefix`` settings, defaults ``_hash`` and
    ``_schema``).

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
        Paths cannot start with the store's reserved ``hash_prefix`` or
        ``schema_prefix`` sections (defaults ``_hash/`` and ``_schema/``).

    Store Boundary:
        Paths must stay inside the store location: paths with ``..``
        components and, for ``file`` stores, absolute paths outside the
        location are rejected on insert and on access. An absolute path
        inside a ``file`` store's location is recorded relative to it. Set
        ``dj.config.allow_filepath_outside_store`` to accept references that
        point outside the store.

    Warning:
        The file must exist in the store at the specified path.
        DataJoint does not manage the lifecycle of referenced files.
    """

    name = "filepath"

    def get_dtype(self, is_store: bool) -> str:
        """Filepath requires a store (use @store syntax)."""
        if not is_store:
            raise DataJointError(
                "<filepath> requires @ symbol. Use <filepath@> for default store or <filepath@store> to specify store."
            )
        return "json"

    def encode(
        self, value: Any, *, key: dict | None = None, context: dict | None = None, store_name: str | None = None
    ) -> dict:
        """
        Store path reference as JSON metadata.

        Parameters
        ----------
        value : str
            Relative path within the store. Cannot use the store's reserved
            hash/schema sections.
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
            If path uses the store's reserved hash/schema sections.
        DataJointError
            If path leads outside the store location (see
            ``allow_filepath_outside_store``).
        FileNotFoundError
            If file does not exist in the store.
        """
        import posixpath
        from datetime import datetime, timezone

        from ..storage import path_in_location

        config = self._codec_config(key, context)
        if config is None:
            from ..settings import config  # type: ignore[assignment]
        assert config is not None

        path = str(value)

        # Resolve the store once: with no store named, <filepath@> uses
        # stores.filepath_default (filepath is not part of OAS). The checks
        # below and the recorded row must all use this store.
        store_name = config.resolve_store_name(store_name, use_filepath_default=True)
        spec = config.get_store_spec(store_name)

        # Reject paths outside the store before any prefix checks. An absolute
        # path inside a file store's location is recorded relative to it.
        backend = self._get_backend(store_name, config)
        backend._full_path(path)
        if spec.get("protocol") == "file":
            path = path_in_location(spec["location"], path) or path

        # Validate path doesn't use reserved sections (hash and schema).
        # Normalize first so that "./_hash/x" or "//_hash/x" are caught too.
        path_normalized = posixpath.normpath(path.replace("\\", "/")).lstrip("/")
        reserved_prefixes = []

        hash_prefix = spec.get("hash_prefix")
        if hash_prefix:
            reserved_prefixes.append(("hash_prefix", hash_prefix))

        schema_prefix = spec.get("schema_prefix")
        if schema_prefix:
            reserved_prefixes.append(("schema_prefix", schema_prefix))

        # Check if path starts with any reserved prefix. Compare without case:
        # on case-insensitive file systems "_HASH/x" is the same as "_hash/x".
        for prefix_name, prefix_value in reserved_prefixes:
            prefix_normalized = prefix_value.strip("/") + "/"
            if (path_normalized + "/").casefold().startswith(prefix_normalized.casefold()):
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
        if not backend.exists(path):
            raise FileNotFoundError(f"File not found in store '{store_name}': {path}")

        # Detect whether the path is a directory or a file
        is_dir = backend.isdir(path)

        # Get file size (not applicable for directories)
        size = None
        if not is_dir:
            try:
                size = backend.size(path)
            except (FileNotFoundError, OSError):
                pass

        return {
            "path": path,
            "store": store_name,
            "size": size,
            "is_dir": is_dir,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def decode(self, stored: dict, *, key: dict | None = None, context: dict | None = None) -> Any:
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

        config = self._codec_config(key, context)
        backend = self._get_backend(stored.get("store"), config)
        return ObjectRef.from_json(stored, backend=backend)

    @staticmethod
    def _get_backend(store_name: str | None, config):
        """
        Get the store backend, confined to the store location unless
        ``allow_filepath_outside_store`` is set.
        """
        from ..hash_registry import get_store_backend

        if config is None:
            from ..settings import config  # type: ignore[assignment]
        backend = get_store_backend(store_name, config=config)
        backend.confine_paths = not config.get("allow_filepath_outside_store", False)
        return backend

    def validate(self, value: Any) -> None:
        """Validate that value is a path string or Path object."""
        from pathlib import Path

        if not isinstance(value, (str, Path)):
            raise TypeError(f"<filepath> expects a path string or Path, got {type(value).__name__}")
