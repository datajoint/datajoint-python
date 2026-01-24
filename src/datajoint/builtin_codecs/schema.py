"""
Schema-addressed storage base class.
"""

from __future__ import annotations

from ..codecs import Codec
from ..errors import DataJointError


class SchemaCodec(Codec, register=False):
    """
    Abstract base class for schema-addressed codecs.

    Schema-addressed storage is an OAS (Object-Augmented Schema) addressing
    scheme where paths mirror the database schema structure:
    ``{schema}/{table}/{pk}/{attribute}``. This creates a browsable
    organization in object storage that reflects the schema design.

    Subclasses must implement:
        - ``name``: Codec name for ``<name@>`` syntax
        - ``encode()``: Serialize and upload content
        - ``decode()``: Create lazy reference from metadata
        - ``validate()``: Validate input values

    Helper Methods:
        - ``_extract_context()``: Parse key dict into schema/table/field/pk
        - ``_build_path()``: Construct storage path from context
        - ``_get_backend()``: Get storage backend by name

    Comparison with Hash-addressed:
        - **Schema-addressed** (this): Path from schema structure, no dedup
        - **Hash-addressed**: Path from content hash, automatic dedup

    Example::

        class MyCodec(SchemaCodec):
            name = "my"

            def encode(self, value, *, key=None, store_name=None):
                schema, table, field, pk = self._extract_context(key)
                path, _ = self._build_path(schema, table, field, pk, ext=".dat")
                backend = self._get_backend(store_name)
                backend.put_buffer(serialize(value), path)
                return {"path": path, "store": store_name, ...}

            def decode(self, stored, *, key=None):
                backend = self._get_backend(stored.get("store"))
                return MyRef(stored, backend)

    See Also
    --------
    HashCodec : Hash-addressed storage with content deduplication.
    ObjectCodec : Schema-addressed storage for files/folders.
    NpyCodec : Schema-addressed storage for numpy arrays.
    """

    def get_dtype(self, is_store: bool) -> str:
        """
        Return storage dtype. Schema-addressed codecs require @ modifier.

        Parameters
        ----------
        is_store : bool
            Must be True for schema-addressed codecs.

        Returns
        -------
        str
            "json" for metadata storage.

        Raises
        ------
        DataJointError
            If is_store is False (@ modifier missing).
        """
        if not is_store:
            raise DataJointError(f"<{self.name}> requires @ (store only)")
        return "json"

    def _extract_context(self, key: dict | None) -> tuple[str, str, str, dict]:
        """
        Extract schema, table, field, and primary key from context dict.

        Parameters
        ----------
        key : dict or None
            Context dict with ``_schema``, ``_table``, ``_field``,
            and primary key values.

        Returns
        -------
        tuple[str, str, str, dict]
            ``(schema, table, field, primary_key)``
        """
        key = dict(key) if key else {}
        schema = key.pop("_schema", "unknown")
        table = key.pop("_table", "unknown")
        field = key.pop("_field", "data")
        primary_key = {k: v for k, v in key.items() if not k.startswith("_")}
        return schema, table, field, primary_key

    def _build_path(
        self,
        schema: str,
        table: str,
        field: str,
        primary_key: dict,
        ext: str | None = None,
        store_name: str | None = None,
    ) -> tuple[str, str]:
        """
        Build schema-addressed storage path.

        Constructs a path that mirrors the database schema structure:
        ``{schema}/{table}/{pk_values}/{field}{ext}``

        Supports partitioning if configured in the store.

        Parameters
        ----------
        schema : str
            Schema name.
        table : str
            Table name.
        field : str
            Field/attribute name.
        primary_key : dict
            Primary key values.
        ext : str, optional
            File extension (e.g., ".npy", ".zarr").
        store_name : str, optional
            Store name for retrieving partition configuration.

        Returns
        -------
        tuple[str, str]
            ``(path, token)`` where path is the storage path and token
            is a unique identifier.
        """
        from ..storage import build_object_path
        from .. import config

        # Get store configuration for partition_pattern and token_length
        spec = config.get_store_spec(store_name)
        partition_pattern = spec.get("partition_pattern")
        token_length = spec.get("token_length", 8)

        return build_object_path(
            schema=schema,
            table=table,
            field=field,
            primary_key=primary_key,
            ext=ext,
            partition_pattern=partition_pattern,
            token_length=token_length,
        )

    def _get_backend(self, store_name: str | None = None):
        """
        Get storage backend by name.

        Parameters
        ----------
        store_name : str, optional
            Store name. If None, returns default store.

        Returns
        -------
        StorageBackend
            Storage backend instance.
        """
        from ..hash_registry import get_store_backend

        return get_store_backend(store_name)
