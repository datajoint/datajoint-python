"""
Schema-addressed storage base class.
"""

from __future__ import annotations

import warnings

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
        - ``_extract_context()``: Parse key/context into schema/table/field/pk
        - ``_codec_config()``: Read the calling connection's config
        - ``_build_path()``: Construct storage path from context
        - ``_get_backend()``: Get storage backend by name

    ``_build_path`` and ``_get_backend`` take a ``config`` and fall back to the
    global ``dj.config`` without one. Always pass the calling connection's
    config: in a process holding connections for several users, the global one
    belongs to none of them, and the fallback resolves a different store
    silently rather than raising.

    Since 2.3.4 that config arrives in an explicit ``context`` argument rather
    than hidden among the primary key values. **Declare ``context=None`` in
    ``encode`` and ``decode`` and pass it to the helpers**, as the example below
    does.

    The old underscore keys in ``key`` still work and are still populated, so a
    codec written before 2.3.4 keeps running — but reading them raises a
    ``DeprecationWarning`` and they are removed in 2.4.

    Comparison with Hash-addressed:
        - **Schema-addressed** (this): Path from schema structure, no dedup
        - **Hash-addressed**: Path from content hash, automatic dedup

    Example::

        class MyCodec(SchemaCodec):
            name = "my"

            def encode(self, value, *, key=None, context=None, store_name=None):
                schema, table, field, pk = self._extract_context(key, context)
                config = self._codec_config(key, context)
                path, _ = self._build_path(
                    schema, table, field, pk, ext=".dat",
                    store_name=store_name, config=config,
                )
                backend = self._get_backend(store_name, config=config)
                backend.put_buffer(serialize(value), path)
                return {"path": path, "store": store_name, ...}

            def decode(self, stored, *, key=None, context=None):
                config = self._codec_config(key, context)
                backend = self._get_backend(stored.get("store"), config=config)
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

    def _extract_context(self, key: dict | None, context: dict | None = None) -> tuple[str, str, str, dict]:
        """
        Extract schema, table, field, and primary key.

        Parameters
        ----------
        key : dict or None
            Primary key values. Before 2.3.4 this also carried connection
            context under ``_schema``, ``_table``, ``_field`` and ``_config``;
            those keys are still populated and still read, with a
            ``DeprecationWarning``, when ``context`` is not supplied.
        context : dict or None
            Connection context with ``schema``, ``table``, ``field`` and
            ``config``. Pass the ``context`` argument your ``encode``/``decode``
            received.

        Returns
        -------
        tuple[str, str, str, dict]
            ``(schema, table, field, primary_key)``
        """
        key = dict(key) if key else {}
        if context is None and any(k.startswith("_") for k in key):
            warnings.warn(
                "Reading connection context from the `key` dict is deprecated and will "
                "be removed in DataJoint 2.4. Accept a `context` argument in encode()/decode() "
                "and pass it to _extract_context(key, context). See "
                "https://github.com/datajoint/datajoint-python/issues/1550",
                DeprecationWarning,
                stacklevel=2,
            )
        context = context or {}
        schema = context.get("schema", key.pop("_schema", "unknown"))
        table = context.get("table", key.pop("_table", "unknown"))
        field = context.get("field", key.pop("_field", "data"))
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
        config=None,
    ) -> tuple[str, str]:
        """
        Build schema-addressed storage path.

        Constructs a path that mirrors the database schema structure:
        ``{schema_prefix}/{schema}/{table}/{pk_values}/{field}{ext}``

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
        config : Config, optional
            Config instance. If None, falls back to global settings.config.

        Returns
        -------
        tuple[str, str]
            ``(path, token)`` where path is the storage path and token
            is a unique identifier.
        """
        from ..storage import build_object_path

        if config is None:
            from ..settings import config

        # Get store configuration for partition_pattern and token_length
        spec = config.get_store_spec(store_name)
        partition_pattern = spec.get("partition_pattern")
        token_length = spec.get("token_length", 8)
        schema_prefix = spec["schema_prefix"]  # always present: settings applies the default

        return build_object_path(
            schema=schema,
            table=table,
            field=field,
            primary_key=primary_key,
            ext=ext,
            partition_pattern=partition_pattern,
            token_length=token_length,
            schema_prefix=schema_prefix,
        )

    def _get_backend(self, store_name: str | None = None, config=None):
        """
        Get storage backend by name.

        Parameters
        ----------
        store_name : str, optional
            Store name. If None, returns default store.
        config : Config, optional
            Config instance. If None, falls back to global settings.config.

        Returns
        -------
        StorageBackend
            Storage backend instance.
        """
        from ..hash_registry import get_store_backend

        return get_store_backend(store_name, config=config)
