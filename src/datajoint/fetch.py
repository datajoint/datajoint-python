"""
Fetch utilities for retrieving and decoding data from the database.
"""

import json
import uuid as uuid_module

import numpy as np

from .errors import DataJointError


def _get(connection, attr, data, squeeze, download_path):
    """
    Retrieve and decode attribute data from the database.

    In the simplified type system:

    - Native types pass through unchanged
    - JSON types are parsed
    - UUID types are converted from bytes
    - Blob types return raw bytes (unless a codec handles them)
    - Codecs handle all custom encoding/decoding via type chains

    For composed types (e.g., ``<blob@>`` using ``<hash>``), decoders are
    applied in reverse order: innermost first, then outermost.

    Parameters
    ----------
    connection : dj.Connection
        Database connection.
    attr : Attribute
        Attribute from the table's heading.
    data : any
        Raw value fetched from the database.
    squeeze : bool
        If True, squeeze singleton dimensions from arrays.
    download_path : str
        Path for downloading external data (attachments, filepaths).

    Returns
    -------
    any
        Decoded data in Python-native format.
    """
    from .settings import config

    if data is None:
        return None

    # Get the final storage type and type chain if codec present
    if attr.codec:
        from .codecs import resolve_dtype

        # Include store if present to get correct chain for external storage
        store = getattr(attr, "store", None)
        if store is not None:
            dtype_spec = f"<{attr.codec.name}@{store}>"
        else:
            dtype_spec = f"<{attr.codec.name}>"
        final_dtype, type_chain, _ = resolve_dtype(dtype_spec)

        # First, process the final dtype (what's stored in the database)
        if final_dtype.lower() == "json":
            data = json.loads(data)
        elif final_dtype.lower() in ("longblob", "blob", "mediumblob", "tinyblob"):
            pass  # Blob data is already bytes
        elif final_dtype.lower() == "binary(16)":
            data = uuid_module.UUID(bytes=data)

        # Temporarily set download_path for types that need it (attachments, filepaths)
        original_download_path = config.get("download_path", ".")
        config["download_path"] = str(download_path)
        try:
            # Apply decoders in reverse order: innermost first, then outermost
            for attr_type in reversed(type_chain):
                data = attr_type.decode(data, key=None)
        finally:
            config["download_path"] = original_download_path

        # Apply squeeze for blob types (removes singleton dimensions from arrays)
        if squeeze and isinstance(data, np.ndarray):
            data = data.squeeze()

        return data

    # No codec - handle native types
    if attr.json:
        return json.loads(data)

    if attr.uuid:
        return uuid_module.UUID(bytes=data)

    if attr.is_blob:
        return data  # raw bytes (use <blob> for automatic deserialization)

    # Native types - pass through unchanged
    return data


class Fetch1:
    """
    Fetch handler for queries that return exactly one row.

    Parameters
    ----------
    expression : QueryExpression
        Query expression to fetch from.
    """

    def __init__(self, expression):
        self._expression = expression

    def __call__(self, *attrs, squeeze=False, download_path="."):
        """
        Fetch exactly one row from the query result.

        Parameters
        ----------
        *attrs : str
            Attribute names to return. If empty, returns all as dict.
            Use ``"KEY"`` to fetch primary key as a dict.
        squeeze : bool, optional
            If True, remove singleton dimensions from arrays. Default False.
        download_path : str, optional
            Path for downloading external data. Default ``"."``.

        Returns
        -------
        dict or tuple or value
            If no attrs: dict with all attributes.
            If one attr: single value.
            If multiple attrs: tuple of values.

        Raises
        ------
        DataJointError
            If query does not return exactly one row.

        Examples
        --------
        >>> row = table.fetch1()              # dict with all attributes
        >>> a, b = table.fetch1('a', 'b')     # tuple of values
        >>> x = table.fetch1('x')             # single value
        >>> pk = table.fetch1('KEY')          # primary key dict
        """
        heading = self._expression.heading

        if not attrs:
            # Fetch all attributes, return as dict
            cursor = self._expression.cursor(as_dict=True)
            row = cursor.fetchone()
            if not row or cursor.fetchone():
                raise DataJointError("fetch1 requires exactly one tuple in the input set.")
            return {
                name: _get(
                    self._expression.connection,
                    heading[name],
                    row[name],
                    squeeze=squeeze,
                    download_path=download_path,
                )
                for name in heading.names
            }
        else:
            # Handle "KEY" specially - it means primary key columns
            def is_key(attr):
                return attr == "KEY"

            has_key = any(is_key(a) for a in attrs)

            if has_key and len(attrs) == 1:
                # Just fetching KEY - return the primary key dict
                keys = self._expression.keys()
                if len(keys) != 1:
                    raise DataJointError(f"fetch1 should only return one tuple. {len(keys)} tuples found")
                return keys[0]

            # Fetch specific attributes, return as tuple
            # Replace KEY with primary key columns for projection
            proj_attrs = []
            for attr in attrs:
                if is_key(attr):
                    proj_attrs.extend(self._expression.primary_key)
                else:
                    proj_attrs.append(attr)

            dicts = self._expression.proj(*proj_attrs).to_dicts(squeeze=squeeze, download_path=download_path)
            if len(dicts) != 1:
                raise DataJointError(f"fetch1 should only return one tuple. {len(dicts)} tuples found")
            row = dicts[0]

            # Build result values, handling KEY specially
            values = []
            for attr in attrs:
                if is_key(attr):
                    # Return dict of primary key columns
                    values.append({k: row[k] for k in self._expression.primary_key})
                else:
                    values.append(row[attr])

            return values[0] if len(attrs) == 1 else tuple(values)
