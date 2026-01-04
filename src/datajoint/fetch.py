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

    For composed types (e.g., <blob@> using <hash>), decoders are applied
    in reverse order: innermost first, then outermost.

    :param connection: a dj.Connection object
    :param attr: attribute from the table's heading
    :param data: raw value fetched from the database
    :param squeeze: if True squeeze blobs (legacy, unused)
    :param download_path: for fetches that download data (attachments, filepaths)
    :return: decoded data
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
    Fetch object for fetching the result of a query yielding exactly one row.

    :param expression: a query expression to fetch from.
    """

    def __init__(self, expression):
        self._expression = expression

    def __call__(self, *attrs, squeeze=False, download_path="."):
        """
        Fetches the result of a query expression that yields exactly one entry.

        If no attributes are specified, returns the result as a dict.
        If attributes are specified returns the corresponding results as a tuple.

        Examples:
            d = rel.fetch1()           # returns dict with all attributes
            a, b = rel.fetch1('a', 'b')  # returns tuple of attribute values

        :param attrs: attributes to return when expanding into a tuple.
                      If empty, returns a dict with all attributes.
        :param squeeze: when True, remove extra dimensions from arrays
        :param download_path: for fetches that download data, e.g. attachments
        :return: dict (no attrs) or tuple/value (with attrs)
        :raises DataJointError: if not exactly one row in result
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
