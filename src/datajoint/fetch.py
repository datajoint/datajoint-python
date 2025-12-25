"""
Data fetching utilities for DataJoint query expressions.

This module provides the Fetch and Fetch1 classes that handle retrieving
data from the database, unpacking blobs, and downloading external files.
"""

from __future__ import annotations

import itertools
import json
import numbers
import uuid
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

import numpy as np
import pandas

from datajoint.condition import Top

from . import hash
from .errors import DataJointError
from .objectref import ObjectRef
from .settings import config
from .storage import StorageBackend
from .utils import safe_write

if TYPE_CHECKING:
    from collections.abc import Generator, Iterator

    from .connection import Connection
    from .expression import QueryExpression
    from .heading import Attribute


class key:
    """
    Sentinel object for requesting primary key in expression.fetch().

    The string "KEY" can be used interchangeably with this class.

    Example:
        >>> table.fetch('attribute', dj.key)  # fetch attribute values and keys
        >>> table.fetch('KEY')  # equivalent using string
    """

    pass


def is_key(attr: Any) -> bool:
    """Check if an attribute reference represents the primary key."""
    return attr is key or attr == "KEY"


def to_dicts(recarray: np.ndarray) -> Generator[dict, None, None]:
    """
    Convert a numpy record array to a generator of dictionaries.

    Args:
        recarray: A numpy structured/record array.

    Yields:
        Dictionary for each row with field names as keys.
    """
    for rec in recarray:
        yield dict(zip(recarray.dtype.names, rec.tolist()))


def _get(
    connection: Connection,
    attr: Attribute,
    data: Any,
    squeeze: bool,
    download_path: str | Path,
) -> Any:
    """
    Process and unpack a single attribute value from the database.

    Handles special attribute types including blobs, attachments, external storage,
    UUIDs, JSON, and object references.

    Args:
        connection: The database connection for accessing external stores.
        attr: Attribute metadata from the table's heading.
        data: Raw value fetched from the database.
        squeeze: If True, remove extra dimensions from blob arrays.
        download_path: Directory for downloading attachments.

    Returns:
        The unpacked/processed attribute value.
    """
    if data is None:
        return
    if attr.is_object:
        # Object type - return ObjectRef handle
        json_data = json.loads(data) if isinstance(data, str) else data
        try:
            spec = config.get_object_storage_spec()
            backend = StorageBackend(spec)
        except DataJointError:
            backend = None
        return ObjectRef.from_json(json_data, backend=backend)
    if attr.json:
        return json.loads(data)

    extern = connection.schemas[attr.database].external[attr.store] if attr.is_external else None

    # apply custom attribute type decoder if present
    def adapt(x):
        return attr.adapter.decode(x, key=None) if attr.adapter else x

    if attr.is_filepath:
        return adapt(extern.download_filepath(uuid.UUID(bytes=data))[0])
    if attr.is_attachment:
        # Steps:
        # 1. get the attachment filename
        # 2. check if the file already exists at download_path, verify checksum
        # 3. if exists and checksum passes then return the local filepath
        # 4. Otherwise, download the remote file and return the new filepath
        _uuid = uuid.UUID(bytes=data) if attr.is_external else None
        attachment_name = extern.get_attachment_name(_uuid) if attr.is_external else data.split(b"\0", 1)[0].decode()
        local_filepath = Path(download_path) / attachment_name
        if local_filepath.is_file():
            attachment_checksum = _uuid if attr.is_external else hash.uuid_from_buffer(data)
            if attachment_checksum == hash.uuid_from_file(local_filepath, init_string=attachment_name + "\0"):
                return adapt(str(local_filepath))  # checksum passed, no need to download again
            # generate the next available alias filename
            for n in itertools.count():
                f = local_filepath.parent / (local_filepath.stem + "_%04x" % n + local_filepath.suffix)
                if not f.is_file():
                    local_filepath = f
                    break
                if attachment_checksum == hash.uuid_from_file(f, init_string=attachment_name + "\0"):
                    return adapt(str(f))  # checksum passed, no need to download again
        # Save attachment
        if attr.is_external:
            extern.download_attachment(_uuid, attachment_name, local_filepath)
        else:
            # write from buffer
            safe_write(local_filepath, data.split(b"\0", 1)[1])
        return adapt(str(local_filepath))  # download file from remote store

    if attr.uuid:
        return adapt(uuid.UUID(bytes=data))
    elif attr.is_blob:
        blob_data = extern.get(uuid.UUID(bytes=data)) if attr.is_external else data
        # Adapters (like <djblob>) handle deserialization in decode()
        # Without adapter, blob columns return raw bytes (no deserialization)
        if attr.adapter:
            return attr.adapter.decode(blob_data, key=None)
        return blob_data  # raw bytes
    else:
        return adapt(data)


class Fetch:
    """
    Handler for retrieving multiple rows from a query expression.

    Provides flexible data retrieval with support for various output formats,
    attribute selection, ordering, and pagination.

    Args:
        expression: The QueryExpression to fetch data from.
    """

    def __init__(self, expression: QueryExpression) -> None:
        self._expression = expression

    def __call__(
        self,
        *attrs: str,
        offset: int | None = None,
        limit: int | None = None,
        order_by: str | list[str] | None = None,
        format: str | None = None,
        as_dict: bool | None = None,
        squeeze: bool = False,
        download_path: str | Path = ".",
    ) -> np.ndarray | list[dict] | pandas.DataFrame | list | tuple:
        """
        Fetch results from the database into various output formats.

        Args:
            *attrs: Attribute names to fetch. If empty, fetches all attributes.
                Use "KEY" or dj.key to include primary key values.
            offset: Number of rows to skip before returning results.
            limit: Maximum number of rows to return.
            order_by: Attribute(s) for sorting. Use "KEY" for primary key ordering,
                append " DESC" for descending order (e.g., "timestamp DESC").
            format: Output format when fetching all attributes:
                - None: Use config['fetch_format'] default
                - "array": Return numpy structured array
                - "frame": Return pandas DataFrame
            as_dict: If True, return list of dictionaries. Defaults to True for
                "KEY" fetches, False otherwise.
            squeeze: If True, remove extra dimensions from blob arrays.
            download_path: Directory for downloading attachments.

        Returns:
            Data in the requested format:
            - Single attr: array of values
            - Multiple attrs: tuple of arrays
            - No attrs: structured array, DataFrame, or list of dicts
        """
        if offset or order_by or limit:
            self._expression = self._expression.restrict(
                Top(
                    limit,
                    order_by,
                    offset,
                )
            )

        attrs_as_dict = as_dict and attrs
        if attrs_as_dict:
            # absorb KEY into attrs and prepare to return attributes as dict (issue #595)
            if any(is_key(k) for k in attrs):
                attrs = list(self._expression.primary_key) + [a for a in attrs if a not in self._expression.primary_key]
        if as_dict is None:
            as_dict = bool(attrs)  # default to True for "KEY" and False otherwise
        # format should not be specified with attrs or is_dict=True
        if format is not None and (as_dict or attrs):
            raise DataJointError(
                "Cannot specify output format when as_dict=True or when attributes are selected to be fetched separately."
            )
        if format not in {None, "array", "frame"}:
            raise DataJointError('Fetch output format must be in {{"array", "frame"}} but "{}" was given'.format(format))

        if not (attrs or as_dict) and format is None:
            format = config["fetch_format"]  # default to array
            if format not in {"array", "frame"}:
                raise DataJointError(
                    'Invalid entry "{}" in datajoint.config["fetch_format"]: use "array" or "frame"'.format(format)
                )

        get = partial(
            _get,
            self._expression.connection,
            squeeze=squeeze,
            download_path=download_path,
        )
        if attrs:  # a list of attributes provided
            attributes = [a for a in attrs if not is_key(a)]
            ret = self._expression.proj(*attributes)
            ret = ret.fetch(
                offset=offset,
                limit=limit,
                order_by=order_by,
                as_dict=False,
                squeeze=squeeze,
                download_path=download_path,
                format="array",
            )
            if attrs_as_dict:
                ret = [{k: v for k, v in zip(ret.dtype.names, x) if k in attrs} for x in ret]
            else:
                return_values = [
                    (
                        list((to_dicts if as_dict else lambda x: x)(ret[self._expression.primary_key]))
                        if is_key(attribute)
                        else ret[attribute]
                    )
                    for attribute in attrs
                ]
                ret = return_values[0] if len(attrs) == 1 else return_values
        else:  # fetch all attributes as a numpy.record_array or pandas.DataFrame
            cur = self._expression.cursor(as_dict=as_dict)
            heading = self._expression.heading
            if as_dict:
                ret = [dict((name, get(heading[name], d[name])) for name in heading.names) for d in cur]
            else:
                ret = list(cur.fetchall())
                record_type = (
                    heading.as_dtype
                    if not ret
                    else np.dtype(
                        [
                            (
                                (
                                    name,
                                    type(value),
                                )  # use the first element to determine blob type
                                if heading[name].is_blob and isinstance(value, numbers.Number)
                                else (name, heading.as_dtype[name])
                            )
                            for value, name in zip(ret[0], heading.as_dtype.names)
                        ]
                    )
                )
                try:
                    ret = np.array(ret, dtype=record_type)
                except Exception as e:
                    raise e
                for name in heading:
                    # unpack blobs and externals
                    ret[name] = list(map(partial(get, heading[name]), ret[name]))
                if format == "frame":
                    ret = pandas.DataFrame(ret).set_index(heading.primary_key)
        return ret


class Fetch1:
    """
    Handler for fetching exactly one row from a query expression.

    Raises an error if the query returns zero or more than one row.

    Args:
        expression: The QueryExpression to fetch from.
    """

    def __init__(self, expression: QueryExpression) -> None:
        self._expression = expression

    def __call__(
        self,
        *attrs: str,
        squeeze: bool = False,
        download_path: str | Path = ".",
    ) -> dict | Any | tuple:
        """
        Fetch exactly one row from the query expression.

        Args:
            *attrs: Attribute names to fetch. If empty, returns all attributes
                as a dictionary. If specified, returns values as a tuple.
            squeeze: If True, remove extra dimensions from blob arrays.
            download_path: Directory for downloading attachments.

        Returns:
            - No attrs: Dictionary with all attribute values
            - One attr: Single value
            - Multiple attrs: Tuple of values

        Raises:
            DataJointError: If the query returns zero or more than one row.

        Examples:
            >>> d = rel.fetch1()  # returns dict
            >>> a, b = rel.fetch1('a', 'b')  # returns tuple
            >>> val = rel.fetch1('value')  # returns single value
        """
        heading = self._expression.heading

        if not attrs:  # fetch all attributes, return as ordered dict
            cur = self._expression.cursor(as_dict=True)
            ret = cur.fetchone()
            if not ret or cur.fetchone():
                raise DataJointError("fetch1 requires exactly one tuple in the input set.")
            ret = dict(
                (
                    name,
                    _get(
                        self._expression.connection,
                        heading[name],
                        ret[name],
                        squeeze=squeeze,
                        download_path=download_path,
                    ),
                )
                for name in heading.names
            )
        else:  # fetch some attributes, return as tuple
            attributes = [a for a in attrs if not is_key(a)]
            result = self._expression.proj(*attributes).fetch(squeeze=squeeze, download_path=download_path, format="array")
            if len(result) != 1:
                raise DataJointError("fetch1 should only return one tuple. %d tuples found" % len(result))
            return_values = tuple(
                (next(to_dicts(result[self._expression.primary_key])) if is_key(attribute) else result[attribute][0])
                for attribute in attrs
            )
            ret = return_values[0] if len(attrs) == 1 else return_values
        return ret
