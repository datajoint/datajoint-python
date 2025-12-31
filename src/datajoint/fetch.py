import json
import numbers
import uuid as uuid_module
from functools import partial

import numpy as np
import pandas

from datajoint.condition import Top

from .errors import DataJointError
from .settings import config


class key:
    """
    object that allows requesting the primary key as an argument in expression.fetch()
    The string "KEY" can be used instead of the class key
    """

    pass


def is_key(attr):
    return attr is key or attr == "KEY"


def to_dicts(recarray):
    """convert record array to a dictionaries"""
    for rec in recarray:
        yield dict(zip(recarray.dtype.names, rec.tolist()))


def _get(connection, attr, data, squeeze, download_path):
    """
    Retrieve and decode attribute data from the database.

    In the simplified type system:
    - Native types pass through unchanged
    - JSON types are parsed
    - UUID types are converted from bytes
    - Blob types return raw bytes (unless an adapter handles them)
    - Adapters (AttributeTypes) handle all custom encoding/decoding via type chains

    For composed types (e.g., <xblob> using <content>), decoders are applied
    in reverse order: innermost first, then outermost.

    :param connection: a dj.Connection object
    :param attr: attribute from the table's heading
    :param data: raw value fetched from the database
    :param squeeze: if True squeeze blobs (legacy, unused)
    :param download_path: for fetches that download data (legacy, unused in simplified model)
    :return: decoded data
    """
    if data is None:
        return None

    # Get the final storage type and type chain if adapter present
    if attr.adapter:
        from .attribute_type import resolve_dtype

        final_dtype, type_chain, _ = resolve_dtype(f"<{attr.adapter.type_name}>")

        # First, process the final dtype (what's stored in the database)
        if final_dtype.lower() == "json":
            data = json.loads(data)
        elif final_dtype.lower() in ("longblob", "blob", "mediumblob", "tinyblob"):
            pass  # Blob data is already bytes
        elif final_dtype.lower() == "binary(16)":
            data = uuid_module.UUID(bytes=data)

        # Apply decoders in reverse order: innermost first, then outermost
        for attr_type in reversed(type_chain):
            data = attr_type.decode(data, key=None)

        return data

    # No adapter - handle native types
    if attr.json:
        return json.loads(data)

    if attr.uuid:
        return uuid_module.UUID(bytes=data)

    if attr.is_blob:
        return data  # raw bytes (use <djblob> for automatic deserialization)

    # Native types - pass through unchanged
    return data


class Fetch:
    """
    A fetch object that handles retrieving elements from the table expression.

    :param expression: the QueryExpression object to fetch from.
    """

    def __init__(self, expression):
        self._expression = expression

    def __call__(
        self,
        *attrs,
        offset=None,
        limit=None,
        order_by=None,
        format=None,
        as_dict=None,
        squeeze=False,
        download_path=".",
    ):
        """
        Fetches the expression results from the database into an np.array or list of dictionaries and
        unpacks blob attributes.

        :param attrs: zero or more attributes to fetch. If not provided, the call will return all attributes of this
                        table. If provided, returns tuples with an entry for each attribute.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: a single attribute or the list of attributes to order the results. No ordering should be assumed
                        if order_by=None. To reverse the order, add DESC to the attribute name or names: e.g. ("age DESC",
                        "frequency") To order by primary key, use "KEY" or "KEY DESC"
        :param format: Effective when as_dict=None and when attrs is empty None: default from config['fetch_format'] or
                        'array' if not configured "array": use numpy.key_array "frame": output pandas.DataFrame. .
        :param as_dict: returns a list of dictionaries instead of a record array. Defaults to False for .fetch() and to
                        True for .fetch('KEY')
        :param squeeze:  if True, remove extra dimensions from arrays
        :param download_path: for fetches that download data, e.g. attachments
        :return: the contents of the table in the form of a structured numpy.array or a dict list
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
    Fetch object for fetching the result of a query yielding one row.

    :param expression: a query expression to fetch from.
    """

    def __init__(self, expression):
        self._expression = expression

    def __call__(self, *attrs, squeeze=False, download_path="."):
        """
        Fetches the result of a query expression that yields one entry.

        If no attributes are specified, returns the result as a dict.
        If attributes are specified returns the corresponding results as a tuple.

        Examples:
        d = rel.fetch1()   # as a dictionary
        a, b = rel.fetch1('a', 'b')   # as a tuple

        :params *attrs: attributes to return when expanding into a tuple.
                 If attrs is empty, the return result is a dict
        :param squeeze:  When true, remove extra dimensions from arrays in attributes
        :param download_path: for fetches that download data, e.g. attachments
        :return: the one tuple in the table in the form of a dict
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
