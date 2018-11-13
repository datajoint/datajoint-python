from collections import OrderedDict
from functools import partial
import numpy as np
from .blob import unpack
from .errors import DataJointError
from . import key as PRIMARY_KEY
import warnings


def is_key(attr):
    return attr is PRIMARY_KEY or attr == 'KEY'


def to_dicts(recarray):
    """convert record array to a dictionaries"""
    for rec in recarray:
        yield dict(zip(recarray.dtype.names, rec.tolist()))


class Fetch:
    """
    A fetch object that handles retrieving elements from the table expression.
    :param relation: the table expression to fetch from
    """

    def __init__(self, expression):
        self._expression = expression

    def __call__(self, *attrs, offset=None, limit=None, order_by=None, as_dict=False, squeeze=False):
        """
        Fetches the expression results from the database into an np.array or list of dictionaries and unpacks blob attributes.

        :param attrs: zero or more attributes to fetch. If not provided, the call will return
        all attributes of this relation. If provided, returns tuples with an entry for each attribute.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param as_dict: returns a list of dictionaries instead of a record array
        :param squeeze:  if True, remove extra dimensions from arrays
        :return: the contents of the relation in the form of a structured numpy.array or a dict list
        """

        # if 'order_by' passed in a string, make into list
        if isinstance(order_by, str):
            order_by = [order_by]

        # if attrs are specified then as_dict cannot be true
        if attrs and as_dict:
            raise DataJointError('Cannot specify attributes to return when as_dict=True. '
                                 'Use proj() to select attributes or set as_dict=False')

        if limit is None and offset is not None:
            warnings.warn('Offset set, but no limit. Setting limit to a large number. '
                          'Consider setting a limit explicitly.')
            limit = 2 * len(self._expression)

        if not attrs:
            # fetch all attributes
            cur = self._expression.cursor(as_dict=as_dict, limit=limit, offset=offset, order_by=order_by)
            heading = self._expression.heading
            if as_dict:
                ret = [OrderedDict((name, unpack(d[name], squeeze=squeeze) if heading[name].is_blob else d[name])
                                   for name in heading.names)
                       for d in cur]
            else:
                ret = list(cur.fetchall())
                ret = np.array(ret, dtype=heading.as_dtype)
                for name in heading:
                    if heading[name].is_external:
                        external_table = self._expression.connection.schemas[heading[name].database].external_table
                        ret[name] = list(map(external_table.get, ret[name]))
                    elif heading[name].is_blob:
                        ret[name] = list(map(partial(unpack, squeeze=squeeze), ret[name]))
        else:  # if list of attributes provided
            attributes = [a for a in attrs if not is_key(a)]
            result = self._expression.proj(*attributes).fetch(
                offset=offset, limit=limit, order_by=order_by, as_dict=False, squeeze=squeeze)
            return_values = [
                list(to_dicts(result[self._expression.primary_key]))
                if is_key(attribute) else result[attribute]
                for attribute in attrs]
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret

    def keys(self, **kwargs):
        """
        DEPRECATED
        Iterator that returns primary keys as a sequence of dicts.
        """
        warnings.warn('Use of `rel.fetch.keys()` notation is deprecated. '
                      'Please use `rel.fetch("KEY")` or `rel.fetch(dj.key)` for equivalent result', stacklevel=2)
        yield from self._expression.proj().fetch(as_dict=True, **kwargs)


class Fetch1:
    """
    Fetch object for fetching exactly one row.
    :param relation: relation the fetch object fetches data from
    """

    def __init__(self, relation):
        self._expression = relation

    def __call__(self, *attrs, squeeze=False):
        """
        Fetches the expression results from the database when the expression is known to yield only one entry.

        If no attributes are specified, returns the result as a dict.
        If attributes are specified returns the corresponding results as a tuple.

        Examples:
        d = rel.fetch1()   # as a dictionary
        a, b = rel.fetch1('a', 'b')   # as a tuple

        :params *attrs: attributes to return when expanding into a tuple. If empty, the return result is a dict
        :param squeeze:  When true, remove extra dimensions from arrays in attributes
        :return: the one tuple in the relation in the form of a dict
        """

        heading = self._expression.heading

        if not attrs:  # fetch all attributes, return as ordered dict
            cur = self._expression.cursor(as_dict=True)
            ret = cur.fetchone()
            if not ret or cur.fetchone():
                raise DataJointError('fetch1 should only be used for relations with exactly one tuple')

            def get_external(attr, _hash):
                return self._expression.connection.schemas[attr.database].external_table.get(_hash)

            ret = OrderedDict((name, get_external(heading[name], ret[name])) if heading[name].is_external
                              else (name, unpack(ret[name], squeeze=squeeze) if heading[name].is_blob else ret[name])
                              for name in heading.names)
        else:  # fetch some attributes, return as tuple
            attributes = [a for a in attrs if not is_key(a)]
            result = self._expression.proj(*attributes).fetch(squeeze=squeeze)
            if len(result) != 1:
                raise DataJointError('fetch1 should only return one tuple. %d tuples were found' % len(result))
            return_values = tuple(
                next(to_dicts(result[self._expression.primary_key]))
                if is_key(attribute) else result[attribute][0]
                for attribute in attrs)
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret
