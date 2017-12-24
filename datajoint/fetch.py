from collections import OrderedDict
from collections.abc import Callable, Iterable
from functools import partial
import numpy as np
from .blob import unpack
from . import DataJointError
from . import key as PRIMARY_KEY
import warnings


def update_dict(d1, d2):
    return {k: (d2[k] if k in d2 else d1[k]) for k in d1}


def iskey(attr):
    return attr is PRIMARY_KEY or attr == 'KEY'


class FetchBase:
    def __init__(self, arg):
        # prepare copy constructor
        if isinstance(arg, self.__class__):
            self.sql_behavior = dict(arg.sql_behavior)
            self.ext_behavior = dict(arg.ext_behavior)
            self._relation = arg._relation
        else:
            self._initialize_behavior()
            self._relation = arg


    def _initialize_behavior(self):
        self.sql_behavior = {}
        self.ext_behavior = dict(squeeze=False)



class Fetch(FetchBase, Callable):
    """
    A fetch object that handles retrieving elements from the database table.

    :param relation: relation the fetch object fetches data from
    """

    def _initialize_behavior(self):
        super()._initialize_behavior()
        self.sql_behavior = dict(self.sql_behavior, offset=None, limit=None, order_by=None, as_dict=False)

    def __call__(self, *attrs, **kwargs):
        """
        Fetches the query results from the database into an np.array or list of dictionaries and unpacks blob attributes.

        :param attrs: OPTIONAL. one or more attributes to fetch. If not provided, the call will return
        all attributes of this relation. If provided, returns tuples with an entry for each attribute.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param as_dict: returns a list of dictionaries instead of a record array
        :param squeeze:  if True, remove extra dimensions from arrays
        :return: the contents of the relation in the form of a structured numpy.array or a dict list
        """

        # check unexpected arguments
        try:
            raise TypeError("fetch() got an unexpected argument '%s'" % next(
                k for k in kwargs if k not in {'offset', 'limit', 'as_dict', 'squeeze', 'order_by'}))
        except StopIteration:
            pass   # arguments are okay

        # if 'order_by' passed in a string, make into list
        if isinstance(kwargs.get('order_by'), str):
            kwargs['order_by'] = [kwargs['order_by']]

        sql_behavior = update_dict(self.sql_behavior, kwargs)
        ext_behavior = update_dict(self.ext_behavior, kwargs)
        total_behavior = dict(sql_behavior)
        total_behavior.update(ext_behavior)

        # if attrs are specified then as_dict cannot be true
        if attrs and sql_behavior['as_dict']:
            raise DataJointError('Cannot specify attributes to return when as_dict=True. '
                                 'Use proj() to select attributes or set as_dict=False')

        unpack_ = partial(unpack, squeeze=ext_behavior['squeeze'])

        if sql_behavior['limit'] is None and sql_behavior['offset'] is not None:
            warnings.warn('Offset set, but no limit. Setting limit to a large number. '
                          'Consider setting a limit explicitly.')
            sql_behavior['limit'] = 2 * len(self._relation)

        if len(attrs) == 0: # fetch all attributes
            cur = self._relation.cursor(**sql_behavior)
            heading = self._relation.heading
            if sql_behavior['as_dict']:
                ret = [OrderedDict((name, unpack_(d[name]) if heading[name].is_blob else d[name])
                                   for name in heading.names)
                       for d in cur.fetchall()]
            else:
                ret = list(cur.fetchall())
                ret = np.array(ret, dtype=heading.as_dtype)
                for name in heading:
                    if heading[name].is_external:
                        external_table = self._relation.connection.schemas[heading[name].database].external_table
                        ret[name] = list(map(external_table.get, ret[name]))
                    elif heading[name].is_blob:
                        ret[name] = list(map(unpack_, ret[name]))

        else:  # if list of attributes provided
            attributes = [a for a in attrs if not iskey(a)]
            result = self._relation.proj(*attributes).fetch(**total_behavior)
            return_values = [
                list(to_dicts(result[self._relation.primary_key]))
                if iskey(attribute) else result[attribute]
                for attribute in attrs]
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret

    def keys(self, **kwargs):
        """
        Iterator that returns primary keys as a sequence of dicts.
        """
        yield from self._relation.proj().fetch(**dict(self.sql_behavior, as_dict=True, **kwargs))


class Fetch1(FetchBase, Callable):
    """
    Fetch object for fetching exactly one row.

    :param relation: relation the fetch object fetches data from
    """

    def __call__(self, *attrs, squeeze=False):
        """
        Fetches the query results from the database when the query is known to contain only one entry.

        If no attributes are specified, returns the result as a dict.
        If attributes are specified returns the corresponding results as a tuple.

        Examples:
        d = rel.fetch1()   # as a dictionary
        a, b = rel.fetch1('a', 'b')   # as a tuple

        :params *attrs: attributes to return when expanding into a tuple. If empty, the return result is a dict
        :param squeeze:  When true, remove extra dimensions from arrays in attributes
        :return: the one tuple in the relation in the form of a dict
        """

        heading = self._relation.heading
        squeeze = squeeze or self.ext_behavior['squeeze']  # for backward compatibility
        unpack_ = partial(unpack, squeeze=squeeze)

        if not attrs:  # fetch all attributes, return as ordered dict
            cur = self._relation.cursor(as_dict=True)
            ret = cur.fetchone()
            if not ret or cur.fetchone():
                raise DataJointError('fetch1 should only be used for relations with exactly one tuple')

            def get_external(attr, _hash):
                return self._relation.connection.schemas[attr.database].external_table.get(_hash)

            ret = OrderedDict((name, get_external(heading[name], ret[name])) if heading[name].is_external
                              else (name, unpack_(ret[name]) if heading[name].is_blob else ret[name])
                              for name in heading.names)

        else:  # fetch some attributes, return as tuple
            attributes = [a for a in attrs if not iskey(a)]
            result = self._relation.proj(*attributes).fetch(squeeze=squeeze)
            if len(result) != 1:
                raise DataJointError('fetch1 should only return one tuple. %d tuples were found' % len(result))
            return_values = tuple(
                next(to_dicts(result[self._relation.primary_key]))
                if iskey(attribute) else result[attribute][0]
                for attribute in attrs)
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret


def to_dicts(recarray):
    for rec in recarray:
        yield dict(zip(recarray.dtype.names, rec.tolist()))
