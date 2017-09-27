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

    def copy(self):
        """
        DEPRECATED

        Creates and returns a copy of this object
        :return: copy FetchBase derivatives
        """
        warnings.warn('Use of `copy` on `fetch` object is deprecated', stacklevel=2)

        return self.__class__(self)

    def _initialize_behavior(self):
        self.sql_behavior = {}
        self.ext_behavior = dict(squeeze=False)

    @property
    def squeeze(self):
        """
        DEPRECATED

        Changes the state of the fetch object to squeeze the returned values as much as possible.
        :return: a copy of the fetch object
        """

        warnings.warn('Use of `squeeze` on `fetch` object is deprecated. Please use `squeeze=True` keyword arguments '
                      'in the call to `fetch`/`keys` instead', stacklevel=2)

        ret = self.copy()
        ret.ext_behavior['squeeze'] = True
        return ret

    @staticmethod
    def _prepare_attributes(item):
        """
        Used by fetch.__getitem__ to deal with slices
        :param item: the item passed to __getitem__. Can be a string, a tuple, a list, or a slice.
        :return: a tuple of items to fetch, a list of the corresponding attributes
        :raise DataJointError: if item does not match one of the datatypes above
        """
        if isinstance(item, str) or item is PRIMARY_KEY:
            item = (item,)
        try:
            attributes = tuple(i for i in item if i is not PRIMARY_KEY)
        except TypeError:
            raise DataJointError("Index must be a sequence or a string.")
        return item, attributes

    def __len__(self):
        return len(self._relation)


class Fetch(FetchBase, Callable, Iterable):
    """
    A fetch object that handles retrieving elements from the database table.

    :param relation: relation the fetch object fetches data from
    """

    def _initialize_behavior(self):
        super()._initialize_behavior()
        self.sql_behavior = dict(self.sql_behavior, offset=None, limit=None, order_by=None, as_dict=False)

    def order_by(self, *args):
        """
        DEPRECATED

        Changes the state of the fetch object to order the results by a particular attribute.
        The commands are handed down to mysql.
        :param args: the attributes to sort by. If DESC is passed after the name, then the order is descending.
        :return: a copy of the fetch object
        Example:

        >>> my_relation.fetch.order_by('language', 'name DESC')

        """
        warnings.warn('Use of `order_by` on `fetch` object is deprecated. Please use `order_by` keyword arguments in '
                      'the call to `fetch`/`keys` instead', stacklevel=2)
        self = Fetch(self)
        if len(args) > 0:
            self.sql_behavior['order_by'] = args
        return self

    @property
    def as_dict(self):
        """
        DEPRECATED

        Changes the state of the fetch object to return dictionaries.
        :return: a copy of the fetch object
        Example:

        >>> my_relation.fetch.as_dict()

        """
        warnings.warn('Use of `as_dict` on `fetch` object is deprecated. Please use `as_dict` keyword arguments in the '
                      'call to `fetch`/`keys` instead', stacklevel=2)
        ret = Fetch(self)
        ret.sql_behavior['as_dict'] = True
        return ret

    def limit(self, limit):
        """
        DEPRECATED

        Limits the number of items fetched.

        :param limit: limit on the number of items
        :return: a copy of the fetch object
        """
        warnings.warn('Use of `limit` on `fetch` object is deprecated. Please use `limit` keyword arguments in '
                      'the call to `fetch`/`keys` instead', stacklevel=2)
        ret = Fetch(self)
        ret.sql_behavior['limit'] = limit
        return ret

    def offset(self, offset):
        """
        DEPRECATED

        Offsets the number of itms fetched. Needs to be applied with limit.

        :param offset: offset
        :return: a copy of the fetch object
        """

        warnings.warn('Use of `offset` on `fetch` object is deprecated. Please use `offset` keyword arguments in '
                      'the call to `fetch`/`keys` instead', stacklevel=2)
        ret = Fetch(self)
        if ret.sql_behavior['limit'] is None:
            warnings.warn('Fetch offset should be used with a limit.')
        ret.sql_behavior['offset'] = offset
        return ret

    def __call__(self, *attrs, **kwargs):
        """
        Fetches the relation from the database table into an np.array and unpacks blob attributes.

        :param attrs: OPTIONAL. one or more attributes to fetch. If not provided, the call will return
        all attributes of this relation. If provided, returns tuples with an entry for each attribute.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param as_dict: returns a list of dictionaries instead of a record array
        :return: the contents of the relation in the form of a structured numpy.array
        """
        # if 'order_by' passed in a string, make into list
        if isinstance(kwargs.get('order_by'), str):
            kwargs['order_by'] = [kwargs['order_by']]

        sql_behavior = update_dict(self.sql_behavior, kwargs)
        ext_behavior = update_dict(self.ext_behavior, kwargs)
        total_behavior = dict(sql_behavior)
        total_behavior.update(ext_behavior)

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
                for blob_name in heading.blobs:
                    ret[blob_name] = list(map(unpack_, ret[blob_name]))

        else:  # if list of attributes provided
            attributes = [a for a in attrs if a is not PRIMARY_KEY]
            result = self._relation.proj(*attributes).fetch(**total_behavior)
            return_values = [
                list(to_dicts(result[self._relation.primary_key]))
                if attribute is PRIMARY_KEY else result[attribute]
                for attribute in attrs]
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret

    def __iter__(self):
        """
        Iterator that returns the contents of the database.
        """
        sql_behavior = dict(self.sql_behavior)
        ext_behavior = dict(self.ext_behavior)

        unpack_ = partial(unpack, squeeze=ext_behavior['squeeze'])

        cur = self._relation.cursor(**sql_behavior)

        heading = self._relation.heading
        do_unpack = tuple(h in heading.blobs for h in heading.names)
        values = cur.fetchone()
        while values:
            if sql_behavior['as_dict']:
                yield OrderedDict(
                    (field_name, unpack_(values[field_name])) if up
                    else (field_name, values[field_name])
                    for field_name, up in zip(heading.names, do_unpack))
            else:
                yield tuple(unpack_(value) if up else value for up, value in zip(do_unpack, values))
            values = cur.fetchone()

    def keys(self, **kwargs):
        """
        Iterator that returns primary keys as a sequence of dicts.
        """
        yield from self._relation.proj().fetch(**dict(self.sql_behavior, as_dict=True, **kwargs))

    def __getitem__(self, item):
        """
        DEPRECATED

        Fetch attributes as separate outputs.
        datajoint.key is a special value that requests the entire primary key
        :return: tuple with an entry for each element of item

        Examples:
        >>> a, b = relation['a', 'b']
        >>> a, b, key = relation['a', 'b', datajoint.key]
        """

        warnings.warn('Use of `rel.fetch[a, b]` notation is deprecated. Please use `rel.fetch(a, b) for equivalent '
                      'result', stacklevel=2)

        behavior = dict(self.sql_behavior)
        behavior.update(self.ext_behavior)

        single_output = isinstance(item, str) or item is PRIMARY_KEY or isinstance(item, int)
        item, attributes = self._prepare_attributes(item)
        result = self._relation.proj(*attributes).fetch(**behavior)
        return_values = [
            list(to_dicts(result[self._relation.primary_key]))
            if attribute is PRIMARY_KEY else result[attribute]
            for attribute in item]
        return return_values[0] if single_output else return_values

    def __repr__(self):
        repr_str = """Fetch object for {items} items on {name}\n""".format(name=self._relation.__class__.__name__,
                                                                           items=len(self._relation)    )
        behavior = dict(self.sql_behavior)
        behavior.update(self.ext_behavior)
        repr_str += '\n'.join(
            ["\t{key}:\t{value}".format(key=k, value=str(v)) for k, v in behavior.items() if v is not None])
        return repr_str


class Fetch1(FetchBase, Callable):
    """
    Fetch object for fetching exactly one row.

    :param relation: relation the fetch object fetches data from
    """

    def __call__(self, *attrs, **kwargs):
        """
        This version of fetch is called when self is expected to contain exactly one tuple.
        :return: the one tuple in the relation in the form of a dict
        """
        heading = self._relation.heading
        ext_behavior = update_dict(self.ext_behavior, kwargs)
        unpack_ = partial(unpack, squeeze=ext_behavior['squeeze'])

        if len(attrs) == 0:  # fetch all attributes
            cur = self._relation.cursor(as_dict=True)
            ret = cur.fetchone()
            if not ret or cur.fetchone():
                raise DataJointError('fetch1 should only be used for relations with exactly one tuple')
            ret = OrderedDict((name, unpack_(ret[name]) if heading[name].is_blob else ret[name])
                               for name in heading.names)
        else:
            attributes = [a for a in attrs if a is not PRIMARY_KEY]
            result = self._relation.proj(*attributes).fetch(**ext_behavior)
            if len(result) != 1:
                raise DataJointError('fetch1 should only return one tuple. %d tuples were found' % len(result))
            return_values = tuple(
                next(to_dicts(result[self._relation.primary_key]))
                if attribute is PRIMARY_KEY else result[attribute][0]
                for attribute in attrs)
            ret = return_values[0] if len(attrs) == 1 else return_values

        return ret

    def __getitem__(self, item):
        """
        DEPRECATED

        Fetch attributes as separate outputs.
        datajoint.key is a special value that requests the entire primary key
        :return: tuple with an entry for each element of item

        Examples:

        >>> a, b = relation['a', 'b']
        >>> a, b, key = relation['a', 'b', datajoint.key]

        """
        warnings.warn('Use of `rel.fetch[a, b]` notation is deprecated. Please use `rel.fetch(a, b) for equivalent '
                      'result', stacklevel=2)

        behavior = dict(self.sql_behavior)
        behavior.update(self.ext_behavior)

        single_output = isinstance(item, str) or item is PRIMARY_KEY
        item, attributes = self._prepare_attributes(item)
        result = self._relation.proj(*attributes).fetch(**behavior)
        if len(result) != 1:
            raise DataJointError('fetch1 should only return one tuple. %d tuples were found' % len(result))
        return_values = tuple(
            next(to_dicts(result[self._relation.primary_key]))
            if attribute is PRIMARY_KEY else result[attribute][0]
            for attribute in item)
        return return_values[0] if single_output else return_values


def to_dicts(recarray):
    for rec in recarray:
        yield dict(zip(recarray.dtype.names, rec.tolist()))
