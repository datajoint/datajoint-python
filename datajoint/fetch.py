from collections import OrderedDict
import itertools
from .blob import unpack
import numpy as np
from datajoint import DataJointError
from . import key as PRIMARY_KEY
from collections import abc


def prepare_attributes(relation, item):
    if isinstance(item, str) or item is PRIMARY_KEY:
        item = (item,)
    elif isinstance(item, int):
        item = (relation.heading.names[item],)
    elif isinstance(item, slice):
        attributes = relation.heading.names
        start = attributes.index(item.start) if isinstance(item.start, str) else item.start
        stop = attributes.index(item.stop) if isinstance(item.stop, str) else item.stop
        item = attributes[slice(start, stop, item.step)]
    try:
        attributes = tuple(i for i in item if i is not PRIMARY_KEY)
    except TypeError:
        raise DataJointError("Index must be a slice, a tuple, a list, a string.")
    return item, attributes


class Fetch:
    def __init__(self, relation):
        """

        """

        self.behavior = dict(
            offset=0, limit=None, order_by=None, as_dict=False
        )
        self._relation = relation

    def from_to(self, fro, to):
        self.behavior['offset'] = fro
        self.behavior['limit'] = to - fro
        return self

    def order_by(self, *args):

        if len(args) > 0:
            self.behavior['order_by'] = self.behavior['order_by'] if self.behavior['order_by'] is not None else []
            self.behavior['order_by'].extend(args)
        return self

    def as_dict(self):
        self.behavior['as_dict'] = True


    def limit_to(self, limit):
        self.behavior['limit'] = limit
        return self

    def set_behavior(self, **kwargs):
        self.behavior.update(kwargs)
        return self

    def __call__(self, **kwargs):
        """
        Fetches the relation from the database table into an np.array and unpacks blob attributes.

        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param descending: the list of attributes to order the results
        :param as_dict: returns a list of dictionaries instead of a record array
        :return: the contents of the relation in the form of a structured numpy.array

        """
        behavior = dict(self.behavior, **kwargs)

        cur = self._relation.cursor(**behavior)

        heading = self._relation.heading
        if behavior['as_dict']:
            ret = [OrderedDict((name, unpack(d[name]) if heading[name].is_blob else d[name])
                               for name in heading.names)
                   for d in cur.fetchall()]
        else:
            ret = np.array(list(cur.fetchall()), dtype=heading.as_dtype)
            for blob_name in heading.blobs:
                ret[blob_name] = list(map(unpack, ret[blob_name]))

        return ret

    def __iter__(self):
        """
        Iterator that returns the contents of the database.
        """
        behavior = dict(self.behavior)

        cur = self._relation.cursor(**behavior)

        heading = self._relation.heading
        do_unpack = tuple(h in heading.blobs for h in heading.names)
        values = cur.fetchone()
        while values:
            if behavior['as_dict']:
                yield OrderedDict(
                    (field_name, unpack(values[field_name])) if up
                    else (field_name, values[field_name])
                    for field_name, up in zip(heading.names, do_unpack))
            else:
                yield tuple(unpack(value) if up else value for up, value in zip(do_unpack, values))
            values = cur.fetchone()

    def keys(self, **kwargs):
        """
        Iterator that returns primary keys.
        """
        b = dict(self.behavior, **kwargs)
        if 'as_dict' not in kwargs:
            b['as_dict'] = True
        yield from self._relation.project().fetch.set_behavior(**b)

    def __getitem__(self, item):
        """
        Fetch attributes as separate outputs.
        datajoint.key is a special value that requests the entire primary key
        :return: tuple with an entry for each element of item

        Examples:
        a, b = relation['a', 'b']
        a, b, key = relation['a', 'b', datajoint.key]
        results = relation['a':'z']    # return attributes a-z as a tuple
        results = relation[:-1]   # return all but the last attribute
        """
        single_output = isinstance(item, str) or item is PRIMARY_KEY or isinstance(item, int)
        item, attributes = prepare_attributes(self._relation, item)

        result = self._relation.project(*attributes).fetch(**self.behavior)
        return_values = [
            np.ndarray(result.shape,
                       np.dtype({name: result.dtype.fields[name] for name in self._relation.primary_key}),
                       result, 0, result.strides)
            if attribute is PRIMARY_KEY
            else result[attribute]
            for attribute in item
            ]
        return return_values[0] if single_output else return_values


class Fetch1:
    def __init__(self, relation):
        self._relation = relation

    def __call__(self):
        """
        This version of fetch is called when self is expected to contain exactly one tuple.
        :return: the one tuple in the relation in the form of a dict
        """
        heading = self._relation.heading

        cur = self._relation.cursor(as_dict=True)
        ret = cur.fetchone()
        if not ret or cur.fetchone():
            raise DataJointError('fetch1 should only be used for relations with exactly one tuple')

        return OrderedDict((name, unpack(ret[name]) if heading[name].is_blob else ret[name])
                           for name in heading.names)

    def __getitem__(self, item):
        """
        Fetch attributes as separate outputs.
        datajoint.key is a special value that requests the entire primary key
        :return: tuple with an entry for each element of item

        Examples:
        a, b = relation['a', 'b']
        a, b, key = relation['a', 'b', datajoint.key]
        results = relation['a':'z']    # return attributes a-z as a tuple
        results = relation[:-1]   # return all but the last attribute
        """
        single_output = isinstance(item, str) or item is PRIMARY_KEY or isinstance(item, int)
        item, attributes = prepare_attributes(self._relation, item)

        result = self._relation.project(*attributes).fetch()
        return_values = tuple(
            np.ndarray(result.shape,
                       np.dtype({name: result.dtype.fields[name] for name in self._relation.primary_key}),
                       result, 0, result.strides)
            if attribute is PRIMARY_KEY
            else result[attribute][0]
            for attribute in item
        )
        return return_values[0] if single_output else return_values
