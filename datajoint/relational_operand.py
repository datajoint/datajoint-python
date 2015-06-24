"""
classes for relational algebra
"""

import numpy as np
import abc
import re
from collections import OrderedDict
from copy import copy
from datajoint import DataJointError, config
import logging

from .blob import unpack

logger = logging.getLogger(__name__)


class RelationalOperand(metaclass=abc.ABCMeta):
    """
    RelationalOperand implements relational algebra and fetch methods.
    RelationalOperand objects reference other relation objects linked by operators.
    The leaves of this tree of objects are base relations.
    When fetching data from the database, this tree of objects is compiled into an SQL expression.
    It is a mixin class that provides relational operators, iteration, and fetch capability.
    RelationalOperand operators are: restrict, pro, and join.
    """

    _restrictions = None

    @property
    def restrictions(self):
        return [] if self._restrictions is None else self._restrictions

    @property
    def primary_key(self):
        return self.heading.primary_key

    # --------- abstract properties -----------

    @property
    @abc.abstractmethod
    def connection(self):
        """
        :return: a datajoint.Connection object
        """
        pass

    @property
    @abc.abstractmethod
    def from_clause(self):
        """
        :return: a string containing the FROM clause of the SQL SELECT statement
        """
        pass

    @property
    @abc.abstractmethod
    def heading(self):
        """
        :return: a valid datajoint.Heading object
        """
        pass

    # --------- relational operators -----------

    def __mul__(self, other):
        """
        relational join
        """
        return Join(self, other)

    def __mod__(self, attributes=None):
        """
        relational projection operator.  See RelationalOperand.project
        """
        return self.project(*attributes)

    def project(self, *attributes, **renamed_attributes):
        """
        Relational projection operator.
        :param attributes: a list of attribute names to be included in the result.
        :return: a new relation with selected fields
        Primary key attributes are always selected and cannot be excluded.
        Therefore obj.project() produces a relation with only the primary key attributes.
        If attributes includes the string '*', all attributes are selected.
        Each attribute can only be used once in attributes or renamed_attributes.  Therefore, the projected
        relation cannot have more attributes than the original relation.
        """
        # if the first attribute is a relation, it will be aggregated
        group = None
        if attributes and isinstance(attributes[0], RelationalOperand):
            group = attributes.pop[0]
        return Projection(self, group, *attributes, **renamed_attributes)

    def aggregate(self, _group, *attributes, **renamed_attributes):
        """
        Relational aggregation operator
        :param group:  relation whose tuples can be used in aggregation operators
        :param extensions:
        :return: a relation representing the aggregation/projection operator result
        """
        if _group is not None and not isinstance(_group, RelationalOperand):
            raise DataJointError('The second argument must be a relation or None')
        return Projection(self, _group, *attributes, **renamed_attributes)

    def __iand__(self, restriction):
        """
        in-place relational restriction or semijoin
        """
        if restriction is not None:
            if self._restrictions is None:
                self._restrictions = []
            self._restrictions.append(restriction)
        return self

    def __and__(self, restriction):
        """
        relational restriction or semijoin
        """
        ret = copy(self)
        ret._restrictions = list(ret.restrictions)  # copy restriction list
        ret &= restriction
        return ret

    def __isub__(self, restriction):
        """
        in-place antijoin (inverted restriction)
        """
        self &= Not(restriction)
        return self

    def __sub__(self, restriction):
        """
        inverted restriction aka antijoin
        """
        return self & Not(restriction)

    # ------ data retrieval methods -----------

    def make_select(self, attribute_spec=None):
        if attribute_spec is None:
            attribute_spec = self.heading.as_sql
        return 'SELECT ' + attribute_spec + ' FROM ' + self.from_clause + self.where_clause

    def __len__(self):
        """
        number of tuples in the relation.  This also takes care of the truth value
        """
        cur = self.connection.query(self.make_select('count(*)'))
        return cur.fetchone()[0]

    def __contains__(self, item):
        """
        "item in relation" is equivalient to "len(relation & item)>0"
        """
        return len(self & item) > 0

    def __call__(self, *args, **kwargs):
        """
        calling a relation is equivalent to fetching from it
        """
        return self.fetch(*args, **kwargs)

    def fetch1(self):
        """
        This version of fetch is called when self is expected to contain exactly one tuple.
        :return: the one tuple in the relation in the form of a dict
        """
        heading = self.heading
        cur = self.cursor(as_dict=True)
        ret = cur.fetchone()
        if not ret or cur.fetchone():
            raise DataJointError('fetch1 should only be used for relations with exactly one tuple')
        return OrderedDict((name, unpack(ret[name]) if heading[name].is_blob else ret[name])
                           for name in self.heading.names)

    def fetch(self, offset=0, limit=None, order_by=None, descending=False, as_dict=False):
        """
        fetches the relation from the database table into an np.array and unpacks blob attributes.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param descending: the list of attributes to order the results
        :param as_dict: returns a list of dictionaries instead of a record array
        :return: the contents of the relation in the form of a structured numpy.array
        """
        cur = self.cursor(offset=offset, limit=limit, order_by=order_by,
                          descending=descending, as_dict=as_dict)
        heading = self.heading
        if as_dict:
            ret = [OrderedDict((name, unpack(d[name]) if heading[name].is_blob else d[name])
                               for name in self.heading.names)
                   for d in cur.fetchall()]
        else:
            ret = np.array(list(cur.fetchall()), dtype=heading.as_dtype)
            for blob_name in heading.blobs:
                ret[blob_name] = list(map(unpack, ret[blob_name]))
        return ret

    def cursor(self, offset=0, limit=None, order_by=None, descending=False, as_dict=False):
        """
        Return query cursor.
        See Relation.fetch() for input description.
        :return: cursor to the query
        """
        if offset and limit is None:
            raise DataJointError('limit is required when offset is set')
        sql = self.make_select()
        if order_by is not None:
            sql += ' ORDER BY ' + ', '.join(order_by)
            if descending:
                sql += ' DESC'
        if limit is not None:
            sql += ' LIMIT %d' % limit
            if offset:
                sql += ' OFFSET %d' % offset
        logger.debug(sql)
        return self.connection.query(sql, as_dict=as_dict)

    def __repr__(self):
        limit = config['display.limit']
        width = config['display.width']
        rel = self.project(*self.heading.non_blobs)  # project out blobs
        template = '%%-%d.%ds' % (width, width)
        columns = rel.heading.names
        repr_string = ' '.join([template % column for column in columns]) + '\n'
        repr_string += ' '.join(['+' + '-' * (width - 2) + '+' for _ in columns]) + '\n'
        for tup in rel.fetch(limit=limit):
            repr_string += ' '.join([template % column for column in tup]) + '\n'
        if len(self) > limit:
            repr_string += '...\n'
        repr_string += ' (%d tuples)\n' % len(self)
        return repr_string

    def __iter__(self):
        """
        Iterator that yields individual tuples of the current table dictionaries.
        """
        cur = self.cursor()
        heading = self.heading  # construct once for efficiency
        do_unpack = tuple(h in heading.blobs for h in heading.names)
        values = cur.fetchone()
        while values:
            yield {field_name: unpack(value) if up else value
                   for field_name, up, value in zip(heading.names, do_unpack, values)}
            values = cur.fetchone()

    @property
    def where_clause(self):
        """
        convert the restriction into an SQL WHERE
        """
        if not self.restrictions:
            return ''

        def make_condition(arg):
            if isinstance(arg, dict):
                conditions = ['`%s`=%s' % (k, repr(v)) for k, v in arg.items() if k in self.heading]
            elif isinstance(arg, np.void):
                conditions = ['`%s`=%s' % (k, arg[k]) for k in arg.dtype.fields]
            else:
                raise DataJointError('invalid restriction type')
            return ' AND '.join(conditions)

        condition_string = []
        for r in self.restrictions:
            negate = isinstance(r, Not)
            if negate:
                r = r.restriction
            if isinstance(r, dict) or isinstance(r, np.void):
                r = make_condition(r)
            elif isinstance(r, np.ndarray) or isinstance(r, list):
                r = '(' + ') OR ('.join([make_condition(q) for q in r]) + ')'
            elif isinstance(r, RelationalOperand):
                common_attributes = ','.join([q for q in self.heading.names if r.heading.names])
                r = '(%s) in (SELECT %s FROM %s%s)' % (
                    common_attributes, common_attributes, r.from_clause, r.where_clause)

            assert isinstance(r, str), 'condition must be converted into a string'
            r = '(' + r + ')'
            if negate:
                r = 'NOT ' + r
            condition_string.append(r)

        return ' WHERE ' + ' AND '.join(condition_string)


class Not:
    """
    inverse restriction
    """

    def __init__(self, restriction):
        self.restriction = restriction


class Join(RelationalOperand):
    __counter = 0

    def __init__(self, arg1, arg2):
        if not isinstance(arg2, RelationalOperand):
            raise DataJointError('a relation can only be joined with another relation')
        if arg1.connection != arg2.connection:
            raise DataJointError('Cannot join relations with different database connections')
        self._arg1 = Subquery(arg1) if arg1.heading.computed else arg1
        self._arg2 = Subquery(arg1) if arg2.heading.computed else arg2
        self._restrictions = self._arg1.restrictions + self._arg2.restrictions

    @property
    def connection(self):
        return self._arg1.connection

    @property
    def counter(self):
        self.__counter += 1
        return self.__counter

    @property
    def heading(self):
        return self._arg1.heading + self._arg2.heading

    @property
    def from_clause(self):
        return '%s NATURAL JOIN %s' % (self._arg1.from_clause, self._arg2.from_clause)


class Projection(RelationalOperand):
    def __init__(self, arg, group=None, *attributes, **renamed_attributes):
        """
        See RelationalOperand.project()
        """
        alias_parser = re.compile(
            '^\s*(?P<sql_expression>\S(.*\S)?)\s*->\s*(?P<alias>[a-z][a-z_0-9]*)\s*$')
        # expand extended attributes in the form 'sql_expression -> new_attribute'
        self._attributes = []
        self._renamed_attributes = renamed_attributes
        for attribute in attributes:
            alias_match = alias_parser.match(attribute)
            if alias_match:
                d = alias_match.groupdict()
                self._renamed_attributes.update({d['alias']: d['sql_expression']})
            else:
                self._attributes.append(attribute)
        if group:
            if arg.connection != group.connection:
                raise DataJointError('Cannot join relations with different database connections')
            self._group = Subquery(group)
            self._arg = Subquery(arg)
        else:
            self._group = None
            if arg.heading.computed:
                self._arg = Subquery(arg)
            else:
                # project without subquery
                self._arg = arg
                self._restrictions = self._arg.restrictions

    @property
    def connection(self):
        return self._arg.connection

    @property
    def heading(self):
        return self._arg.heading.project(*self._attributes, **self._renamed_attributes)

    @property
    def from_clause(self):
        if self._group is None:
            return self._arg.from_clause
        else:
            return "(%s) NATURAL LEFT JOIN (%s) GROUP BY `%s`" % (
                self._arg.from_clause, self._group.from_clause,
                '`,`'.join(self.heading.primary_key))


class Subquery(RelationalOperand):
    """
    A Subquery encapsulates its argument in a SELECT statement, enabling its use as a subquery.
    The attribute list and the WHERE clause are resolved.
    """
    __counter = 0

    def __init__(self, arg):
        self._arg = arg

    @property
    def connection(self):
        return self._arg.connection

    @property
    def counter(self):
        Subquery.__counter += 1
        return Subquery.__counter

    @property
    def from_clause(self):
        return '(' + self._arg.make_select() + ') as `_s%x`' % self.counter

    @property
    def heading(self):
        return self._arg.heading.resolve()
