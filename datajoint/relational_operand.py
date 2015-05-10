"""
classes for relational algebra
"""

import numpy as np
import abc
import re
from copy import copy
from datajoint import DataJointError
from .blob import unpack
import logging
import numpy.lib.recfunctions as rfn

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
    _restrictions = []

    @abc.abstractproperty
    def sql(self):
        """
        The sql property returns the tuple: (SQL command, Heading object) for its relation.
        The SQL command does not include the attribute list or the WHERE clause.
        :return:  sql, heading
        """
        pass

    @abc.abstractproperty
    def conn(self):
        """
        All relations must keep track of their connection object
        :return:
        """
        pass

    @property
    def restrictions(self):
        return self._restrictions
            
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
        group = attributes.pop[0] \
            if attributes and isinstance(attributes[0], RelationalOperand) else None
        return self.aggregate(group, *attributes, **renamed_attributes)

    def aggregate(self, _group, *attributes, **renamed_attributes):
        """
        Relational aggregation operator
        :param group:  relation whose tuples can be used in aggregation operators
        :param extensions:
        :return: a relation representing the aggregation/projection operator result
        """
        if _group is not None and not isinstance(_group, RelationalOperand):
            raise DataJointError('The second argument must be a relation or None')
        alias_parser = re.compile(
            '^\s*(?P<sql_expression>\S(.*\S)?)\s*->\s*(?P<alias>[a-z][a-z_0-9]*)\s*$')

        # expand extended attributes in the form 'sql_expression -> new_attribute'
        _attributes = []
        for attribute in attributes:
            alias_match = alias_parser.match(attribute)
            if alias_match:
                d = alias_match.group_dict()
                renamed_attributes.update({d['alias']: d['sql_expression']})
            else:
                _attributes += attribute
        return Projection(self, _group, *_attributes, **renamed_attributes)

    def __iand__(self, restriction):
        """
        in-place relational restriction or semijoin
        """
        if self._restrictions is None:
            self._restrictions = []
        self._restrictions.append(restriction)
        return self

    def __and__(self, restriction):
        """
        relational restriction or semijoin
        """
        if self._restrictions is None:
            self._restrictions = []
        ret = copy(self)  # todo: why not deepcopy it?
        ret._restrictions = list(ret._restrictions)  # copy restriction
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

    @property
    def count(self):
        cur = self.conn.query('SELECT count(*) FROM ' + self.sql[0] + self._where)
        return cur.fetchone()[0]

    def __call__(self, *args, **kwargs):
        return self.fetch(*args, **kwargs)

    def fetch(self, offset=0, limit=None, order_by=None, descending=False):
        """
        fetches the relation from the database table into an np.array and unpacks blob attributes.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param descending: the list of attributes to order the results
        :return: the contents of the relation in the form of a structured numpy.array
        """
        return np.atleast_1d(rfn.stack_arrays(list(self.__iter__(offset, limit, order_by, descending)), usemask=False))

    def cursor(self, offset=0, limit=None, order_by=None, descending=False):
        """
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param descending: the list of attributes to order the results
        :return: cursor to the query
        """
        if offset and limit is None:
            raise DataJointError('')
        sql, heading = self.sql
        sql = 'SELECT ' + heading.as_sql + ' FROM ' + sql
        if order_by is not None:
            sql += ' ORDER BY ' + ', '.join(self._orderBy)
            if descending:
                sql += ' DESC'
        if limit is not None:
            sql += ' LIMIT %d' % limit
            if offset:
                sql += ' OFFSET %d' % offset
        logger.debug(sql)
        return self.conn.query(sql)

    def __repr__(self):
        limit = 13 #TODO: move some of these display settings into the config
        width = 12
        template = '%%-%d.%ds' % (width, width)
        repr_string = ' '.join([template % column for column in self.heading]) + '\n'
        repr_string += ' '.join(['+' + '-' * (width - 2) + '+' for _ in self.heading]) + '\n'
        tuples = self.project(*self.heading.non_blobs).fetch(limit=limit)
        for tup in tuples:
            repr_string += ' '.join([template % column for column in tup]) + '\n'
        if self.count > limit:
            repr_string += '...\n'
        repr_string += '%d tuples\n' % self.count
        return repr_string
        
    def __iter__(self, offset=0, limit=None, order_by=None, descending=False):
        """
        Iterator that yields individual tuples of the current table (as record arrays).


        :param offset: parameter passed to the :func:`cursor`
        :param limit: parameter passed to the :func:`cursor`
        :param order_by: parameter passed to the :func:`cursor`
        :param descending: parameter passed to the :func:`cursor`
        """
        cur = self.cursor(offset, limit, order_by, descending)
        do_unpack = tuple(h in self.heading.blobs for h in self.heading.names)
        q = cur.fetchone()
        while q:
            yield np.array([tuple(unpack(field) if up else field for up, field in zip(do_unpack, q))],
                           dtype=self.heading.as_dtype)[0]
            q = cur.fetchone()

    @property
    def _where(self):
        """
        convert the restriction into an SQL WHERE
        """
        if not self.restrictions:
            return ''
        
        def make_condition(arg):
            if isinstance(arg, dict):
                conditions = ['`%s`=%s' % (k, repr(v)) for k, v in arg.items()]
            elif isinstance(arg, np.void):
                conditions = ['`%s`=%s' % (k, arg[k]) for k in arg.dtype.fields]
            else:
                raise DataJointError('invalid restriction type')
            return ' AND '.join(conditions)

        condition_string = []
        for r in self._restrictions:
            negate = isinstance(r, Not)
            if negate:
                r = r.restrictions
            if isinstance(r, dict) or isinstance(r, np.void):
                r = make_condition(r)
            elif isinstance(r, np.ndarray) or isinstance(r, list):
                r = '('+') OR ('.join([make_condition(q) for q in r])+')'
            elif isinstance(r, RelationalOperand):
                common_attributes = ','.join([q for q in self.heading.names if r.heading.names])  
                r = '(%s) in (SELECT %s FROM %s)' % (common_attributes, common_attributes, r.sql)
                
            assert isinstance(r, str), 'condition must be converted into a string'
            r = '('+r+')'
            if negate:
                r = 'NOT '+r
            condition_string.append(r)

        return ' WHERE ' + ' AND '.join(condition_string)


class Not:
    """
    inverse of a restriction
    """
    def __init__(self, restriction):
        self.__restriction = restriction

    @property
    def restriction(self):
        return self.__restriction


class Join(RelationalOperand):
    subquery_counter = 0

    def __init__(self, rel1, rel2):
        if not isinstance(rel2, RelationalOperand):
            raise DataJointError('a relation can only be joined with another relation')
        if rel1.conn is not rel2.conn:
            raise DataJointError('Cannot join relations with different database connections')
        self.conn = rel1.conn
        self._rel1 = Subquery(rel1)
        self._rel2 = Subquery(rel2)

    @property
    def conn(self):
        return self._rel1.conn

    @property
    def heading(self):
        return self._rel1.heading.join(self._rel2.heading)

    @property
    def counter(self):
        self.subquery_counter += 1
        return self.subquery_counter

    @property
    def sql(self):
        return '%s NATURAL JOIN %s as `_j%x`' % (self._rel1.sql, self._rel2.sql, self.counter)


class Projection(RelationalOperand):
    subquery_counter = 0

    def __init__(self, relation, group=None, *attributes, **renamed_attributes):
        """
        See RelationalOperand.project()
        """
        if group:
            if relation.conn is not group.conn:
                raise DataJointError('Cannot join relations with different database connections')
            self._group = Subquery(group)
            self._relation = Subquery(relation)
        else:
            self._group = None
            self._relation = relation
        self._projection_attributes = attributes
        self._renamed_attributes = renamed_attributes

    @property
    def conn(self):
        return self._relation.conn

    @property
    def sql(self):
        sql, heading = self._relation.sql
        heading = heading.project(self._projection_attributes, self._renamed_attributes)
        if self._group is not None:
            group_sql, group_heading = self._group.sql
            sql = ("(%s) NATURAL LEFT JOIN (%s) GROUP BY `%s`" %
                   (sql, group_sql, '`,`'.join(heading.primary_key)))
        return sql, heading


class Subquery(RelationalOperand):
    """
    A Subquery encapsulates its argument in a SELECT statement, enabling its use as a subquery.
    The attribute list and the WHERE clause are resolved.
    """
    _counter = 0

    def __init__(self, rel):
        self._rel = rel

    @property
    def conn(self):
        return self._rel.conn

    @property
    def counter(self):
        Subquery._counter += 1
        return Subquery._counter

    @property
    def sql(self):
        return ('(SELECT ' + self._rel.heading.as_sql +
                ' FROM ' + self._rel.sql + self._rel.where + ') as `_s%x`' % self.counter),\
            self._rel.heading.clear_aliases()