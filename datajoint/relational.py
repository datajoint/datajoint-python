"""
classes for relational algebra
"""

import numpy as np
import abc
from copy import copy
from datajoint import DataJointError
from .blob import unpack
import logging

logger = logging.getLogger(__name__)


class Relation(metaclass=abc.ABCMeta):
    """
    Relation implements relational algebra and fetch methods.
    Relation objects reference other relation objects linked by operators.
    The leaves of this tree of objects are base relations.
    When fetching data from the database, this tree of objects is compiled into an SQL expression.
    It is a mixin class that provides relational operators, iteration, and fetch capability.
    Relation operators are: restrict, pro, and join.
    """    
    _restrictions = []

    @abc.abstractproperty
    def sql(self):
        return NotImplemented
    
    @abc.abstractproperty
    def heading(self):
        return NotImplemented

    @property
    def restrictions(self):
        return self._restrictions
            
    def __mul__(self, other):
        """
        relational join
        """
        return Join(self, other)

    def __mod__(self, attribute_list):
        """
        relational projection operator.
        :param attribute_list: list of attribute specifications.
        The attribute specifications are strings in following forms:
        'name' - specific attribute
        'name->new_name'  - rename attribute.  The old attribute is kept only if specifically included.
        'sql_expression->new_name' - extend attribute, i.e. a new computed attribute.
        :return: a new relation with specified heading
        """
        self.project(attribute_list)

    def project(self, *selection, **aliases):
        """
        Relational projection operator.
        :param attributes: a list of attribute names to be included in the result.
        :param renames: a dict of attributes to be renamed
        :return: a new relation with selected fields
        Primary key attributes are always selected and cannot be excluded.
        Therefore obj.project() produces a relation with only the primary key attributes.
        If selection includes the string '*', all attributes are selected.
        Each attribute can only be used once in attributes or renames.  Therefore, the projected
        relation cannot have more attributes than the original relation.
        """
        group = selection.pop[0] if selection and isinstance(selection[0], Relation) else None
        return self.aggregate(group, *selection, **aliases)

    def aggregate(self, group, *selection, **aliases):
        """
        Relational aggregation operator
        :param grouped_relation:
        :param extensions:
        :return:
        """
        if group is not None and not isinstance(group, Relation):
            raise DataJointError('The second argument of aggregate must be a relation')
        # convert the string notation for aliases to
        # handling of the variable group is unclear here
        # and thus ommitted
        return Projection(self, *selection, **aliases)

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
        in-place inverted restriction aka antijoin
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
        sql = 'SELECT count(*) FROM ' + self.sql + self._where_clause
        cur = self.conn.query(sql)
        return cur.fetchone()[0]

    def fetch(self, *args, **kwargs):
        return self(*args, **kwargs)

    def __call__(self, offset=0, limit=None, order_by=None, descending=False):
        """
        fetches the relation from the database table into an np.array and unpacks blob attributes.
        :param offset: the number of tuples to skip in the returned result
        :param limit: the maximum number of tuples to return
        :param order_by: the list of attributes to order the results. No ordering should be assumed if order_by=None.
        :param descending: the list of attributes to order the results
        :return: the contents of the relation in the form of a structured numpy.array
        """
        cur = self.cursor(offset, limit, order_by, descending)
        ret = np.array(list(cur), dtype=self.heading.as_dtype)
        for f in self.heading.blobs:
            for i in range(len(ret)):
                ret[i][f] = unpack(ret[i][f])
        return ret

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
        sql = 'SELECT ' + self.heading.as_sql + ' FROM ' + self.sql
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
        
    def __iter__(self):
        """
        iterator  yields primary key tuples
        """
        cur, h = self.project().cursor()
        q = cur.fetchone()
        while q:
            yield np.array([q, ], dtype=h.asdtype)
            q = cur.fetchone()

    @property
    def _where_clause(self):
        """
        make there WHERE clause based on the current restriction
        """
        if not self._restrictions:
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
            elif isinstance(r, Relation):
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
        self._restriction = restriction


class Join(Relation):
    alias_counter = 0

    def __init__(self, rel1, rel2):
        if not isinstance(rel2, Relation):
            raise DataJointError('relvars can only be joined with other relvars')
        if rel1.conn is not rel2.conn:
            raise DataJointError('Cannot join relations with different database connections')
        self.conn = rel1.conn
        self._rel1 = rel1
        self._rel2 = rel2
    
    @property
    def heading(self):
        return self._rel1.heading.join(self._rel2.heading)
        
    @property 
    def sql(self):
        Join.alias_counter += 1
        return '%s NATURAL JOIN %s as `j%x`' % (self._rel1.sql, self._rel2.sql, Join.alias_counter)


class Projection(Relation):
    alias_counter = 0

    def __init__(self, relation, *attributes, **renames):
        """
        See Relation.project()
        """
        self.conn = relation.conn
        self._relation = relation
        self._projection_attributes = attributes
        self._renamed_attributes = renames

    @property 
    def sql(self):
        return self._relation.sql
        
    @property
    def heading(self):
        return self._relation.heading.pro(*self._projection_attributes, **self._renamed_attributes)


class Subquery(Relation):
    alias_counter = 0
    
    def __init__(self, rel):
        self.conn = rel.conn
        self._rel = rel
        
    @property
    def sql(self):
        self.alias_counter += 1
        return '(SELECT ' + self._rel.heading.as_sql + ' FROM ' + self._rel.sql + ') as `s%x`' % self.alias_counter
        
    @property
    def heading(self):
        return self._rel.heading.resolve_computations()