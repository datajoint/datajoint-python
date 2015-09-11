from collections.abc import Iterable, Mapping
import numpy as np
import abc
import re
from copy import copy
import logging
from . import DataJointError

from .fetch import Fetch, Fetch1

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

    @property
    @abc.abstractmethod
    def from_clause(self):
        """
        :return: a string containing the FROM clause of the SQL SELECT statement
        """

    @property
    @abc.abstractmethod
    def heading(self):
        """
        :return: a valid datajoint.Heading object
        """

    @property
    def select_fields(self):
        """
        :return: string specifying the attributes to return
        """
        return self.heading.as_sql

    @property
    def _grouped(self):
        return False

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
        return Projection(self, *attributes, **renamed_attributes)

    def aggregate(self, group, *attributes, **renamed_attributes):
        """
        Relational aggregation operator

        :param group:  relation whose tuples can be used in aggregation operators
        :param attributes:
        :param renamed_attributes:
        :return: a relation representing the aggregation/projection operator result
        """
        if not isinstance(group, RelationalOperand):
            raise DataJointError('The second argument must be a relation')
        return Aggregation(
            Join(self, Subquery(group), left=True),
            *attributes, **renamed_attributes)

    def __and__(self, restriction):
        """
        relational restriction or semijoin
        :return: a restricted copy of the argument
        """
        ret = copy(self)
        ret.restrict(restriction, *ret.restrictions)
        return ret

    def restrict(self, *restrictions):
        """
        In-place restriction. Primarily intended for datajoint's internal use.
        Users are encouraged to use self & restriction to apply a restriction.
        Each condition in restrictions is applied and the conditions are combined with AND.
        However, each member of restrictions can be a list of conditions, which are combined with OR.
        :param restrictions: list of restrictions.
        """
        # remove Nones and duplicates
        restrictions = [r for r in restrictions if r is not None and r not in self.restrictions]
        if restrictions:
            if any(is_empty_set(r) for r in restrictions):
                # if any condition is an empty list, return empty
                self._restrictions = ['FALSE']
            else:
                if self._restrictions is None:
                    self._restrictions = restrictions
                else:
                    self._restrictions.extend(restrictions)

    def attributes_in_restrictions(self):
        """
        :return: list of attributes that are probably used in the restrictions.
        This is used internally for optimizing SQL statements
        """
        where_clause = self.where_clause
        return set(name for name in self.heading.names if name in where_clause)

    def __sub__(self, restriction):
        """
        inverted restriction aka antijoin
        """
        return self & (None if is_empty_set(restriction) else Not(restriction))

    @abc.abstractmethod
    def _repr_helper(self):
        """
        :return: (string) basic representation of the relation
        """

    def __repr__(self):
        ret = self._repr_helper()
        if self._restrictions:
            ret += ' & %r' % self._restrictions
        return ret

    def make_select(self, select_fields=None):
        return 'SELECT {fields} FROM {from_}{where}{group}'.format(
            fields=select_fields if select_fields else self.select_fields,
            from_=self.from_clause,
            where=self.where_clause,
            group=' GROUP BY `%s`' % '`,`'.join(self.primary_key) if self._grouped else ''
            )

    def __len__(self):
        """
        number of tuples in the relation.  This also takes care of the truth value
        """
        if self._grouped:
            return len(Subquery(self))
        cur = self.connection.query(self.make_select('count(*)'))
        return cur.fetchone()[0]

    def __contains__(self, item):
        """
        "item in relation" is equivalent to "len(relation & item)>0"
        """
        return len(self & item) > 0

    def cursor(self, offset=0, limit=None, order_by=None, as_dict=False):
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

        if limit is not None:
            sql += ' LIMIT %d' % limit
            if offset:
                sql += ' OFFSET %d' % offset
        logger.debug(sql)
        return self.connection.query(sql, as_dict=as_dict)

    @property
    def fetch1(self):
        return Fetch1(self)

    @property
    def fetch(self):
        return Fetch(self)

    @property
    def where_clause(self):
        """
        convert the restriction into an SQL WHERE
        """
        if not self.restrictions:
            return ''

        def make_condition(arg):
            if isinstance(arg, Mapping):
                condition = ['`%s`=%s' % (k, repr(v)) for k, v in arg.items() if k in self.heading]
            elif isinstance(arg, np.void):
                condition = ['`%s`=%s' % (k, arg[k]) for k in arg.dtype.fields if k in self.heading]
            else:
                raise DataJointError('invalid restriction type')
            return ' AND '.join(condition) if condition else 'TRUE'

        conditions = []
        for r in self.restrictions:
            negate = isinstance(r, Not)
            if negate:
                r = r.restriction
            if isinstance(r, Mapping) or isinstance(r, np.void):
                r = make_condition(r)
            elif isinstance(r, np.ndarray) or isinstance(r, list):
                r = '(' + ') OR ('.join([make_condition(q) for q in r]) + ')'
            elif isinstance(r, RelationalOperand):
                common_attributes = [q for q in self.heading.names if q in r.heading.names]
                if not common_attributes:
                    r = 'FALSE' if negate else 'TRUE'
                else:
                    common_attributes = '`'+'`,`'.join(common_attributes)+'`'
                    r = '({fields}) {not_}in ({subquery})'.format(
                        fields=common_attributes,
                        not_="not " if negate else "",
                        subquery=r.make_select(common_attributes))
                negate = False
            if not isinstance(r, str):
                raise DataJointError('Invalid restriction object')
            if r:
                conditions.append('%s(%s)' % ('not ' if negate else '', r))
        return ' WHERE ' + ' AND '.join(conditions)


class Not:
    """
    inverse restriction
    """
    def __init__(self, restriction):
        self.restriction = restriction


class Join(RelationalOperand):
    """
    Relational join
    """

    def __init__(self, arg1, arg2, left=False):
        if not isinstance(arg2, RelationalOperand):
            raise DataJointError('a relation can only be joined with another relation')
        if arg1.connection != arg2.connection:
            raise DataJointError('Cannot join relations with different database connections')
        self._arg1 = Subquery(arg1) if arg1.heading.computed else arg1
        self._arg2 = Subquery(arg2) if arg2.heading.computed else arg2
        self._restrictions = self._arg1.restrictions + self._arg2.restrictions
        self._left = left
        self._heading = self._arg1.heading.join(self._arg2.heading, left=left)

    def _repr_helper(self):
        return "(%r) * (%r)" % (self._arg1, self._arg2)

    @property
    def connection(self):
        return self._arg1.connection

    @property
    def heading(self):
        return self._heading

    @property
    def from_clause(self):
        return '{from1} NATURAL {left}JOIN {from2}'.format(
            from1=self._arg1.from_clause,
            left="LEFT " if self._left else "",
            from2=self._arg2.from_clause)

    @property
    def select_fields(self):
        return '*'


class Projection(RelationalOperand):
    def __init__(self, arg, *attributes, **renamed_attributes):
        """
        See RelationalOperand.project()
        """
        # parse attributes in the form 'sql_expression -> new_attribute'
        alias_parser = re.compile(
            '^\s*(?P<sql_expression>\S(.*\S)?)\s*->\s*(?P<alias>[a-z][a-z_0-9]*)\s*$')
        self._attributes = []
        self._renamed_attributes = renamed_attributes
        for attribute in attributes:
            alias_match = alias_parser.match(attribute)
            if alias_match:
                d = alias_match.groupdict()
                self._renamed_attributes.update({d['alias']: d['sql_expression']})
            else:
                self._attributes.append(attribute)
        self._arg = arg

        restricting_on_removed_attributes = bool(
            arg.attributes_in_restrictions() - set(self.heading.names))
        renaming_renamed_attributes = renamed_attributes and arg.heading.computed
        use_subquery = restricting_on_removed_attributes or renaming_renamed_attributes
        if use_subquery:
            self._arg = Subquery(arg)
        else:
            self.restrict(*arg.restrictions)

    def _repr_helper(self):
        return "(%r).project(%r)" % (self._arg, self._attributes)

    @property
    def connection(self):
        return self._arg.connection

    @property
    def heading(self):
        return self._arg.heading.project(*self._attributes, **self._renamed_attributes)

    @property
    def from_clause(self):
        return self._arg.from_clause

    def __and__(self, restriction):
        """
        When restricting on renamed attributes, enclose in subquery
        """
        has_restriction = isinstance(restriction, RelationalOperand) or restriction
        do_subquery = has_restriction and self.heading.computed
        return Subquery(self) & restriction if do_subquery else super().__and__(restriction)


class Aggregation(Projection):
    @property
    def _grouped(self):
        return True


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
    def select_fields(self):
        return '*'

    @property
    def heading(self):
        return self._arg.heading.resolve()

    def _repr_helper(self):
        return "%r" % self._arg


def is_empty_set(arg):
    return isinstance(arg, (list, set, tuple, np.ndarray, RelationalOperand)) and len(arg) == 0
