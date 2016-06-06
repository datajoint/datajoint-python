import collections
import numpy as np
import abc
import re
from copy import copy
import logging
from . import DataJointError, config
import datetime
from .fetch import Fetch, Fetch1
from .heading import Heading

logger = logging.getLogger(__name__)


class AndList(list):
    """
    A list of restrictions to by applied to a relation.  The restrictions are AND-ed.
    Each restriction can be a list or set or a relation whose elements are OR-ed.
    But the elements that are lists can contain other AndLists.

    Example:
    rel2 = rel & dj.AndList((cond1, cond2, cond3))
    is equivalent to
    rel2 = rel & cond1 & cond2 & cond3
    """
    pass


class OrList(list):
    """
    A list of restrictions to by applied to a relation.  The restrictions are OR-ed.
    If any restriction is .
    But the elements that are lists can contain other AndLists.

    Example:
    rel2 = rel & dj.ORList((cond1, cond2, cond3))
    is equivalent to
    rel2 = rel & [cond1, cond2, cond3]

    Since ORList is just an alias for list, it is not necessary and is only provided
    for consistency with AndList.
    """
    pass


class RelationalOperand(metaclass=abc.ABCMeta):
    """
    RelationalOperand implements the relational algebra.
    RelationalOperand objects link other relational operands with relational operators.
    The leaves of this tree of objects are base relations.
    When fetching data from the database, this tree of objects is compiled into an SQL expression.
    RelationalOperand operators are restrict, join, proj, and aggregate.
    """

    _restrictions = None

    @property
    def restrictions(self):
        if self._restrictions is None:
            self._restrictions = AndList()
        return self._restrictions

    def clear_restrictions(self):
        self._restrictions = None

    @property
    def primary_key(self):
        return self.heading.primary_key

    @property
    def where_clause(self):
        """
        convert to a WHERE clause string
        """
        def make_condition(arg, _negate=False):
            if isinstance(arg, str):
                return arg, _negate
            elif isinstance(arg, AndList):
                if arg:
                    return '(' + ' AND '.join([make_condition(element)[0] for element in arg]) + ')', _negate
                else:
                    return 'FALSE' if _negate else 'TRUE', False

            #  semijoin or antijoin
            elif isinstance(arg, RelationalOperand):
                common_attributes = [q for q in self.heading.names if q in arg.heading.names]
                if not common_attributes:
                    condition = 'FALSE' if _negate else 'TRUE'
                else:
                    common_attributes = '`' + '`,`'.join(common_attributes) + '`'
                    condition = '({fields}) {not_}in ({subquery})'.format(
                        fields=common_attributes,
                        not_="not " if _negate else "",
                        subquery=arg.make_sql(common_attributes))
                return condition, False  # _negate is cleared

            # mappings are turned into ANDed equality conditions
            elif isinstance(arg, collections.abc.Mapping):
                condition = ['`%s`=%r' %
                             (k, v if not isinstance(v, (datetime.date, datetime.datetime, datetime.time)) else str(v))
                             for k, v in arg.items() if k in self.heading]
            elif isinstance(arg, np.void):
                # element of a record array
                condition = ['`%s`=%s' % (k, arg[k]) for k in arg.dtype.fields if k in self.heading]
            else:
                raise DataJointError('Invalid restriction type')
            return ' AND '.join(condition) if condition else 'TRUE', _negate

        if len(self.restrictions) == 0:  # an empty list -> no WHERE clause
            return ''

        # An empty or-list in the restrictions immediately causes an empty result
        assert isinstance(self.restrictions, AndList)
        if any(restricts_to_empty(r) for r in self.restrictions):
            return ' WHERE FALSE'

        conditions = []
        for item in self.restrictions:
            negate = isinstance(item, Not)
            if negate:
                item = item.restriction  # NOT is added below
            if isinstance(item, (list, tuple, set, np.ndarray)):
                # process an OR list
                temp = [make_condition(q)[0] for q in item if q is not restricts_to_empty(q)]
                item = '(' + ') OR ('.join(temp) + ')' if temp else 'FALSE'
            else:
                item, negate = make_condition(item, negate)
            if not item:
                raise DataJointError('Empty condition')
            conditions.append(('NOT (%s)' if negate else '(%s)') % item)
        return ' WHERE ' + ' AND '.join(conditions)

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
        return other*self if isinstance(other, U) else Join(self, other)

    def __mod__(self, attributes=None):
        """
        relational projection operator.  See RelationalOperand.project
        """
        return self.proj(*attributes)

    def project(self, *args, **kwargs):
        """
        alias for self.proj() for backward compatibility
        """
        return self.proj(*args, **kwargs)

    def proj(self, *attributes, **renamed_attributes):
        """
        Relational projection operator.
        :param attributes: a list of attribute names to be included in the result.
        :return: a new relation with selected fields
        Primary key attributes are always selected and cannot be excluded.
        Therefore obj.proj() produces a relation with only the primary key attributes.
        If attributes includes the string '*', all attributes are selected.
        Each attribute can only be used once in attributes or renamed_attributes.  Therefore, the projected
        relation cannot have more attributes than the original relation.
        """
        return Projection(self, *attributes, **renamed_attributes)

    def force_proj(self, *attributes, **renamed_attributes):
        """
        Relation projection operator that allows projecting out primary key.
        Use the same way as self.proj(...) but the primary key is not automatically included.
        """

    def aggregate(self, group, *attributes, keep_all_rows=False, **renamed_attributes):
        """
        Relational aggregation operator

        :param group:  relation whose tuples can be used in aggregation operators
        :param attributes:
        :param keep_all_rows: True = preserve the number of tuples in the result (equivalent of LEFT JOIN in SQL)
        :param renamed_attributes:
        :return: a relation representing the aggregation/projection operator result
        """
        if not isinstance(group, RelationalOperand):
            raise DataJointError('The second argument must be a relation')
        return Aggregation(
            Join(self, group, aggregated=True, keep_all_rows=keep_all_rows),
            *attributes, **renamed_attributes)

    def __iand__(self, restriction):
        """
        in-place restriction

        See relational_operand.restrict for more detail.
        """
        self.restrict(restriction)
        return self

    def __and__(self, restriction):
        """
        relational restriction or semijoin
        :return: a restricted copy of the argument

        See relational_operand.restrict for more detail.
        """
        ret = copy(self)
        ret.clear_restrictions()
        ret.restrict(self.restrictions)
        ret.restrict(restriction)
        return ret

    def __isub__(self, restriction):
        """
        in-place inverted restriction

        See relational_operand.restrict for more detail.
        """
        self.restrict(Not(restriction))
        return self

    def __sub__(self, restriction):
        """
        inverse restriction aka antijoin
        :return: a restricted copy of the argument

        See relational_operand.restrict for more detail.
        """
        return self & Not(restriction)

    def restrict(self, restriction):
        """
        In-place restriction.  Restricts the relation to a subset of its original tuples.
        rel.restrict(restriction)  is equivalent to  rel = rel & restriction  or  rel &= restriction
        rel.restrict(Not(restriction))  is equivalent to  rel = rel - restriction  or  rel -= restriction
        The primary key of the result is unaffected.
        Successive restrictions are combined using the logical AND.
        The AndList class is provided to play the role of successive restrictions.
        Any relation, collection, or sequence other than an AndList are treated as OrLists.
        However, the class OrList is still provided for cases when explicitness is required.
        Inverse restriction is accomplished by either using the subtraction operator or the Not class.

        The expressions in each row equivalent:
        rel & True                          rel
        rel & False                         the empty relation
        rel & 'TRUE'                        rel
        rel & 'FALSE'                       the empty relation
        rel - cond                          rel & Not(cond)
        rel - 'TRUE'                        rel & False
        rel - 'FALSE'                       rel
        rel & AndList((cond1,cond2))        rel & cond1 & cond2
        rel & AndList()                     rel
        rel & [cond1, cond2]                rel & OrList((cond1, cond2))
        rel & []                            rel & False
        rel & None                          rel & False
        rel & any_empty_relation            rel & False
        rel - AndList((cond1,cond2))        rel & [Not(cond1), Not(cond2)]
        rel - [cond1, cond2]                rel & Not(cond1) & Not(cond2)
        rel - AndList()                     rel & False
        rel - []                            rel
        rel - None                          rel
        rel - any_empty_relation            rel

        When arg is another relation, the restrictions  rel & arg  and  rel - arg  become the relational semijoin and
        antijoin operators, respectively.
        Then,  rel & arg  restricts rel to tuples that match at least one tuple in arg (hence arg is treated as an OrList).
        Conversely,  rel - arg  restricts rel to tuples that do not match any tuples in arg.
        Two tuples match when their common attributes have equal values or when they have no common attributes.
        All shared attributes must be in the primary key of either rel or arg or both or an error will be raised.

        relational_operand.restrict is the only access point that modifies restrictions. All other operators must
        ultimately call restrict()

        :param restriction: a sequence or an array (treated as OR list), another relation, an SQL condition string, or
        an AndList.
        """
        if isinstance(restriction, U):
            raise DataJointError('Restriction by dj.U is invalid')
        if restriction is True or isinstance(restriction, str) and restriction.upper() == "TRUE":
            return
        if isinstance(restriction, AndList):
            self.restrictions.extend(restriction)
        elif restricts_to_empty(restriction):
            self.clear_restrictions()
            self.restrictions.append('FALSE')
        else:
            self.restrictions.append(restriction)

    @property
    def fetch1(self):
        return Fetch1(self)

    @property
    def fetch(self):
        return Fetch(self)

    def attributes_in_restrictions(self):
        """
        :return: list of attributes that are probably used in the restrictions.
        This is used internally for optimizing SQL statements
        """
        s = self.where_clause
        return set(name for name in self.heading.names if name in s)

    @abc.abstractmethod
    def _repr_helper(self):
        """
        :return: (string) basic representation of the relation
        """

    def __repr__(self):
        if config['loglevel'].lower() == 'debug':
            ret = self._repr_helper()
            if self._restrictions:
                ret += ' & %r' % self._restrictions
        else:
            rel = self.proj(*self.heading.non_blobs)  # project out blobs
            limit = config['display.limit']
            width = config['display.width']

            tups = rel.fetch(limit=limit)
            columns = rel.heading.names

            widths = {f: min(max([len(f)] + [len(str(e)) for e in tups[f]])+4,width) for f in columns}

            templates = {f: '%%-%d.%ds' % (widths[f], widths[f]) for f in columns}
            repr_string = ' '.join([templates[column] % column for column in columns]) + '\n'
            repr_string += ' '.join(['+' + '-' * (widths[column] - 2) + '+' for column in columns]) + '\n'
            for tup in tups:
                repr_string += ' '.join([templates[column] % tup[column] for column in columns]) + '\n'
            if len(rel) > limit:
                repr_string += '...\n'
            repr_string += ' (%d tuples)\n' % len(rel)
            return repr_string

        return ret

    def _repr_html_(self):
        limit = config['display.limit']
        rel = self.proj(*self.heading.non_blobs)  # project out blobs
        columns = rel.heading.names
        info = self.heading.table_info
        content = dict(
            title="" if info is None else "<h3>%s</h3>" % info['comment'],
            head='</th><th>'.join(columns),
            body='</tr><tr>'.join(
                ['\n'.join(['<td>%s</td>' % column for column in tup]) for tup in rel.fetch(limit=limit)]),
            tuples=len(rel)
        )
        return """ %(title)s
            <div style="max-height:1000px;max-width:1500px;overflow:auto;">
            <table border="1" class="dataframe">
                <thead> <tr style="text-align: right;"> <th> %(head)s </th> </tr> </thead>
                <tbody> <tr> %(body)s </tr> </tbody>
            </table>
            <p>%(tuples)i tuples</p></div>
            """ % content

    def make_sql(self, select_fields=None):
        return 'SELECT {fields} FROM {from_}{where}{group}'.format(
            fields=select_fields if select_fields else self.select_fields,
            from_=self.from_clause,
            where=self.where_clause,
            group=' GROUP BY `%s`' % '`,`'.join(self.primary_key) if self._grouped else '')

    def __len__(self):
        """
        number of tuples in the relation.  This also takes care of the truth value
        """
        if self._grouped:
            return len(Subquery(self))

        cur = self.connection.query(self.make_sql('count(*)'))
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
        sql = self.make_sql()
        if order_by is not None:
            sql += ' ORDER BY ' + ', '.join(order_by)

        if limit is not None:
            sql += ' LIMIT %d' % limit
            if offset:
                sql += ' OFFSET %d' % offset
        logger.debug(sql)
        return self.connection.query(sql, as_dict=as_dict)


class Not:
    """
    invert restriction
    """

    def __init__(self, restriction):
        self.restriction = True if isinstance(restriction, U) else restriction


class Join(RelationalOperand):
    """
    Relational join
    """

    def __init__(self, arg1, arg2, aggregated=False, keep_all_rows=False):
        assert not keep_all_rows or aggregated     # keep_all_rows can be set only in aggregation
        if not isinstance(arg2, RelationalOperand):
            raise DataJointError('a relation can only be joined with another relation')
        if arg1.connection != arg2.connection:
            raise DataJointError('Cannot join relations with different database connections')
        self._arg1 = Subquery(arg1) if isinstance(arg1, Projection) else arg1
        self._arg2 = Subquery(arg2) if isinstance(arg2, Projection) else arg2
        self._heading = self._arg1.heading.join(self._arg2.heading, aggregated=aggregated)
        self.restrict(self._arg1.restrictions)
        self.restrict(self._arg2.restrictions)
        self._left = keep_all_rows

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
        return self.heading.as_sql


class Projection(RelationalOperand):
    def __init__(self, arg, *attributes, **renamed_attributes):
        """
        See RelationalOperand.proj()
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
        use_subquery = restricting_on_removed_attributes or arg.heading.computed
        if use_subquery:
            self._arg = Subquery(arg)
        else:
            self.restrict(arg.restrictions)

    def _repr_helper(self):
        return "(%r).proj(%r)" % (self._arg, self._attributes)

    @property
    def connection(self):
        return self._arg.connection

    @property
    def heading(self):
        return self._arg.heading.proj(*self._attributes, **self._renamed_attributes)

    @property
    def _grouped(self):
        return self._arg._grouped

    @property
    def from_clause(self):
        return self._arg.from_clause

    def __and__(self, restriction):
        has_restriction = isinstance(restriction, RelationalOperand) or bool(restriction)
        do_subquery = has_restriction and self.heading.computed
        ret = Subquery(self) if do_subquery else self
        ret.restrict(restriction)
        return ret


class Aggregation(Projection):
    @property
    def _grouped(self):
        return True


class Subquery(RelationalOperand):
    """
    A Subquery encapsulates its argument in a SELECT statement, enabling its use as a subquery.
    The attribute list and the WHERE clause are resolved.  Thus, a subquery no longer has any renamed attributes.
    """
    __counter = 0

    def __init__(self, arg, extend_primary_key=()):
        self._arg = arg
        self._extend_primary_key = extend_primary_key

    @property
    def connection(self):
        return self._arg.connection

    @property
    def counter(self):
        Subquery.__counter += 1
        return Subquery.__counter

    @property
    def from_clause(self):
        return '(' + self._arg.make_sql() + ') as `_s%x`' % self.counter

    @property
    def select_fields(self):
        return '*'

    @property
    def heading(self):
        return self._arg.heading.resolve(extend_primary_key=self._extend_primary_key)

    def _repr_helper(self):
        return "%r" % self._arg


class U:
    """
    dj.U objects are special relations representing all possible values their attributes.
    dj.U objects cannot be queried on their own but are useful for forming some relational queries.
    dj.U('attr1', ..., 'attrn') represents a relation with the primary key attributes attr1 ... attrn.
    The body of the relation is filled with all possible combinations of values of the attributes.
    Without any attributes, dj.U() represents the relation with one tuple and no attributes.
    The Third Manifesto refers to dj.U() as TABLE_DEE.

    Relational restriction:
    dj.U can be used to enumerate unique combinations of values of attributes from other relations.

    The following expression produces a relation containing all unique combinations of contrast and brightness
    found in relation stimulus:
    dj.U('contrast', 'brightness') & stimulus

    The following expression produces a relation containing all unique combinations of contrast and brightness that is
    contained in relation1 but not contained in relation 2.
    (dj.U('contrast', 'brightness') & relation1) - relation2

    Relational aggregation:
    In aggregation, dj.U is used to compute aggregate expressions on the entire relation.

    The following expression produces a relation with one tuple and one attribute s containing the total number
    of tuples in relation:
    dj.U().aggregate(relation, n='count(*)')

    The following expression produces a relation with one tuple containing the number n of distinct values of attr
    in relation.
    dj.U().aggregate(relation, n='count(distinct attr)')

    The following expression produces a relation with one tuple and one attribute s containing the total sum of attr
      from relation:
    dj.U().aggregate(relation, s='sum(attr)')   # sum of attr from the entire relation

    The following expression produces a relation with the count n of tuples in relation containing each unique
    combination of values in attr1 and attr2.
    dj.U(attr1,attr2).aggregate(relation, n='count(*)')

    Joins:
    If relation rel has attributes 'attr1' and 'attr2', then rel*dj.U('attr1','attr2') or produces a relation that is
    identical to rel except attr1 and attr2 are included in the primary key.  This is useful for producing a join on
    non-primary key attributes.
    For example, if attr is in both rel1 and rel2 but not in their primary  keys, then rel1*rel2 will throw an error
    because in most cases, it does not make sense to join on non-primary key attributes and users must first rename
    attr in one of the operands.  The expression dj.U('attr')*rel1*rel2 overrides this constraint.
    Join is commutative.
    """

    def __init__(self, *attributes):
        self._join = None
        self._attributes = attributes

    def __and__(self, arg):
        if not isinstance(arg, RelationalOperand):
            raise DataJointError('Relation U can only be restricted with another relation')
        return arg.force_proj(*self._attributes)

    def __mul__(self, arg):
        if not isinstance(arg, RelationalOperand):
            raise DataJointError('Relation U can only be joined with another relation')
        for attr in self._attributes:
            if attr not in arg.heading:
                raise DataJointError('Attribute %s is not found.' % attr)
        return Subquery(arg, extend_primary_key=self._attributes)

    def aggregate(self, arg, **computed_attributes):
        if not isinstance(arg, RelationalOperand):
            raise DataJointError('Relation U can only aggregate over another relation')
        arg = copy(arg)
        return arg.force_proj(*self._attributes, aggregated=True, **computed_attributes)


def restricts_to_empty(arg):
    """
    returns true if restriction to arg will produce the empty relation.
    """
    return not isinstance(arg, AndList) and (
        arg is None or arg is False or isinstance(arg, str) and arg.upper() == "FALSE" or
        isinstance(arg, (list, set, tuple, np.ndarray, RelationalOperand)) and len(arg) == 0)
