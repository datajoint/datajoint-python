import collections
from itertools import count
import logging
import inspect
import numpy as np
import re
import datetime
import decimal
from . import config
from .errors import DataJointError
from .fetch import Fetch, Fetch1

logger = logging.getLogger(__name__)


def assert_join_compatibility(rel1, rel2):
    """
    Determine if relations rel1 and rel2 are join-compatible.  To be join-compatible, the matching attributes
    in the two relations must be in the primary key of one or the other relation.
    Raises an exception if not compatible.
    :param rel1: A RelationalOperand object
    :param rel2: A RelationalOperand object
    """
    for rel in (rel1, rel2):
        if not isinstance(rel, (U, RelationalOperand)):
            raise DataJointError('Object %r is not a relation and cannot be joined.' % rel)
    if not isinstance(rel1, U) and not isinstance(rel2, U):  # dj.U is always compatible
        try:
            raise DataJointError("Cannot join relations on dependent attribute `%s`" % next(r for r in set(
                rel1.heading.dependent_attributes).intersection(rel2.heading.dependent_attributes)))
        except StopIteration:
            pass


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

    def append(self, restriction):
        if isinstance(restriction, AndList):
            # extend to reduce nesting
            self.extend(restriction)
        else:
            super().append(restriction)


class RelationalOperand:
    """
    RelationalOperand implements the relational algebra.
    RelationalOperand objects link other relational operands with relational operators.
    The leaves of this tree of objects are base relations.
    When fetching data from the database, this tree of objects is compiled into an SQL expression.
    RelationalOperand operators are restrict, join, proj, and aggr.
    """

    def __init__(self, arg=None):
        if arg is None:  # initialize
            # initialize
            self._restriction = AndList()
            self._distinct = False
            self._heading = None
        else:  # copy
            assert isinstance(arg, RelationalOperand), 'Cannot make RelationalOperand from %s' % arg.__class__.__name__
            self._restriction = AndList(arg._restriction)
            self._distinct = arg.distinct
            self._heading = arg._heading

    @classmethod
    def create(cls):  # pragma: no cover
        """abstract method for creating an instance"""
        assert False, "Abstract method `create` must be overridden in subclass."

    @property
    def connection(self):
        """
        :return:  the dj.Connection object
        """
        return self._connection

    @property
    def heading(self):
        """
        :return: the dj.Heading object of the relation
        """
        return self._heading

    @property
    def distinct(self):
        """
        :return: True if the DISTINCT modifier is required to make valid result
        """
        return self._distinct

    @property
    def restriction(self):
        """
        :return:  The AndList of restrictions applied to the relation.
        """
        assert isinstance(self._restriction, AndList)
        return self._restriction

    @property
    def primary_key(self):
        return self.heading.primary_key

    def _make_condition(self, arg):
        """
        Translate the input arg into the equivalent SQL condition (a string)
        :param arg: any valid restriction object.
        :return: an SQL condition string.  It may also be a boolean that is intended to be treated as a string.
        """
        negate = False
        while isinstance(arg, Not):
            negate = not negate
            arg = arg.restriction
        template = "NOT (%s)" if negate else "%s"

        # restrict by string
        if isinstance(arg, str):
            return template % arg.strip()

        # restrict by AndList
        if isinstance(arg, AndList):
            # omit all conditions that evaluate to True
            items = [item for item in (self._make_condition(i) for i in arg) if item is not True]
            if any(item is False for item in items):
                return negate  # if any item is False, the whole thing is False
            if not items:
                return not negate   # and empty AndList is True
            return template % ('(' + ') AND ('.join(items) + ')')

        # restriction by dj.U evaluates to True
        if isinstance(arg, U):
            return not negate

        # restrict by boolean
        if isinstance(arg, bool):
            return negate != arg

        # restrict by a mapping such as a dict -- convert to an AndList of string equality conditions
        if isinstance(arg, collections.abc.Mapping):
            return template % self._make_condition(
                AndList('`%s`=%r' % (k, (v if not isinstance(v, (
                    datetime.date, datetime.datetime, datetime.time, decimal.Decimal)) else str(v)))
                        for k, v in arg.items() if k in self.heading))

        # restrict by a numpy record -- convert to an AndList of string equality conditions
        if isinstance(arg, np.void):
            return template % self._make_condition(
                AndList(('`%s`='+('%s' if self.heading[k].numeric else '"%s"')) % (k, arg[k])
                        for k in arg.dtype.fields if k in self.heading))

        # restrict by a Relation class -- triggers instantiation
        if inspect.isclass(arg) and issubclass(arg, RelationalOperand):
            arg = arg()

        # restrict by another relation (aka semijoin and antijoin)
        if isinstance(arg, RelationalOperand):
            assert_join_compatibility(self, arg)
            common_attributes = [q for q in self.heading.names if q in arg.heading.names]
            return (
                # without common attributes, any non-empty relation matches everything
                (not negate if arg else negate) if not common_attributes
                else '({fields}) {not_}in ({subquery})'.format(
                    fields='`' + '`,`'.join(common_attributes) + '`',
                    not_="not " if negate else "",
                    subquery=arg.make_sql(common_attributes)))

        # if iterable (but not a string, a relation, or an AndList), treat as an OrList
        try:
            or_list = [self._make_condition(q) for q in arg if q is not False]
        except TypeError:
            raise DataJointError('Invalid restriction type %r' % arg)
        else:
            if any(item is True for item in or_list):  # if any item is True, the whole thing is True
                return not negate
            return template % ('(%s)' % ' OR '.join(or_list)) if or_list else negate  # an empty or list is False

    @property
    def where_clause(self):
        """
        convert self.restriction to the SQL WHERE clause
        """
        cond = self._make_condition(self.restriction)
        return '' if cond is True else ' WHERE %s' % cond

    def get_select_fields(self, select_fields=None):
        """
        :return: string specifying the attributes to return
        """
        return self.heading.as_sql if select_fields is None else self.heading.project(select_fields).as_sql

    # --------- relational operators -----------

    def __mul__(self, other):
        """
        natural join of relations self and other
        """
        return other * self if isinstance(other, U) else Join.create(self, other)

    def __add__(self, other):
        """
        union of relations
        """
        return Union.create(self, other)

    def proj(self, *attributes, **named_attributes):
        """
        Relational projection operator.
        :param attributes:  attributes to be included in the result. (The primary key is already included).
        :param named_attributes: new attributes computed or renamed from existing attributes.
        :return: the projected relation.
        Primary key attributes are always cannot be excluded but may be renamed.
        Thus self.proj() produces the relation with only the primary key of self.
        self.proj(a='id') renames the attribute 'id' into 'a' and includes 'a' in the projection.
        self.proj(a='expr') adds a new field a with the value computed with SQL expression.
        self.proj(a='(id)') adds a new computed field named 'a' that has the same value as id
        Each attribute can only be used once in attributes or named_attributes.
        """
        return Projection.create(self, attributes, named_attributes)

    def aggr(self, group, *attributes, keep_all_rows=False, **named_attributes):
        """
        Relational aggregation/projection operator
        :param group:  relation whose tuples can be used in aggregation operators
        :param attributes: attributes of self to include in the resulting relation
        :param keep_all_rows: True = preserve the number of tuples in the result (equivalent of LEFT JOIN in SQL)
        :param named_attributes: renamings and computations on attributes of self and group
        :return: a relation representing the result of the aggregation/projection operator
        """
        return GroupBy.create(self, group, keep_all_rows=keep_all_rows,
                              attributes=attributes, named_attributes=named_attributes)

    aggregate = aggr  # aliased name for aggr

    def __iand__(self, restriction):
        """
        in-place restriction.
        A subquery is created if the argument has renamed attributes.  Then the restriction is not in place.

        See relational_operand.restrict for more detail.
        """
        return (Subquery.create(self) if self.heading.expressions else self).restrict(restriction)

    def __and__(self, restriction):
        """
        relational restriction or semijoin
        :return: a restricted copy of the argument
        See relational_operand.restrict for more detail.
        """
        return (Subquery.create(self)  # the HAVING clause in GroupBy can handle renamed attributes but WHERE cannot
                if self.heading.expressions and not isinstance(self, GroupBy)
                else self.__class__(self)).restrict(restriction)

    def __isub__(self, restriction):
        """
        in-place inverted restriction aka antijoin

        See relational_operand.restrict for more detail.
        """
        return self.restrict(Not(restriction))

    def __sub__(self, restriction):
        """
        inverted restriction aka antijoin
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
        assert not self.heading.expressions or isinstance(self, GroupBy), "Cannot restrict a projection" \
                                                                          " with renamed attributes in place."
        self.restriction.append(restriction)
        return self

    @property
    def fetch1(self):
        return Fetch1(self)

    @property
    def fetch(self):
        return Fetch(self)

    def attributes_in_restriction(self):
        """
        :return: list of attributes that are probably used in the restriction.
            The function errs on the side of false positives.
            For example, if the restriction is "val='id'", then the attribute 'id' would be flagged.
            This is used internally for optimizing SQL statements.
        """
        return set(name for name in self.heading.names
                   if re.search(r'\b' + name + r'\b', self.where_clause))

    def __repr__(self):
        return super().__repr__() if config['loglevel'].lower() == 'debug' else self.preview()

    def preview(self, limit=None, width=None):
        """
        returns a preview of the contents of the relation.
        """
        heading = self.heading
        rel = self.proj(*heading.non_blobs)
        if limit is None:
            limit = config['display.limit']
        if width is None:
            width = config['display.width']
        tuples = rel.fetch(limit=limit+1)
        has_more = len(tuples) > limit
        tuples = tuples[:limit]
        columns = heading.names
        widths = {f: min(max([len(f)] +
            [len(str(e)) for e in tuples[f]] if f in tuples.dtype.names else [len('=BLOB=')]) + 4, width) for f in columns}
        templates = {f: '%%-%d.%ds' % (widths[f], widths[f]) for f in columns}
        return (
            ' '.join([templates[f] % ('*' + f if f in rel.primary_key else f) for f in columns]) + '\n' +
            ' '.join(['+' + '-' * (widths[column] - 2) + '+' for column in columns]) + '\n' +
            '\n'.join(' '.join(templates[f] % (tup[f] if f in tup.dtype.names else '=BLOB=')
                for f in columns) for tup in tuples) +
            ('\n   ...\n' if has_more else '\n') +
            (' (%d tuples)\n' % len(rel) if config['display.show_tuple_count'] else ''))

    def _repr_html_(self):
        heading = self.heading
        rel = self.proj(*heading.non_blobs)
        info = heading.table_info
        tuples = rel.fetch(limit=config['display.limit']+1)
        has_more = len(tuples) > config['display.limit']
        tuples = tuples[0:config['display.limit']]

        css = """
        <style type="text/css">
            .Relation{
                border-collapse:collapse;
            }
            .Relation th{
                background: #A0A0A0; color: #ffffff; padding:4px; border:#f0e0e0 1px solid;
                font-weight: normal; font-family: monospace; font-size: 100%;
            }
            .Relation td{
                padding:4px; border:#f0e0e0 1px solid; font-size:100%;
            }
            .Relation tr:nth-child(odd){
                background: #ffffff;
            }
            .Relation tr:nth-child(even){
                background: #f3f1ff;
            }
            /* Tooltip container */
            .djtooltip {
            }
            /* Tooltip text */
            .djtooltip .djtooltiptext {
                visibility: hidden;
                width: 120px;
                background-color: black;
                color: #fff;
                text-align: center;
                padding: 5px 0;
                border-radius: 6px;
                /* Position the tooltip text - see examples below! */
                position: absolute;
                z-index: 1;
            }
            #primary {
                font-weight: bold;
                color: black;
            }

            #nonprimary {
                font-weight: normal;
                color: white;
            }

            /* Show the tooltip text when you mouse over the tooltip container */
            .djtooltip:hover .djtooltiptext {
                visibility: visible;
            }
        </style>
        """
        head_template = """<div class="djtooltip">
                                <p id="{primary}">{column}</p>
                                <span class="djtooltiptext">{comment}</span>
                            </div>"""
        return """
        {css}
        {title}
            <div style="max-height:1000px;max-width:1500px;overflow:auto;">
            <table border="1" class="Relation">
                <thead> <tr style="text-align: right;"> <th> {head} </th> </tr> </thead>
                <tbody> <tr> {body} </tr> </tbody>
            </table>
            {ellipsis}
            {count}</div>
            """.format(
            css=css,
            title="" if info is None else "<b>%s</b>" % info['comment'],
            head='</th><th>'.join(
                head_template.format(column=c, comment=heading.attributes[c].comment,
                                     primary='primary' if c in self.primary_key else 'nonprimary') for c in
                heading.names),
            ellipsis='<p>...</p>' if has_more else '',
            body='</tr><tr>'.join(
                ['\n'.join(['<td>%s</td>' % (tup[name] if name in tup.dtype.names else '=BLOB=')
                    for name in heading.names])
                 for tup in tuples]),
            count=('<p>%d tuples</p>' % len(rel)) if config['display.show_tuple_count'] else '')

    def make_sql(self, select_fields=None):
        return 'SELECT {fields} FROM {from_}{where}'.format(
            fields=("DISTINCT " if self.distinct else "") + self.get_select_fields(select_fields),
            from_=self.from_clause,
            where=self.where_clause)

    def __len__(self):
        """
        number of tuples in the relation.
        """
        return self.connection.query(
            'SELECT ' + (
                'count(DISTINCT `{pk}`)'.format(pk='`,`'.join(self.primary_key)) if self.distinct else 'count(*)') +
            ' FROM {from_}{where}'.format(
                from_=self.from_clause,
                where=self.where_clause)).fetchone()[0]

    def __bool__(self):
        """
        :return:  True if the relation is not empty. Equivalent to len(rel)>0 but may be more efficient.
        """
        return len(self) > 0

    def __contains__(self, item):
        """
        returns True if item is found in the relation.
        :param item: any restriction
        (item in relation) is equivalent to bool(self & item) but may be executed more efficiently.
        """
        return bool(self & item)  # May be optimized e.g. using an EXISTS query

    def __iter__(self):
        self._iter_only_key = all(v.in_key for v in self.heading.attributes.values())
        self._iter_keys = self.fetch('KEY')
        return self

    def __next__(self):
        try:
            key = self._iter_keys.pop(0)
        except AttributeError:
            # self._iter_keys is missing because __iter__ has not been called.
            raise TypeError("'RelationalOperand' object is not an iterator. Use iter(obj) to create an iterator.")
        except IndexError:
            raise StopIteration
        else:
            if self._iter_only_key:
                return key
            else:
                try:
                    return (self & key).fetch1()
                except DataJointError:
                    # The data may have been deleted since the moment the keys were fetched -- move on to next entry.
                    return next(self)

    def cursor(self, offset=0, limit=None, order_by=None, as_dict=False):
        """
        See Relation.fetch() for input description.
        :return: query cursor
        """
        if offset and limit is None:
            raise DataJointError('limit is required when offset is set')
        sql = self.make_sql()
        if order_by is not None:
            sql += ' ORDER BY ' + ', '.join(order_by)
        if limit is not None:
            sql += ' LIMIT %d' % limit + (' OFFSET %d' % offset if offset else "")
        logger.debug(sql)
        return self.connection.query(sql, as_dict=as_dict)


class Not:
    """
    invert restriction
    """
    def __init__(self, restriction):
        self.restriction = restriction


class Join(RelationalOperand):
    """
    Relational join.
    Join is a private DataJoint class not exposed to users.
    """

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is not None:
            assert isinstance(arg, Join), "Join copy constructor requires a Join object"
            self._connection = arg.connection
            self._heading = arg.heading
            self._arg1 = arg._arg1
            self._arg2 = arg._arg2
            self._left = arg._left

    @classmethod
    def create(cls, arg1, arg2, keep_all_rows=False):
        obj = cls()
        if inspect.isclass(arg2) and issubclass(arg2, RelationalOperand):
            arg2 = arg2()   # instantiate if joining with a class
        assert_join_compatibility(arg1, arg2)
        if arg1.connection != arg2.connection:
            raise DataJointError("Cannot join relations from different connections.")
        obj._connection = arg1.connection
        obj._arg1 = cls.make_argument_subquery(arg1)
        obj._arg2 = cls.make_argument_subquery(arg2)
        obj._distinct = obj._arg1.distinct or obj._arg2.distinct
        obj._left = keep_all_rows
        obj._heading = obj._arg1.heading.join(obj._arg2.heading)
        obj.restrict(obj._arg1.restriction)
        obj.restrict(obj._arg2.restriction)
        return obj

    @staticmethod
    def make_argument_subquery(arg):
        """
        Decide when a Join argument needs to be wrapped in a subquery
        """
        return Subquery.create(arg) if isinstance(arg, (GroupBy, Projection)) or arg.restriction else arg

    @property
    def from_clause(self):
        return '{from1} NATURAL{left} JOIN {from2}'.format(
            from1=self._arg1.from_clause,
            left=" LEFT" if self._left else "",
            from2=self._arg2.from_clause)


class Union(RelationalOperand):
    """
    Union is a private DataJoint class that implements relational union.
    """

    __count = count()

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is not None:
            assert isinstance(arg, Union), "Union copy constructore requires a Union object"
            self._connection = arg.connection
            self._heading = arg.heading
            self._arg1 = arg._arg1
            self._arg2 = arg._arg2

    @classmethod
    def create(cls, arg1, arg2):
        obj = cls()
        if inspect.isclass(arg2) and issubclass(arg2, RelationalOperand):
            arg2 = arg2()  # instantiate if a class
        if not isinstance(arg1, RelationalOperand) or not isinstance(arg2, RelationalOperand):
            raise DataJointError('a relation can only be unioned with another relation')
        if arg1.connection != arg2.connection:
            raise DataJointError("Cannot operate on relations from different connections.")
        if set(arg1.heading.names) != set(arg2.heading.names):
            raise DataJointError('Union requires the same attributes in both arguments')
        if any(not v.in_key for v in arg1.heading.attributes.values()) or \
                all(not v.in_key for v in arg2.heading.attributes.values()):
            raise DataJointError('Union arguments must not have any secondary attributes.')
        obj._connection = arg1.connection
        obj._heading = arg1.heading
        obj._arg1 = arg1
        obj._arg2 = arg2
        return obj

    def make_sql(self, select_fields=None):
        return "SELECT {_fields} FROM {_from}{_where}".format(
            _fields=self.get_select_fields(select_fields),
            _from=self.from_clause,
            _where=self.where_clause)

    @property
    def from_clause(self):
        return ("(SELECT {fields} FROM {from1}{where1} UNION SELECT {fields} FROM {from2}{where2}) as `_u%x`".format(
            fields=self.get_select_fields(None), from1=self._arg1.from_clause,
            where1=self._arg1.where_clause,
            from2=self._arg2.from_clause,
            where2=self._arg2.where_clause)) % next(self.__count)


class Projection(RelationalOperand):
    """
    Projection is a private DataJoint class that implements relational projection.
    See RelationalOperand.proj() for user interface.
    """

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is not None:
            assert isinstance(arg, Projection), "Projection copy constructor requires a Projection object."
            self._connection = arg.connection
            self._heading = arg.heading
            self._arg = arg._arg

    @classmethod
    def create(cls, arg, attributes=None, named_attributes=None, include_primary_key=True):
        """
        :param arg:  A relation to be be projected
        :param attributes:  attributes to be selected from
        :param named_attributes:  new attributes to select or
        :param include_primary_key:  True if the primary key must be included even if it's not in attributes.
        :return: the resulting Projection object
        """
        obj = cls()
        obj._connection = arg.connection
        named_attributes = {k: v.strip() for k, v in named_attributes.items()}  # clean up values
        obj._distinct = arg.distinct
        if include_primary_key:  # include primary key of relation
            attributes = (list(a for a in arg.primary_key if a not in named_attributes.values()) +
                          list(a for a in attributes if a not in arg.primary_key))
        else:
            # make distinct if the primary key is not completely selected
            obj._distinct = obj._distinct or not set(arg.primary_key).issubset(
                set(attributes) | set(named_attributes.values()))
        if obj._distinct or cls._need_subquery(arg, attributes, named_attributes):
            obj._arg = Subquery.create(arg)
            obj._heading = obj._arg.heading.project(attributes, named_attributes)
            if not include_primary_key:
                obj._heading = obj._heading.extend_primary_key(attributes)
        else:
            obj._arg = arg
            obj._heading = obj._arg.heading.project(attributes, named_attributes)
            obj &= arg.restriction  # copy restriction when no subquery
        return obj

    @staticmethod
    def _need_subquery(arg, attributes, named_attributes):
        """
        Decide whether the projection argument needs to be wrapped in a subquery
        """
        if arg.heading.expressions or arg.distinct:  # argument has any renamed (computed) attributes
            return True
        restricting_attributes = arg.attributes_in_restriction()
        return (not restricting_attributes.issubset(attributes) or  # if any restricting attribute is projected out or
                any(v.strip() in restricting_attributes for v in named_attributes.values()))  # or renamed

    @property
    def from_clause(self):
        return self._arg.from_clause


class GroupBy(RelationalOperand):
    """
    GroupBy(rel, comp1='expr1', ..., compn='exprn')  produces a relation with the primary key specified by rel.heading.
    The computed arguments comp1, ..., compn use aggregation operators on the attributes of rel.
    GroupBy is used RelationalOperand.aggr and U.aggr.
    GroupBy is a private class in DataJoint, not exposed to users.
    """

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is not None:
            # copy constructor
            assert isinstance(arg, GroupBy), "GroupBy copy constructor requires a GroupBy object"
            self._connection = arg.connection
            self._heading = arg.heading
            self._arg = arg._arg
            self._keep_all_rows = arg._keep_all_rows

    @classmethod
    def create(cls, arg, group, attributes=None, named_attributes=None, keep_all_rows=False):
        if inspect.isclass(group) and issubclass(group, RelationalOperand):
            group = group()   # instantiate if a class
        assert_join_compatibility(arg, group)
        obj = cls()
        obj._keep_all_rows = keep_all_rows
        if not set(group.primary_key) - set(arg.primary_key):
            raise DataJointError(
                'The primary key of the grouped relation must contain additional attributes.')
        obj._arg = (Join.make_argument_subquery(group) if isinstance(arg, U)
                    else Join.create(arg, group, keep_all_rows=keep_all_rows))
        obj._connection = obj._arg.connection
        # always include primary key of arg
        attributes = (list(a for a in arg.primary_key if a not in named_attributes.values()) +
                      list(a for a in attributes if a not in arg.primary_key))
        obj._heading = obj._arg.heading.project(
            attributes, named_attributes, force_primary_key=arg.primary_key)
        return obj

    def make_sql(self, select_fields=None):
        return 'SELECT {fields} FROM {from_}{where} GROUP  BY `{group_by}`{having}'.format(
            fields=self.get_select_fields(select_fields),
            from_=self._arg.from_clause,
            where=self._arg.where_clause,
            group_by='`,`'.join(self.primary_key),
            having=re.sub(r'^ WHERE', ' HAVING', self.where_clause))

    def __len__(self):
        return len(Subquery.create(self))


class Subquery(RelationalOperand):
    """
    A Subquery encapsulates its argument in a SELECT statement, enabling its use as a subquery.
    The attribute list and the WHERE clause are resolved.  Thus, a subquery no longer has any renamed attributes.
    A subquery of a subquery is a just a copy of the subquery with no change in SQL.
    """
    __count = count()

    def __init__(self, arg=None):
        super().__init__(arg)
        if arg is not None:
            # copy constructor
            assert isinstance(arg, Subquery)
            self._connection = arg.connection
            self._heading = arg.heading
            self._arg = arg._arg

    @classmethod
    def create(cls, arg):
        """
        construct a subquery from arg
        """
        obj = cls()
        obj._connection = arg.connection
        obj._heading = arg.heading.make_subquery_heading()
        obj._arg = arg
        return obj

    @property
    def from_clause(self):
        return '(' + self._arg.make_sql() + ') as `_s%x`' % next(self.__count)

    def get_select_fields(self, select_fields=None):
        return '*' if select_fields is None else self.heading.project(select_fields).as_sql


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

    >>> dj.U('contrast', 'brightness') & stimulus

    The following expression produces a relation containing all unique combinations of contrast and brightness that is
    contained in relation1 but not contained in relation 2.

    >>> (dj.U('contrast', 'brightness') & relation1) - relation2

    Relational aggregation:

    In aggregation, dj.U is used to compute aggregate expressions on the entire relation.

    The following expression produces a relation with one tuple and one attribute s containing the total number
    of tuples in relation:

    >>> dj.U().aggr(relation, n='count(*)')

    The following expression produces a relation with one tuple containing the number n of distinct values of attr
    in relation.

    >>> dj.U().aggr(relation, n='count(distinct attr)')

    The following expression produces a relation with one tuple and one attribute s containing the total sum of attr
    from relation:

    >>> dj.U().aggr(relation, s='sum(attr)')   # sum of attr from the entire relation

    The following expression produces a relation with the count n of tuples in relation containing each unique
    combination of values in attr1 and attr2.

    >>> dj.U(attr1,attr2).aggr(relation, n='count(*)')

    Joins:

    If relation rel has attributes 'attr1' and 'attr2', then rel*dj.U('attr1','attr2') or produces a relation that is
    identical to rel except attr1 and attr2 are included in the primary key.  This is useful for producing a join on
    non-primary key attributes.
    For example, if attr is in both rel1 and rel2 but not in their primary  keys, then rel1*rel2 will throw an error
    because in most cases, it does not make sense to join on non-primary key attributes and users must first rename
    attr in one of the operands.  The expression dj.U('attr')*rel1*rel2 overrides this constraint.
    Join is commutative.
    """

    def __init__(self, *primary_key):
        self._primary_key = primary_key

    @property
    def primary_key(self):
        return self._primary_key

    def __and__(self, relation):
        if inspect.isclass(relation) and issubclass(relation, RelationalOperand):
            relation = relation()   # instantiate if a class
        if not isinstance(relation, RelationalOperand):
            raise DataJointError('Relation U can only be restricted with another relation.')
        return Projection.create(relation, attributes=self.primary_key,
                                 named_attributes=dict(), include_primary_key=False)

    def __mul__(self, relation):
        """
        Joining relation U * relation has the effect of adding the attributes of U to the primary key of
        the other relation.
        :param relation: other relation
        :return: a copy of the other relation with the primary key extended.
        """
        if inspect.isclass(relation) and issubclass(relation, RelationalOperand):
            relation = relation()   # instantiate if a class
        if not isinstance(relation, RelationalOperand):
            raise DataJointError('Relation U can only be joined with another relation.')
        copy = relation.__class__(relation)
        copy._heading = copy.heading.extend_primary_key(self.primary_key)
        return copy

    def aggr(self, group, **named_attributes):
        """
        Aggregation of the type U('attr1','attr2').aggr(rel, computation="expression")
        has the primary key ('attr1','attr2') and performs aggregation computations for all matching tuples of relation.
        :param group:  The other relation which will be aggregated.
        :param named_attributes: computations of the form new_attribute="sql expression on attributes of group"
        :return: The new relation
        """
        return (
            GroupBy.create(self, group=group, keep_all_rows=False, attributes=(), named_attributes=named_attributes)
            if self.primary_key else
            Projection.create(group, attributes=(), named_attributes=named_attributes, include_primary_key=False))

    aggregate = aggr  # alias for aggr
