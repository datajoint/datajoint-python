""" methods for generating SQL WHERE clauses from datajoint restriction conditions """

import inspect
import collections
import re
import uuid
import datetime
import decimal
import numpy
import pandas
from .errors import DataJointError


class AndList(list):
    """
    A list of conditions to by applied to a query expression by logical conjunction: the conditions are AND-ed.
    All other collections (lists, sets, other entity sets, etc) are applied by logical disjunction (OR).

    Example:
    expr2 = expr & dj.AndList((cond1, cond2, cond3))
    is equivalent to
    expr2 = expr & cond1 & cond2 & cond3
    """

    def append(self, restriction):
        if isinstance(restriction, AndList):
            # extend to reduce nesting
            self.extend(restriction)
        else:
            super().append(restriction)


class Not:
    """ invert restriction """

    def __init__(self, restriction):
        self.restriction = restriction


def assert_join_compatibility(expr1, expr2):
    """
    Determine if expressions expr1 and expr2 are join-compatible.  To be join-compatible, the matching attributes
    in the two expressions must be in the primary key of one or the other expression.
    Raises an exception if not compatible.
    :param expr1: A QueryExpression object
    :param expr2: A QueryExpression object
    """
    from .expression import QueryExpression, U

    for rel in (expr1, expr2):
        if not isinstance(rel, (U, QueryExpression)):
            raise DataJointError('Object %r is not a QueryExpression and cannot be joined.' % rel)
    if not isinstance(expr1, U) and not isinstance(expr2, U):  # dj.U is always compatible
        try:
            raise DataJointError("Cannot join query expressions on dependent attribute `%s`" % next(r for r in set(
                expr1.heading.secondary_attributes).intersection(expr2.heading.secondary_attributes)))
        except StopIteration:
            pass


def make_condition(query_expression, condition):
    """
    Translate the input condition into the equivalent SQL condition (a string)
    :param query_expression: a dj.QueryExpression object to apply condition
    :param condition: any valid restriction object.
    :return: an SQL condition string or a boolean value.
    """
    from .expression import QueryExpression, U

    def prep_value(k, v):
        """prepare value v for inclusion as a string in an SQL condition"""
        if query_expression.heading[k].uuid:
            if not isinstance(v, uuid.UUID):
                try:
                    v = uuid.UUID(v)
                except (AttributeError, ValueError):
                    raise DataJointError('Badly formed UUID {v} in restriction by `{k}`'.format(k=k, v=v)) from None
            return "X'%s'" % v.bytes.hex()
        if isinstance(v, (datetime.date, datetime.datetime, datetime.time, decimal.Decimal)):
            return '"%s"' % v
        return '%r' % v

    negate = False
    while isinstance(condition, Not):
        negate = not negate
        condition = condition.restriction
    template = "NOT (%s)" if negate else "%s"

    # restrict by string
    if isinstance(condition, str):
        return template % condition.strip().replace("%", "%%")  # escape % in strings, see issue #376

    # restrict by AndList
    if isinstance(condition, AndList):
        # omit all conditions that evaluate to True
        items = [item for item in (query_expression._make_condition(i) for i in condition) if item is not True]
        if any(item is False for item in items):
            return negate  # if any item is False, the whole thing is False
        if not items:
            return not negate  # and empty AndList is True
        return template % ('(' + ') AND ('.join(items) + ')')

    # restriction by dj.U evaluates to True
    if isinstance(condition, U):
        return not negate

    # restrict by boolean
    if isinstance(condition, bool):
        return negate != condition

    # restrict by a mapping such as a dict -- convert to an AndList of string equality conditions
    if isinstance(condition, collections.abc.Mapping):
        return template % query_expression._make_condition(
            AndList('`%s`=%s' % (k, prep_value(k, v)) for k, v in condition.items() if k in query_expression.heading))

    # restrict by a numpy record -- convert to an AndList of string equality conditions
    if isinstance(condition, numpy.void):
        return template % make_condition(query_expression,
                                         AndList(('`%s`=%s' % (k, prep_value(k, condition[k]))
                                                  for k in condition.dtype.fields if k in query_expression.heading)))

    # restrict by a QueryExpression subclass -- triggers instantiation
    if inspect.isclass(condition) and issubclass(condition, QueryExpression):
        condition = condition()

    # restrict by another expression (aka semijoin and antijoin)
    if isinstance(condition, QueryExpression):
        assert_join_compatibility(query_expression, condition)
        common_attributes = [q for q in condition.heading.names if q in query_expression.heading.names]
        return (
            # without common attributes, any non-empty set matches everything
            (not negate if condition else negate) if not common_attributes
            else '({fields}) {not_}in ({subquery})'.format(
                fields='`' + '`,`'.join(common_attributes) + '`',
                not_="not " if negate else "",
                subquery=condition.make_sql(common_attributes)))

    # restrict by pandas.DataFrames
    if isinstance(condition, pandas.DataFrame):
        condition = condition.to_records()  # convert to numpy.recarray

    # if iterable (but not a string, a QueryExpression, or an AndList), treat as an OrList
    try:
        or_list = [make_condition(query_expression, q) for q in condition]
    except TypeError:
        raise DataJointError('Invalid restriction type %r' % condition)
    else:
        or_list = [item for item in or_list if item is not False]  # ignore all False conditions
        if any(item is True for item in or_list):  # if any item is True, the whole thing is True
            return not negate
        return template % ('(%s)' % ' OR '.join(or_list)) if or_list else negate  # an empty or list is False


def get_attribute_names_from_condition(condition):
    """
    extract all column names from a WHERE clause condition
    :param condition: SQL condition
    :return: list of inferred column names
    This may be MySQL-specific
    """

    # remove escaped quotes
    condition = re.sub(r'(\\\")|(\\\')', '', condition)

    # remove quoted text
    condition = re.sub(r"'[^']*'", "", condition)
    condition = re.sub(r'"[^"]*"', '', condition)

    result = set()

    # find all tokens in back quotes and remove them
    result.update(re.findall(r"`([a-z][a-z_0-9]*)`", condition))
    condition = re.sub(r"`[a-z][a-z_0-9]*`", '', condition)

    # remove space before parentheses
    condition = re.sub(r"\s*\(", "(", condition)

    # remove tokens followed by ( since they must be functions
    condition = re.sub(r"(\b[a-z][a-z_0-9]*)\(", "(", condition)
    remaining_tokens = set(re.findall(r"`\b[a-z][a-z_0-9]*\b", condition))

    # update result removing reserved words
    result.update(remaining_tokens - {"in", "between", "like", "and", "or"})

    return result
