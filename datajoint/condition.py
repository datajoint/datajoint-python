"""methods for generating SQL WHERE clauses from datajoint restriction conditions"""

import collections
import datetime
import decimal
import inspect
import json
import re
import uuid
from dataclasses import dataclass
from typing import List, Union

import numpy
import pandas

from .errors import DataJointError

JSON_PATTERN = re.compile(
    r"^(?P<attr>\w+)(\.(?P<path>[\w.*\[\]]+))?(:(?P<type>[\w(,\s)]+))?$"
)


def translate_attribute(key):
    match = JSON_PATTERN.match(key)
    if match is None:
        return match, key
    match = match.groupdict()
    if match["path"] is None:
        return match, match["attr"]
    else:
        return match, "json_value(`{}`, _utf8mb4'$.{}'{})".format(
            *[
                ((f" returning {v}" if k == "type" else v) if v else "")
                for k, v in match.items()
            ]
        )


class PromiscuousOperand:
    """
    A container for an operand to ignore join compatibility
    """

    def __init__(self, operand):
        self.operand = operand


class AndList(list):
    """
    A list of conditions to by applied to a query expression by logical conjunction: the
    conditions are AND-ed. All other collections (lists, sets, other entity sets, etc) are
    applied by logical disjunction (OR).

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


@dataclass
class Top:
    """
    A restriction to the top entities of a query.
    In SQL, this corresponds to ORDER BY ... LIMIT ... OFFSET
    """

    limit: Union[int, None] = 1
    order_by: Union[str, List[str]] = "KEY"
    offset: int = 0

    def __post_init__(self):
        self.order_by = self.order_by or ["KEY"]
        self.offset = self.offset or 0

        if self.limit is not None and not isinstance(self.limit, int):
            raise TypeError("Top limit must be an integer")
        if not isinstance(self.order_by, (str, collections.abc.Sequence)) or not all(
            isinstance(r, str) for r in self.order_by
        ):
            raise TypeError("Top order_by attributes must all be strings")
        if not isinstance(self.offset, int):
            raise TypeError("The offset argument must be an integer")
        if self.offset and self.limit is None:
            self.limit = 999999999999  # arbitrary large number to allow query
        if isinstance(self.order_by, str):
            self.order_by = [self.order_by]


class Not:
    """invert restriction"""

    def __init__(self, restriction):
        self.restriction = restriction


def assert_join_compatibility(expr1, expr2):
    """
    Determine if expressions expr1 and expr2 are join-compatible.  To be join-compatible,
    the matching attributes in the two expressions must be in the primary key of one or the
    other expression.
    Raises an exception if not compatible.

    :param expr1: A QueryExpression object
    :param expr2: A QueryExpression object
    """
    from .expression import QueryExpression, U

    for rel in (expr1, expr2):
        if not isinstance(rel, (U, QueryExpression)):
            raise DataJointError(
                "Object %r is not a QueryExpression and cannot be joined." % rel
            )
    if not isinstance(expr1, U) and not isinstance(
        expr2, U
    ):  # dj.U is always compatible
        try:
            raise DataJointError(
                "Cannot join query expressions on dependent attribute `%s`"
                % next(
                    r
                    for r in set(expr1.heading.secondary_attributes).intersection(
                        expr2.heading.secondary_attributes
                    )
                )
            )
        except StopIteration:
            pass  # all ok


def make_condition(query_expression, condition, columns):
    """
    Translate the input condition into the equivalent SQL condition (a string)

    :param query_expression: a dj.QueryExpression object to apply condition
    :param condition: any valid restriction object.
    :param columns: a set passed by reference to collect all column names used in the
        condition.
    :return: an SQL condition string or a boolean value.
    """
    from .expression import Aggregation, QueryExpression, U

    def prep_value(k, v):
        """prepare SQL condition"""
        key_match, k = translate_attribute(k)
        if key_match["path"] is None:
            k = f"`{k}`"
        if (
            query_expression.heading[key_match["attr"]].json
            and key_match["path"] is not None
            and isinstance(v, dict)
        ):
            return f"{k}='{json.dumps(v)}'"
        if v is None:
            return f"{k} IS NULL"
        if query_expression.heading[key_match["attr"]].uuid:
            if not isinstance(v, uuid.UUID):
                try:
                    v = uuid.UUID(v)
                except (AttributeError, ValueError):
                    raise DataJointError(
                        "Badly formed UUID {v} in restriction by `{k}`".format(k=k, v=v)
                    )
            return f"{k}=X'{v.bytes.hex()}'"
        if isinstance(
            v,
            (
                datetime.date,
                datetime.datetime,
                datetime.time,
                decimal.Decimal,
                list,
            ),
        ):
            return f'{k}="{v}"'
        if isinstance(v, str):
            v = v.replace("%", "%%").replace("\\", "\\\\")
            return f'{k}="{v}"'
        return f"{k}={v}"

    def combine_conditions(negate, conditions):
        return f"{'NOT ' if negate else ''} ({')AND('.join(conditions)})"

    negate = False
    while isinstance(condition, Not):
        negate = not negate
        condition = condition.restriction

    # restrict by string
    if isinstance(condition, str):
        columns.update(extract_column_names(condition))
        return combine_conditions(
            negate, conditions=[condition.strip().replace("%", "%%")]
        )  # escape %, see issue #376

    # restrict by AndList
    if isinstance(condition, AndList):
        # omit all conditions that evaluate to True
        items = [
            item
            for item in (
                make_condition(query_expression, cond, columns) for cond in condition
            )
            if item is not True
        ]
        if any(item is False for item in items):
            return negate  # if any item is False, the whole thing is False
        if not items:
            return not negate  # and empty AndList is True
        return combine_conditions(negate, conditions=items)

    # restriction by dj.U evaluates to True
    if isinstance(condition, U):
        return not negate

    # restrict by boolean
    if isinstance(condition, bool):
        return negate != condition

    # restrict by a mapping/dict -- convert to an AndList of string equality conditions
    if isinstance(condition, collections.abc.Mapping):
        common_attributes = set(c.split(".", 1)[0] for c in condition).intersection(
            query_expression.heading.names
        )
        if not common_attributes:
            return not negate  # no matching attributes -> evaluates to True
        columns.update(common_attributes)
        return combine_conditions(
            negate,
            conditions=[
                prep_value(k, v)
                for k, v in condition.items()
                if k.split(".", 1)[0] in common_attributes  # handle json indexing
            ],
        )

    # restrict by a numpy record -- convert to an AndList of string equality conditions
    if isinstance(condition, numpy.void):
        common_attributes = set(condition.dtype.fields).intersection(
            query_expression.heading.names
        )
        if not common_attributes:
            return not negate  # no matching attributes -> evaluate to True
        columns.update(common_attributes)
        return combine_conditions(
            negate,
            conditions=[prep_value(k, condition[k]) for k in common_attributes],
        )

    # restrict by a QueryExpression subclass -- trigger instantiation and move on
    if inspect.isclass(condition) and issubclass(condition, QueryExpression):
        condition = condition()

    # restrict by another expression (aka semijoin and antijoin)
    check_compatibility = True
    if isinstance(condition, PromiscuousOperand):
        condition = condition.operand
        check_compatibility = False

    if isinstance(condition, QueryExpression):
        if check_compatibility:
            assert_join_compatibility(query_expression, condition)
        common_attributes = [
            q for q in condition.heading.names if q in query_expression.heading.names
        ]
        columns.update(common_attributes)
        if isinstance(condition, Aggregation):
            condition = condition.make_subquery()
        return (
            # without common attributes, any non-empty set matches everything
            (not negate if condition else negate)
            if not common_attributes
            else "({fields}) {not_}in ({subquery})".format(
                fields="`" + "`,`".join(common_attributes) + "`",
                not_="not " if negate else "",
                subquery=condition.make_sql(common_attributes),
            )
        )

    # restrict by pandas.DataFrames
    if isinstance(condition, pandas.DataFrame):
        condition = condition.to_records()  # convert to numpy.recarray and move on

    # if iterable (but not a string, a QueryExpression, or an AndList), treat as an OrList
    try:
        or_list = [make_condition(query_expression, q, columns) for q in condition]
    except TypeError:
        raise DataJointError("Invalid restriction type %r" % condition)
    else:
        or_list = [
            item for item in or_list if item is not False
        ]  # ignore False conditions
        if any(item is True for item in or_list):  # if any item is True, entirely True
            return not negate
        return (
            f"{'NOT ' if negate else ''} ({' OR '.join(or_list)})"
            if or_list
            else negate
        )


def extract_column_names(sql_expression):
    """
    extract all presumed column names from an sql expression such as the WHERE clause,
    for example.

    :param sql_expression: a string containing an SQL expression
    :return: set of extracted column names
    This may be MySQL-specific for now.
    """
    assert isinstance(sql_expression, str)
    result = set()
    s = sql_expression  # for terseness
    # remove escaped quotes
    s = re.sub(r"(\\\")|(\\\')", "", s)
    # remove quoted text
    s = re.sub(r"'[^']*'", "", s)
    s = re.sub(r'"[^"]*"', "", s)
    # find all tokens in back quotes and remove them
    result.update(re.findall(r"`([a-z][a-z_0-9]*)`", s))
    s = re.sub(r"`[a-z][a-z_0-9]*`", "", s)
    # remove space before parentheses
    s = re.sub(r"\s*\(", "(", s)
    # remove tokens followed by ( since they must be functions
    s = re.sub(r"(\b[a-z][a-z_0-9]*)\(", "(", s)
    remaining_tokens = set(re.findall(r"\b[a-z][a-z_0-9]*\b", s))
    # update result removing reserved words
    result.update(
        remaining_tokens
        - {
            "is",
            "in",
            "between",
            "like",
            "and",
            "or",
            "null",
            "not",
            "interval",
            "second",
            "minute",
            "hour",
            "day",
            "month",
            "week",
            "year",
        }
    )
    return result
