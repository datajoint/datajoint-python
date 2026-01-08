"""
SQL WHERE clause generation from DataJoint restriction conditions.

This module provides utilities for converting various restriction formats
(dicts, strings, QueryExpressions) into SQL WHERE clauses.
"""

from __future__ import annotations

import collections
import datetime
import decimal
import inspect
import json
import logging
import re
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import numpy
import pandas

from .errors import DataJointError

if TYPE_CHECKING:
    from .expression import QueryExpression

logger = logging.getLogger(__name__.split(".")[0])

JSON_PATTERN = re.compile(r"^(?P<attr>\w+)(\.(?P<path>[\w.*\[\]]+))?(:(?P<type>[\w(,\s)]+))?$")


def translate_attribute(key: str) -> tuple[dict | None, str]:
    """
    Translate an attribute key, handling JSON path notation.

    Parameters
    ----------
    key : str
        Attribute name, optionally with JSON path (e.g., ``"attr.path.field"``).

    Returns
    -------
    tuple
        (match_dict, sql_expression) where match_dict contains parsed
        components or None if no JSON path.
    """
    match = JSON_PATTERN.match(key)
    if match is None:
        return match, key
    match = match.groupdict()
    if match["path"] is None:
        return match, match["attr"]
    else:
        return match, "json_value(`{}`, _utf8mb4'$.{}'{})".format(
            *[((f" returning {v}" if k == "type" else v) if v else "") for k, v in match.items()]
        )


class PromiscuousOperand:
    """
    Wrapper to bypass join compatibility checking.

    Used when you want to force a natural join without semantic matching.

    Parameters
    ----------
    operand : QueryExpression
        The operand to wrap.
    """

    def __init__(self, operand: QueryExpression) -> None:
        self.operand = operand


class AndList(list):
    """
    List of conditions combined with logical AND.

    All conditions in the list are AND-ed together. Other collections
    (lists, sets, QueryExpressions) are OR-ed.

    Examples
    --------
    >>> expr & dj.AndList((cond1, cond2, cond3))
    # equivalent to
    >>> expr & cond1 & cond2 & cond3
    """

    def append(self, restriction: Any) -> None:
        if isinstance(restriction, AndList):
            # extend to reduce nesting
            self.extend(restriction)
        else:
            super().append(restriction)


@dataclass
class Top:
    """
    Restrict query to top N entities with ordering.

    In SQL, corresponds to ``ORDER BY ... LIMIT ... OFFSET``.

    Parameters
    ----------
    limit : int, optional
        Maximum number of rows to return. Default 1.
    order_by : str or list[str] or None, optional
        Attributes to order by. ``"KEY"`` for primary key order.
        ``None`` means inherit ordering from an existing Top (or default to KEY).
        Default ``"KEY"``.
    offset : int, optional
        Number of rows to skip. Default 0.

    Examples
    --------
    >>> query & dj.Top(5)                    # Top 5 by primary key
    >>> query & dj.Top(10, 'score DESC')     # Top 10 by score descending
    >>> query & dj.Top(10, order_by=None)    # Top 10, inherit existing order
    >>> query & dj.Top(5, offset=10)         # Skip 10, take 5
    """

    limit: int | None = 1
    order_by: str | list[str] | None = "KEY"
    offset: int = 0

    def __post_init__(self) -> None:
        self.offset = self.offset or 0

        if self.limit is not None and not isinstance(self.limit, int):
            raise TypeError("Top limit must be an integer")
        if self.order_by is not None:
            if not isinstance(self.order_by, (str, collections.abc.Sequence)) or not all(
                isinstance(r, str) for r in self.order_by
            ):
                raise TypeError("Top order_by attributes must all be strings")
            if isinstance(self.order_by, str):
                self.order_by = [self.order_by]
        if not isinstance(self.offset, int):
            raise TypeError("The offset argument must be an integer")
        if self.offset and self.limit is None:
            self.limit = 999999999999  # arbitrary large number to allow query

    def merge(self, other: "Top") -> "Top":
        """
        Merge another Top into this one (when other inherits ordering).

        Used when ``other.order_by`` is None or matches ``self.order_by``.

        Parameters
        ----------
        other : Top
            The Top to merge. Its order_by should be None or equal to self.order_by.

        Returns
        -------
        Top
            New Top with merged limit/offset and preserved ordering.
        """
        # Compute effective limit (minimum of defined limits)
        if self.limit is None and other.limit is None:
            new_limit = None
        elif self.limit is None:
            new_limit = other.limit
        elif other.limit is None:
            new_limit = self.limit
        else:
            new_limit = min(self.limit, other.limit)

        return Top(
            limit=new_limit,
            order_by=self.order_by,  # preserve existing ordering
            offset=self.offset + other.offset,  # offsets add
        )


class Not:
    """
    Invert a restriction condition.

    Parameters
    ----------
    restriction : any
        Restriction condition to negate.

    Examples
    --------
    >>> table - condition  # equivalent to table & Not(condition)
    """

    def __init__(self, restriction: Any) -> None:
        self.restriction = restriction


def assert_join_compatibility(
    expr1: QueryExpression,
    expr2: QueryExpression,
    semantic_check: bool = True,
) -> None:
    """
    Check if two expressions are join-compatible.

    Parameters
    ----------
    expr1 : QueryExpression
        First expression.
    expr2 : QueryExpression
        Second expression.
    semantic_check : bool, optional
        If True (default), use semantic matching and error on non-homologous
        namesakes (same name, different lineage). If False, use natural join.

    Raises
    ------
    DataJointError
        If semantic_check is True and expressions have non-homologous namesakes.

    Notes
    -----
    With semantic_check=True:
        Prevents accidental joins on attributes that share names but represent
        different entities. If ~lineage table doesn't exist, a warning is issued.

    With semantic_check=False:
        All namesake attributes are matched (natural join behavior).
    """
    from .expression import QueryExpression, U

    for rel in (expr1, expr2):
        if not isinstance(rel, (U, QueryExpression)):
            raise DataJointError("Object %r is not a QueryExpression and cannot be joined." % rel)

    # dj.U is always compatible (it represents all possible lineages)
    if isinstance(expr1, U) or isinstance(expr2, U):
        return

    if semantic_check:
        # Check if lineage tracking is available for both expressions
        if not expr1.heading.lineage_available or not expr2.heading.lineage_available:
            logger.warning(
                "Semantic check disabled: ~lineage table not found. "
                "To enable semantic matching, rebuild lineage with: "
                "schema.rebuild_lineage()"
            )
            return

        # Error on non-homologous namesakes
        namesakes = set(expr1.heading.names) & set(expr2.heading.names)
        for name in namesakes:
            lineage1 = expr1.heading[name].lineage
            lineage2 = expr2.heading[name].lineage
            # Semantic match requires both lineages to be non-None and equal
            if lineage1 is None or lineage2 is None or lineage1 != lineage2:
                raise DataJointError(
                    f"Cannot join on attribute `{name}`: "
                    f"different lineages ({lineage1} vs {lineage2}). "
                    f"Use .proj() to rename one of the attributes."
                )


def make_condition(
    query_expression: QueryExpression,
    condition: Any,
    columns: set[str],
    semantic_check: bool = True,
) -> str | bool:
    """
    Translate a restriction into an SQL WHERE clause condition.

    Parameters
    ----------
    query_expression : QueryExpression
        The expression to apply the condition to.
    condition : any
        Valid restriction: str, dict, bool, QueryExpression, AndList,
        numpy.void, pandas.DataFrame, or iterable of restrictions.
    columns : set[str]
        Set passed by reference to collect column names used in the condition.
    semantic_check : bool, optional
        If True (default), use semantic matching and error on conflicts.

    Returns
    -------
    str or bool
        SQL condition string, or bool if condition evaluates to constant.

    Notes
    -----
    Restriction types are processed as follows:

    - ``str``: Used directly as SQL condition
    - ``dict``: AND of equality conditions for matching attributes
    - ``bool``: Returns the boolean value (possibly negated)
    - ``QueryExpression``: Generates subquery (semijoin/antijoin)
    - ``AndList``: AND of all conditions
    - ``list/set/tuple``: OR of all conditions
    - ``numpy.void``: Like dict, from record array
    - ``pandas.DataFrame``: Converted to records, then OR-ed
    """
    from .expression import Aggregation, QueryExpression, U

    def prep_value(k, v):
        """prepare SQL condition"""
        key_match, k = translate_attribute(k)
        if key_match["path"] is None:
            k = f"`{k}`"
        if query_expression.heading[key_match["attr"]].json and key_match["path"] is not None and isinstance(v, dict):
            return f"{k}='{json.dumps(v)}'"
        if v is None:
            return f"{k} IS NULL"
        if query_expression.heading[key_match["attr"]].uuid:
            if not isinstance(v, uuid.UUID):
                try:
                    v = uuid.UUID(v)
                except (AttributeError, ValueError):
                    raise DataJointError("Badly formed UUID {v} in restriction by `{k}`".format(k=k, v=v))
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
        return combine_conditions(negate, conditions=[condition.strip().replace("%", "%%")])  # escape %, see issue #376

    # restrict by AndList
    if isinstance(condition, AndList):
        # omit all conditions that evaluate to True
        items = [
            item
            for item in (make_condition(query_expression, cond, columns, semantic_check) for cond in condition)
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
        common_attributes = set(c.split(".", 1)[0] for c in condition).intersection(query_expression.heading.names)
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
        common_attributes = set(condition.dtype.fields).intersection(query_expression.heading.names)
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
    if isinstance(condition, QueryExpression):
        assert_join_compatibility(query_expression, condition, semantic_check=semantic_check)
        # Match on all non-hidden namesakes (hidden attributes excluded)
        common_attributes = [q for q in condition.heading.names if q in query_expression.heading.names]
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
        or_list = [make_condition(query_expression, q, columns, semantic_check) for q in condition]
    except TypeError:
        raise DataJointError("Invalid restriction type %r" % condition)
    else:
        or_list = [item for item in or_list if item is not False]  # ignore False conditions
        if any(item is True for item in or_list):  # if any item is True, entirely True
            return not negate
        return f"{'NOT ' if negate else ''} ({' OR '.join(or_list)})" if or_list else negate


def extract_column_names(sql_expression: str) -> set[str]:
    r"""
    Extract column names from an SQL expression.

    Parameters
    ----------
    sql_expression : str
        SQL expression (e.g., WHERE clause) to parse.

    Returns
    -------
    set[str]
        Set of extracted column names.

    Notes
    -----
    Parsing is MySQL-specific. Identifies columns by:

    1. Names in backticks (``\`column\```)
    2. Bare identifiers not followed by ``(`` (excludes functions)
    3. Excludes SQL reserved words (IS, IN, AND, OR, etc.)
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
