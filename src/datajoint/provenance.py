"""
Runtime gates for ``dj.config["strict_provenance"]``.

When the flag is enabled, this module's context (set by ``AutoPopulate._populate_one``)
tracks which tables and primary key the currently-executing ``make()`` is
allowed to read and write. The read gate in :func:`assert_read_allowed`
fires inside ``QueryExpression.cursor``; the write gate in
:func:`assert_write_allowed` fires inside ``Table.insert``.

The contract is documented in
``datajoint-docs/src/reference/specs/provenance.md`` §3.

Implementation note: the active-make context is stored in a
``contextvars.ContextVar`` so it propagates correctly across threads
that share the parent's context (e.g. the populate-in-subprocess path
which uses ``multiprocessing`` workers, each of which inherits its
parent's contextvar binding at fork time).
"""

from __future__ import annotations

from contextvars import ContextVar
from typing import TYPE_CHECKING, Optional, Tuple

from .errors import DataJointError

if TYPE_CHECKING:
    from .table import Table


# Active context: (the target table, the set of allowed full table names, the current key dict)
_active_strict_make: ContextVar[Optional[Tuple["Table", frozenset[str], dict]]] = ContextVar(
    "_dj_active_strict_make", default=None
)


def push_strict_make_context(target: "Table", allowed_tables: frozenset[str], key: dict):
    """
    Push a strict-make context for the duration of one ``make()`` invocation.

    Returns a token that the caller must pass to :func:`pop_strict_make_context`
    in a ``finally`` block.
    """
    return _active_strict_make.set((target, allowed_tables, key))


def pop_strict_make_context(token) -> None:
    """Pop the strict-make context using a token from :func:`push_strict_make_context`."""
    _active_strict_make.reset(token)


def get_active_context():
    """Return the currently-active strict-make context, or None."""
    return _active_strict_make.get()


def _base_tables(query_expression) -> set[str]:
    """
    Return the set of base-table SQL names that a QueryExpression reads from.

    For a single-table expression (FreeTable / Table / restricted variants),
    returns ``{full_table_name}``. For compound expressions (joins,
    projections of joins), traverses ``support`` recursively.
    """
    # FreeTable / Table: has full_table_name directly
    ftn = getattr(query_expression, "full_table_name", None)
    if isinstance(ftn, str):
        return {ftn}

    bases: set[str] = set()
    support = getattr(query_expression, "_support", None) or []
    for s in support:
        if isinstance(s, str):
            # Direct table name in the support list
            bases.add(s)
        else:
            # Subquery — recurse
            bases.update(_base_tables(s))
    return bases


def assert_read_allowed(query_expression) -> None:
    """
    Verify a fetch is allowed under the active strict-make context.

    Called from ``QueryExpression.cursor`` before SQL is issued. No-op when
    no strict-make context is active (i.e. outside ``make()`` or when
    ``strict_provenance`` is False).

    Allowed reads:

    - Any table in the active context's ``allowed_tables`` set. The set is
      built from ``self.upstream`` (the ancestor graph) plus the target
      table and its Parts.

    Anything else raises ``DataJointError``.

    Known limitation (will sharpen in a follow-up): the check does not
    distinguish reads that came *through* ``self.upstream`` from reads of
    the same ancestor via a direct expression. Both are allowed if the
    table is in the allowed set. The intent is to catch reads from
    *undeclared* dependencies; tightening the "must come through
    ``self.upstream``" path requires propagating an attribution marker
    through QueryExpression composition and is deferred.
    """
    ctx = _active_strict_make.get()
    if ctx is None:
        return  # strict mode off, or outside make()

    _target, allowed_tables, _key = ctx
    bases = _base_tables(query_expression)
    if not bases:
        return  # nothing to check (e.g. dj.U expressions)

    disallowed = bases - allowed_tables
    if disallowed:
        raise DataJointError(
            f"strict_provenance=True: read from undeclared table(s) "
            f"{sorted(disallowed)} is not permitted inside make(). "
            f"Use self.upstream[T] for declared ancestors, or declare a "
            f"foreign-key dependency on the table you want to read."
        )


def assert_write_allowed(target_table, rows) -> None:
    """
    Verify an insert is allowed under the active strict-make context.

    Called from ``Table.insert`` after the existing ``_allow_insert`` check.
    No-op when no strict-make context is active.

    Allowed writes:

    - Target is the current ``make()`` target (``self``) or one of its Part
      tables.
    - Every row's primary-key columns that overlap with the current ``key``
      must equal ``key``'s values.

    Anything else raises ``DataJointError``.
    """
    ctx = _active_strict_make.get()
    if ctx is None:
        return

    make_target, _allowed_tables, key = ctx

    # 1. Target must be `make_target` (self) or one of its Parts.
    target_name = getattr(target_table, "full_table_name", None)
    target_set = {make_target.full_table_name}
    # Collect Part tables of make_target via class __dict__ (not dir/getattr,
    # which would trigger descriptors like the _JobsDescriptor).
    from .user_tables import Part  # local import to avoid circular dep

    for cls in type(make_target).__mro__:
        for attr_name, attr in cls.__dict__.items():
            if attr_name.startswith("_"):
                continue
            if isinstance(attr, type) and issubclass(attr, Part):
                try:
                    part_ftn = attr().full_table_name
                    target_set.add(part_ftn)
                except Exception:
                    pass

    if target_name not in target_set:
        raise DataJointError(
            f"strict_provenance=True: insert into {target_name!r} is not permitted "
            f"inside make() for {make_target.full_table_name!r}. Only the target "
            f"table and its Part tables may be written."
        )

    # 2. Each row's key columns that overlap with the current key must match.
    if isinstance(rows, dict):
        _check_row_key(rows, key)
    else:
        try:
            for row in rows:
                if isinstance(row, dict):
                    _check_row_key(row, key)
                # Non-dict rows (tuples, etc.) bypass — older API; can't check.
        except TypeError:
            pass  # not iterable; let downstream code handle


def _check_row_key(row: dict, current_key: dict) -> None:
    """Raise if any row attribute overlapping with the current key has a different value."""
    for k, v in current_key.items():
        if k in row and row[k] != v:
            raise DataJointError(
                f"strict_provenance=True: inserted row's {k!r}={row[k]!r} does not "
                f"match the current make() key's {k!r}={v!r}. Inserts must be "
                f"consistent with the key being populated."
            )
