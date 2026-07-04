"""
Runtime gates for ``dj.config["strict_provenance"]``.

When the flag is enabled, this module's context (set by ``AutoPopulate._populate_one``)
tracks which tables and primary key the currently-executing ``make()`` is
allowed to read and write. The read gate in :func:`assert_read_allowed`
fires inside ``QueryExpression.cursor``. The write gate has two parts: the
target check in :func:`assert_write_allowed` fires inside ``Table.insert``
(before rows are materialized), and the per-row key-consistency check in
:func:`assert_row_key_allowed` fires inside ``Table._insert_rows`` as each row
is materialized — so the gate never consumes the caller's ``rows`` iterable.

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


def assert_write_allowed(target_table, verb: str = "insert into") -> None:
    """
    Verify the *target* of a write is allowed under the active strict-make context.

    Called from ``Table.insert`` (after the existing ``_allow_insert`` check and
    before any rows are materialized) and from ``Table.update1`` (before the
    UPDATE is issued). No-op when no strict-make context is active.

    Allowed targets:

    - The current ``make()`` target (``self``) or one of its Part tables.

    Per-row key consistency is checked separately by :func:`assert_row_key_allowed`
    as rows are materialized, so this gate never consumes the caller's ``rows``
    iterable — a one-shot generator must survive to reach ``insert``.

    Parameters
    ----------
    target_table : Table
        The table being written to.
    verb : str
        Phrase naming the write operation in the error message
        (``"insert into"`` or ``"update1 on"``).

    Raises ``DataJointError`` if the target is not permitted.
    """
    ctx = _active_strict_make.get()
    if ctx is None:
        return

    make_target, _allowed_tables, _key = ctx

    # Target must be `make_target` (self) or one of its Parts.
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
            f"strict_provenance=True: {verb} {target_name!r} is not permitted "
            f"inside make() for {make_target.full_table_name!r}. Only the target "
            f"table and its Part tables may be written."
        )


def assert_row_key_allowed(row, action: str = "insert") -> None:
    """
    Verify a single written row's key columns match the active ``make()`` key.

    Called per row from ``Table._insert_rows`` as rows are materialized (so the
    check sees a concrete row without the write gate having to consume the
    caller's ``rows`` iterable), and from ``Table.update1`` with the update row.
    No-op when no strict-make context is active or when ``row`` is not a dict
    (numpy records / bare sequences carry no field names to check by — same as
    the previous behavior).

    Parameters
    ----------
    row : dict
        The row being written.
    action : str
        ``"insert"`` or ``"update"`` — selects the error-message wording.

    Raises ``DataJointError`` on a mismatch.
    """
    ctx = _active_strict_make.get()
    if ctx is None:
        return
    if not isinstance(row, dict):
        return
    _make_target, _allowed_tables, key = ctx
    _check_row_key(row, key, action)


def _check_row_key(row: dict, current_key: dict, action: str = "insert") -> None:
    """Raise if any row attribute overlapping with the current key has a different value."""
    past, plural = {"insert": ("inserted", "Inserts"), "update": ("updated", "Updates")}[action]
    for k, v in current_key.items():
        if k in row and row[k] != v:
            raise DataJointError(
                f"strict_provenance=True: {past} row's {k!r}={row[k]!r} does not "
                f"match the current make() key's {k!r}={v!r}. {plural} must be "
                f"consistent with the key being populated."
            )
