"""
Deployment-time operations for configuring an existing DataJoint pipeline.

This module hosts idempotent operational helpers — things you run as part of a
deploy hook to configure a schema for its environment, distinct from
:mod:`datajoint.migrate` which handles one-shot schema/state evolution.

The boundary between the two:

- :mod:`datajoint.migrate` — fix legacy state, evolve a schema definition,
  retroactive corrections. Cadence: one-shot. Examples: ``migrate_columns``,
  ``add_job_metadata_columns``, ``rebuild_lineage``.
- :mod:`datajoint.deploy` — configure an environment for a consumer's
  requirements (CDC tools, replication, role grants, performance tuning).
  Cadence: re-runnable, idempotent. Examples: :func:`set_replica_identity`,
  :func:`add_prov_column`.

Functions in this module should be safe to call repeatedly from a deploy hook
without accumulating side effects.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, Union

from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import _Schema
    from .table import Table

    TargetType = Union["_Schema", type["Table"], "Table"]


def set_replica_identity(
    target: "TargetType",
    mode: Literal["default", "full"] = "full",
    dry_run: bool = True,
) -> dict:
    """
    Apply ``ALTER TABLE ... REPLICA IDENTITY <mode>`` to a schema or table on PostgreSQL.

    ``REPLICA IDENTITY`` controls how much of the **old row** PostgreSQL writes to
    the write-ahead log on UPDATE/DELETE. Under ``DEFAULT``, only primary-key
    columns appear in WAL; under ``FULL``, the entire old row does.

    Why this exists
    ---------------
    Some change-data-capture (CDC) consumers require the full row pre-image to
    drive their downstream models. The canonical example is **Databricks
    Lakehouse Sync**: tables without ``REPLICA IDENTITY FULL`` are silently
    skipped by the sync — no error, just missing data downstream. Other CDC
    tools (Debezium, ClickHouse ClickPipes, Azure CDC) work fine with
    ``DEFAULT`` when tables have a primary key; only Databricks mandates
    ``FULL``.

    This helper is the **operational** way to apply the setting. It is not a
    migration: there's no legacy state being fixed; the setting is simply a
    property of the deployment environment, and a fresh declare in a new
    environment may need it re-applied. It is idempotent — re-applying the
    same mode is a no-op at the storage layer — so it is safe to call from a
    deploy hook on every release.

    Cost
    ----
    The ALTER itself is metadata-only and instant, but requires a brief
    ``AccessExclusiveLock`` on each table — it will block behind in-flight
    writes/reads on a busy table. Run during a quiet window on actively-
    ingested tables.

    The ongoing cost is in WAL volume after the change: UPDATE/DELETE on
    tables with FULL log the entire old row, which can be sizable on tables
    with TOASTed bytea columns. For DataJoint's typical insert-append
    workload, this cost is negligible. The notable scenario is bulk
    ``delete()`` on tables with ``<blob>`` columns — a transient WAL burst
    proportional to the deleted-row payload size.

    Partial-failure semantics
    -------------------------
    If ``connection.query(ddl)`` raises on table N of M, the first N-1
    tables are already modified at the storage layer but the exception
    propagates without returning the partial summary. The operation is
    idempotent, so re-running brings the remaining tables into compliance.

    Compliance considerations
    -------------------------
    Under ``DEFAULT``, only primary-key values appear in WAL. Under ``FULL``,
    entire rows do — including any PHI/PII/sensitive columns. For self-hosted
    PostgreSQL with unrestricted WAL access this is a real consideration; for
    managed PostgreSQL with logical replication confined to a specific
    subscriber (Lakebase, RDS), WAL stays inside the managed environment's
    security boundary. Apply intentionally.

    Parameters
    ----------
    target : Schema or Table
        A :class:`datajoint.Schema` (all user tables) or a
        :class:`datajoint.Table` class/instance (just that table).
    mode : str, default ``"full"``
        ``"default"`` (PK only, minimal WAL) or ``"full"`` (entire row).
    dry_run : bool, default ``True``
        If True, collect the DDL statements but do not execute. Set to False
        to actually apply.

    Returns
    -------
    dict
        - ``tables_analyzed`` (int): number of tables considered.
        - ``tables_modified`` (int): number of tables on which the ALTER ran.
          Always 0 when ``dry_run=True``.
        - ``ddl`` (list[str]): the DDL statements that were (or would be) executed.

    Raises
    ------
    DataJointError
        If the target's backend is not PostgreSQL, or if ``mode`` is not one of
        ``"default"`` / ``"full"``.

    Examples
    --------
    >>> from datajoint.deploy import set_replica_identity
    >>> # Preview
    >>> set_replica_identity(my_schema, mode="full", dry_run=True)
    {'tables_analyzed': 12, 'tables_modified': 0, 'ddl': ['ALTER TABLE "ms"."t1" REPLICA IDENTITY FULL', ...]}
    >>> # Apply
    >>> set_replica_identity(my_schema, mode="full", dry_run=False)
    {'tables_analyzed': 12, 'tables_modified': 12, 'ddl': [...]}
    >>> # Single table
    >>> set_replica_identity(MyTable, mode="full", dry_run=False)

    See Also
    --------
    PostgreSQL: `Logical Replication — Replica Identity
    <https://www.postgresql.org/docs/current/logical-replication-publication.html>`_.
    Databricks: `Lakehouse Sync
    <https://docs.databricks.com/aws/en/oltp/projects/lakehouse-sync>`_.
    """
    mode_normalized = mode.lower() if isinstance(mode, str) else mode
    if mode_normalized not in ("default", "full"):
        raise DataJointError(f"mode must be 'default' or 'full'; got {mode!r}")
    mode = mode_normalized  # type: ignore[assignment]

    from .schemas import _Schema
    from .table import Table

    if isinstance(target, _Schema):
        connection = target.connection
        if connection is None:
            raise DataJointError("Schema has no active connection.")
        adapter = connection.adapter
        if target.database is None:
            raise DataJointError("Schema is not activated. Call schema.activate(...) before set_replica_identity().")
        tables = [adapter.make_full_table_name(target.database, t) for t in target.list_tables()]
    elif isinstance(target, type) and issubclass(target, Table):
        instance = target()
        connection = instance.connection
        if connection is None:
            raise DataJointError(f"Table {target.__name__} has no active connection.")
        adapter = connection.adapter
        tables = [instance.full_table_name]
    elif isinstance(target, Table):
        connection = target.connection
        if connection is None:
            raise DataJointError(f"Table {type(target).__name__} has no active connection.")
        adapter = connection.adapter
        tables = [target.full_table_name]
    else:
        raise DataJointError(f"target must be a Schema or Table class/instance; got {type(target).__name__}")

    if not hasattr(adapter, "replica_identity_ddl"):
        raise DataJointError(
            f"set_replica_identity is PostgreSQL-only; the {adapter.backend} adapter does not support REPLICA IDENTITY."
        )

    result: dict[str, Any] = {
        "tables_analyzed": len(tables),
        "tables_modified": 0,
        "ddl": [],
    }
    for full_name in tables:
        ddl = adapter.replica_identity_ddl(full_name, mode)  # type: ignore[attr-defined]
        result["ddl"].append(ddl)
        if not dry_run:
            connection.query(ddl)
            result["tables_modified"] += 1
    return result


def add_prov_column(target: "TargetType", dry_run: bool = True) -> dict:
    """
    Add the hidden ``_prov`` attribute to Entry (``dj.Manual``) tables that lack it.

    Capture defaults off, so a table carries the slot only if it was declared
    while ``config.provenance.capture`` was on.  Every other table -- declared
    before 2.3.4, or before the deployment enabled capture -- records nothing on
    insert, silently, and this brings it in line.

    It belongs here rather than in :mod:`datajoint.migrate` because it is not a
    one-shot correction of legacy state.  It is idempotent — a table that already
    has the column is reported and left alone — and it stays useful for as long
    as capture can be turned off, which outlives the migration module.

    Parameters
    ----------
    target : Schema, Table class, or Table instance
        Given a Schema, every Entry table in it is processed.
    dry_run : bool, optional
        If True, report what would change without altering anything. Default True.

    Returns
    -------
    dict
        ``tables_analyzed``, ``tables_modified``, ``columns_added``, ``ddl``,
        and ``details`` — a per-table list of dicts.

    Examples
    --------
    >>> from datajoint.deploy import add_prov_column
    >>> add_prov_column(schema, dry_run=True)["ddl"]
    >>> add_prov_column(schema, dry_run=False)["tables_modified"]

    Notes
    -----
    - Only Entry tables are touched.  Computed and Imported tables have no use
      for the slot — their provenance is entailed by the foreign-key graph — and
      a part table inherits its master's.
    - Rows already present keep ``NULL``.  Provenance is recorded at insert and
      is never reconstructed after the fact.
    - **Takes effect on the next schema load.**  A ``Heading`` is read from the
      database once and memoized, so a table already in use keeps its pre-ALTER
      attributes for the life of the process and its inserts go on recording
      nothing.  This is a deploy-time operation: run it before the workers that
      will write through it, as with :func:`set_replica_identity`.
    """

    from . import provenance
    from .declare import PROV_DEFINITION, compile_attribute
    from .schemas import _Schema
    from .table import Table
    from .user_tables import Manual, is_tier

    if isinstance(target, _Schema):
        connection = target.connection
        if connection is None or not target.database:
            raise DataJointError("Schema is not activated. Call schema.activate(...) before add_prov_column().")
        database = target.database
        table_names = list(target.list_tables())
    elif isinstance(target, type) and issubclass(target, Table):
        instance = target()
        connection = instance.connection
        if connection is None:
            raise DataJointError(f"Table {target.__name__} has no active connection.")
        database, table_names = instance.database, [instance.table_name]
    elif isinstance(target, Table):
        connection = target.connection
        if connection is None:
            raise DataJointError(f"Table {type(target).__name__} has no active connection.")
        database, table_names = target.database, [target.table_name]
    else:
        raise DataJointError(f"target must be a Schema or Table class/instance; got {type(target).__name__}")

    if not database:
        raise DataJointError("Cannot add the provenance column: the target has no database.")

    adapter = connection.adapter
    # Compiled by the same path that declares it on a new table, so a retrofitted
    # column is identical to a freshly declared one.
    _name, column_sql, _store, prov_comment = compile_attribute(
        PROV_DEFINITION, in_key=False, foreign_key_sql=[], context={}, adapter=adapter
    )

    # `tables_modified` and `columns_added` count work actually done, as
    # set_replica_identity does; a dry run reports through `ddl` and `details`.
    result: dict[str, Any] = {
        "tables_analyzed": 0,
        "tables_modified": 0,
        "columns_added": 0,
        "ddl": [],
        "details": [],
    }

    for table_name in table_names:
        if not is_tier(table_name, Manual):
            continue
        result["tables_analyzed"] += 1

        existing = {
            row[0]
            for row in connection.query(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                args=(database, table_name),
            ).fetchall()
        }
        if provenance.PROV_ATTRIBUTE in existing:
            result["details"].append({"table": f"{database}.{table_name}", "status": "already_present"})
            continue

        ddl = (
            f"ALTER TABLE {adapter.quote_identifier(database)}.{adapter.quote_identifier(table_name)} ADD COLUMN {column_sql}"
        )
        statements = [ddl]
        # On PostgreSQL the comment is not part of the column definition; without
        # this the retrofitted column loses the `:type:` marker that a freshly
        # declared one carries, and reads back with no original_type.
        comment_ddl = adapter.column_comment_ddl(
            f"{adapter.quote_identifier(database)}.{adapter.quote_identifier(table_name)}",
            provenance.PROV_ATTRIBUTE,
            prov_comment,
        )
        if comment_ddl:
            statements.append(comment_ddl)

        result["ddl"].extend(statements)
        result["details"].append({"table": f"{database}.{table_name}", "status": "pending" if dry_run else "added"})
        if not dry_run:
            for statement in statements:
                connection.query(statement)
            result["tables_modified"] += 1
            result["columns_added"] += 1

    return result
