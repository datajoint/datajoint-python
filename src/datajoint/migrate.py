"""
Migration utilities for DataJoint schema updates.

This module provides tools for migrating existing schemas to use the new
Codec system, particularly for upgrading blob columns to use
explicit `<blob>` type declarations.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import Schema

logger = logging.getLogger(__name__.split(".")[0])

# Pattern to detect blob types
BLOB_TYPES = re.compile(r"^(tiny|small|medium|long|)blob$", re.I)


def analyze_blob_columns(schema: Schema) -> list[dict]:
    """
    Analyze a schema to find blob columns that could be migrated to <blob>.

    This function identifies blob columns that:

    1. Have a MySQL blob type (tinyblob, blob, mediumblob, longblob)
    2. Do NOT already have a codec/type specified in their comment

    All blob size variants are included in the analysis.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to analyze.

    Returns
    -------
    list[dict]
        List of dicts with keys:

        - table_name: Full table name (database.table)
        - column_name: Name of the blob column
        - column_type: MySQL column type (tinyblob, blob, mediumblob, longblob)
        - current_comment: Current column comment
        - needs_migration: True if column should be migrated

    Examples
    --------
    >>> import datajoint as dj
    >>> schema = dj.schema('my_database')
    >>> columns = dj.migrate.analyze_blob_columns(schema)
    >>> for col in columns:
    ...     if col['needs_migration']:
    ...         print(f"{col['table_name']}.{col['column_name']} ({col['column_type']})")
    """
    results = []

    connection = schema.connection

    # Get all tables in the schema
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE '~%%'
    """

    tables = connection.query(tables_query, args=(schema.database,)).fetchall()

    for (table_name,) in tables:
        # Get column information for each table
        columns_query = """
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND DATA_TYPE IN ('tinyblob', 'blob', 'mediumblob', 'longblob')
        """

        columns = connection.query(columns_query, args=(schema.database, table_name)).fetchall()

        for column_name, column_type, comment in columns:
            # Check if comment already has a codec type (starts with :type:)
            has_codec = comment and comment.startswith(":")

            results.append(
                {
                    "table_name": f"{schema.database}.{table_name}",
                    "column_name": column_name,
                    "column_type": column_type,
                    "current_comment": comment or "",
                    "needs_migration": not has_codec,
                }
            )

    return results


def generate_migration_sql(
    schema: Schema,
    target_type: str = "blob",
    dry_run: bool = True,
) -> list[str]:
    """
    Generate SQL statements to migrate blob columns to use <blob>.

    This generates ALTER TABLE statements that update column comments to
    include the `:<blob>:` prefix, marking them as using explicit
    DataJoint blob serialization.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to migrate.
    target_type : str, optional
        The type name to migrate to. Default "blob".
    dry_run : bool, optional
        If True, only return SQL without executing.

    Returns
    -------
    list[str]
        List of SQL ALTER TABLE statements.

    Examples
    --------
    >>> sql_statements = dj.migrate.generate_migration_sql(schema)
    >>> for sql in sql_statements:
    ...     print(sql)

    Notes
    -----
    This is a metadata-only migration. The actual blob data format
    remains unchanged - only the column comments are updated to
    indicate explicit type handling.
    """
    columns = analyze_blob_columns(schema)
    sql_statements = []

    for col in columns:
        if not col["needs_migration"]:
            continue

        # Build new comment with type prefix
        old_comment = col["current_comment"]
        new_comment = f":<{target_type}>:{old_comment}"

        # Escape special characters for SQL
        new_comment_escaped = new_comment.replace("\\", "\\\\").replace("'", "\\'")

        # Parse table name
        db_name, table_name = col["table_name"].split(".")

        # Generate ALTER TABLE statement
        sql = (
            f"ALTER TABLE `{db_name}`.`{table_name}` "
            f"MODIFY COLUMN `{col['column_name']}` {col['column_type']} "
            f"COMMENT '{new_comment_escaped}'"
        )
        sql_statements.append(sql)

    return sql_statements


def migrate_blob_columns(
    schema: Schema,
    target_type: str = "blob",
    dry_run: bool = True,
) -> dict:
    """
    Migrate blob columns in a schema to use explicit <blob> type.

    This updates column comments in the database to include the type
    declaration. The data format remains unchanged.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to migrate.
    target_type : str, optional
        The type name to migrate to. Default "blob".
    dry_run : bool, optional
        If True, only preview changes without applying. Default True.

    Returns
    -------
    dict
        Dict with keys:

        - analyzed: Number of blob columns analyzed
        - needs_migration: Number of columns that need migration
        - migrated: Number of columns migrated (0 if dry_run)
        - sql_statements: List of SQL statements (executed or to be executed)

    Examples
    --------
    >>> # Preview migration
    >>> result = dj.migrate.migrate_blob_columns(schema, dry_run=True)
    >>> print(f"Would migrate {result['needs_migration']} columns")

    >>> # Apply migration
    >>> result = dj.migrate.migrate_blob_columns(schema, dry_run=False)
    >>> print(f"Migrated {result['migrated']} columns")

    Warnings
    --------
    After migration, table definitions should be updated to use
    ``<blob>`` instead of ``longblob`` for consistency. The migration
    only updates database metadata; source code changes are manual.
    """
    columns = analyze_blob_columns(schema)
    sql_statements = generate_migration_sql(schema, target_type=target_type)

    result = {
        "analyzed": len(columns),
        "needs_migration": sum(1 for c in columns if c["needs_migration"]),
        "migrated": 0,
        "sql_statements": sql_statements,
    }

    if dry_run:
        logger.info(f"Dry run: would migrate {result['needs_migration']} columns")
        for sql in sql_statements:
            logger.info(f"  {sql}")
        return result

    # Execute migrations
    connection = schema.connection
    for sql in sql_statements:
        try:
            connection.query(sql)
            result["migrated"] += 1
            logger.info(f"Executed: {sql}")
        except Exception as e:
            logger.error(f"Failed to execute: {sql}\nError: {e}")
            raise DataJointError(f"Migration failed: {e}") from e

    logger.info(f"Successfully migrated {result['migrated']} columns")
    return result


def check_migration_status(schema: Schema) -> dict:
    """
    Check the migration status of blob columns in a schema.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to check.

    Returns
    -------
    dict
        Dict with keys:

        - total_blob_columns: Total number of blob columns
        - migrated: Number of columns with explicit type
        - pending: Number of columns using implicit serialization
        - columns: List of column details

    Examples
    --------
    >>> status = dj.migrate.check_migration_status(schema)
    >>> print(f"Migration progress: {status['migrated']}/{status['total_blob_columns']}")
    """
    columns = analyze_blob_columns(schema)

    return {
        "total_blob_columns": len(columns),
        "migrated": sum(1 for c in columns if not c["needs_migration"]),
        "pending": sum(1 for c in columns if c["needs_migration"]),
        "columns": columns,
    }


# =============================================================================
# Job Metadata Migration
# =============================================================================

# Hidden job metadata columns added by config.jobs.add_job_metadata
JOB_METADATA_COLUMNS = [
    ("_job_start_time", "datetime(3) DEFAULT NULL"),
    ("_job_duration", "float DEFAULT NULL"),
    ("_job_version", "varchar(64) DEFAULT ''"),
]


def _get_existing_columns(connection, database: str, table_name: str) -> set[str]:
    """Get set of existing column names for a table."""
    result = connection.query(
        """
        SELECT COLUMN_NAME
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        args=(database, table_name),
    )
    return {row[0] for row in result.fetchall()}


def _is_autopopulated_table(table_name: str) -> bool:
    """Check if a table name indicates a Computed or Imported table."""
    # Computed tables start with __ (but not part tables which have __ in middle)
    # Imported tables start with _ (but not __)
    if table_name.startswith("__"):
        # Computed table if no __ after the prefix
        return "__" not in table_name[2:]
    elif table_name.startswith("_"):
        # Imported table
        return True
    return False


def add_job_metadata_columns(target, dry_run: bool = True) -> dict:
    """
    Add hidden job metadata columns to existing Computed/Imported tables.

    This migration utility adds the hidden columns (_job_start_time, _job_duration,
    _job_version) to tables that were created before config.jobs.add_job_metadata
    was enabled.

    Parameters
    ----------
    target : Table or Schema
        Either a table class/instance (dj.Computed or dj.Imported) or
        a Schema object. If a Schema, all Computed/Imported tables in
        the schema will be processed.
    dry_run : bool, optional
        If True, only preview changes without applying. Default True.

    Returns
    -------
    dict
        Dict with keys:

        - tables_analyzed: Number of tables checked
        - tables_modified: Number of tables that were/would be modified
        - columns_added: Total columns added across all tables
        - details: List of dicts with per-table information

    Examples
    --------
    >>> import datajoint as dj
    >>> from datajoint.migrate import add_job_metadata_columns
    >>>
    >>> # Preview migration for a single table
    >>> result = add_job_metadata_columns(MyComputedTable, dry_run=True)
    >>> print(f"Would add {result['columns_added']} columns")
    >>>
    >>> # Apply migration to all tables in a schema
    >>> result = add_job_metadata_columns(schema, dry_run=False)
    >>> print(f"Modified {result['tables_modified']} tables")

    Notes
    -----
    - Only Computed and Imported tables are modified (not Manual, Lookup, or Part)
    - Existing rows will have NULL values for _job_start_time and _job_duration
    - Future populate() calls will fill in metadata for new rows
    - This does NOT retroactively populate metadata for existing rows
    """
    from .schemas import Schema
    from .table import Table

    result = {
        "tables_analyzed": 0,
        "tables_modified": 0,
        "columns_added": 0,
        "details": [],
    }

    # Determine tables to process
    if isinstance(target, Schema):
        schema = target
        # Get all user tables in the schema
        tables_query = """
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_TYPE = 'BASE TABLE'
            AND TABLE_NAME NOT LIKE '~%%'
        """
        table_names = [row[0] for row in schema.connection.query(tables_query, args=(schema.database,)).fetchall()]
        tables_to_process = [
            (schema.database, name, schema.connection) for name in table_names if _is_autopopulated_table(name)
        ]
    elif isinstance(target, type) and issubclass(target, Table):
        # Table class
        instance = target()
        tables_to_process = [(instance.database, instance.table_name, instance.connection)]
    elif isinstance(target, Table):
        # Table instance
        tables_to_process = [(target.database, target.table_name, target.connection)]
    else:
        raise DataJointError(f"target must be a Table class, Table instance, or Schema, got {type(target)}")

    for database, table_name, connection in tables_to_process:
        result["tables_analyzed"] += 1

        # Skip non-autopopulated tables
        if not _is_autopopulated_table(table_name):
            continue

        # Check which columns need to be added
        existing_columns = _get_existing_columns(connection, database, table_name)
        columns_to_add = [(name, definition) for name, definition in JOB_METADATA_COLUMNS if name not in existing_columns]

        if not columns_to_add:
            result["details"].append(
                {
                    "table": f"{database}.{table_name}",
                    "status": "already_migrated",
                    "columns_added": 0,
                }
            )
            continue

        # Generate and optionally execute ALTER statements
        table_detail = {
            "table": f"{database}.{table_name}",
            "status": "migrated" if not dry_run else "pending",
            "columns_added": len(columns_to_add),
            "sql_statements": [],
        }

        for col_name, col_definition in columns_to_add:
            sql = f"ALTER TABLE `{database}`.`{table_name}` ADD COLUMN `{col_name}` {col_definition}"
            table_detail["sql_statements"].append(sql)

            if not dry_run:
                try:
                    connection.query(sql)
                    logger.info(f"Added column {col_name} to {database}.{table_name}")
                except Exception as e:
                    logger.error(f"Failed to add column {col_name} to {database}.{table_name}: {e}")
                    table_detail["status"] = "error"
                    table_detail["error"] = str(e)
                    raise DataJointError(f"Migration failed: {e}") from e
            else:
                logger.info(f"Would add column {col_name} to {database}.{table_name}")

        result["tables_modified"] += 1
        result["columns_added"] += len(columns_to_add)
        result["details"].append(table_detail)

    return result
