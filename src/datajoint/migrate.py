"""
Migration utilities for DataJoint schema updates.

This module provides tools for migrating existing schemas to use the new
AttributeType system, particularly for upgrading blob columns to use
explicit `<djblob>` type declarations.
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
    Analyze a schema to find blob columns that could be migrated to <djblob>.

    This function identifies blob columns that:
    1. Have a MySQL blob type (tinyblob, blob, mediumblob, longblob)
    2. Do NOT already have an adapter/type specified in their comment

    All blob size variants are included in the analysis.

    Args:
        schema: The DataJoint schema to analyze.

    Returns:
        List of dicts with keys:
            - table_name: Full table name (database.table)
            - column_name: Name of the blob column
            - column_type: MySQL column type (tinyblob, blob, mediumblob, longblob)
            - current_comment: Current column comment
            - needs_migration: True if column should be migrated

    Example:
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
            # Check if comment already has an adapter type (starts with :type:)
            has_adapter = comment and comment.startswith(":")

            results.append(
                {
                    "table_name": f"{schema.database}.{table_name}",
                    "column_name": column_name,
                    "column_type": column_type,
                    "current_comment": comment or "",
                    "needs_migration": not has_adapter,
                }
            )

    return results


def generate_migration_sql(
    schema: Schema,
    target_type: str = "djblob",
    dry_run: bool = True,
) -> list[str]:
    """
    Generate SQL statements to migrate blob columns to use <djblob>.

    This generates ALTER TABLE statements that update column comments to
    include the `:<djblob>:` prefix, marking them as using explicit
    DataJoint blob serialization.

    Args:
        schema: The DataJoint schema to migrate.
        target_type: The type name to migrate to (default: "djblob").
        dry_run: If True, only return SQL without executing.

    Returns:
        List of SQL ALTER TABLE statements.

    Example:
        >>> sql_statements = dj.migrate.generate_migration_sql(schema)
        >>> for sql in sql_statements:
        ...     print(sql)

    Note:
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
    target_type: str = "djblob",
    dry_run: bool = True,
) -> dict:
    """
    Migrate blob columns in a schema to use explicit <djblob> type.

    This updates column comments in the database to include the type
    declaration. The data format remains unchanged.

    Args:
        schema: The DataJoint schema to migrate.
        target_type: The type name to migrate to (default: "djblob").
        dry_run: If True, only preview changes without applying.

    Returns:
        Dict with keys:
            - analyzed: Number of blob columns analyzed
            - needs_migration: Number of columns that need migration
            - migrated: Number of columns migrated (0 if dry_run)
            - sql_statements: List of SQL statements (executed or to be executed)

    Example:
        >>> # Preview migration
        >>> result = dj.migrate.migrate_blob_columns(schema, dry_run=True)
        >>> print(f"Would migrate {result['needs_migration']} columns")

        >>> # Apply migration
        >>> result = dj.migrate.migrate_blob_columns(schema, dry_run=False)
        >>> print(f"Migrated {result['migrated']} columns")

    Warning:
        After migration, table definitions should be updated to use
        `<djblob>` instead of `longblob` for consistency. The migration
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

    Args:
        schema: The DataJoint schema to check.

    Returns:
        Dict with keys:
            - total_blob_columns: Total number of blob columns
            - migrated: Number of columns with explicit type
            - pending: Number of columns using implicit serialization
            - columns: List of column details

    Example:
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
