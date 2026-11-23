"""
Migration utilities for DataJoint schema updates.

This module provides tools for migrating existing schemas to use the new
Codec system, particularly for upgrading blob columns to use
explicit `<blob>` type declarations.

.. note::
    This module is provided temporarily to assist with migration from pre-2.0.
    It is scheduled for removal in DataJoint 2.4 or 2.5.
    Complete your migrations while on DataJoint 2.3 or earlier.

Note on Terminology
-------------------
This module uses "external storage" because that was the term in DataJoint 0.14.6.
In DataJoint 2.0 documentation, this is called "object storage" (general term)
or "in-store storage" (specific to the @ modifier).
"""

from __future__ import annotations

import base64
import json
import logging
import re
import warnings
from collections.abc import Mapping
from typing import TYPE_CHECKING

from packaging.version import Version

from .errors import DataJointError
from .version import __version__

# Show deprecation warning starting in 2.1
if Version(__version__) >= Version("2.1"):
    warnings.warn(
        "datajoint.migrate is deprecated and is scheduled for removal in DataJoint 2.4 or 2.5. "
        "Complete your schema migrations while on DataJoint 2.3 or earlier.",
        DeprecationWarning,
        stacklevel=2,
    )

if TYPE_CHECKING:
    from .schemas import _Schema as Schema

logger = logging.getLogger(__name__.split(".")[0])

# Patterns for detecting 0.x external storage columns
# In 0.14.6, table definitions used: blob@store, attach@store, filepath@store
# These became column comments: :blob@store:, :attach@store:, :filepath@store:
EXTERNAL_PATTERNS = {
    "blob": re.compile(r":blob@([a-z][\-\w]*):", re.I),
    "attach": re.compile(r":attach@([a-z][\-\w]*):", re.I),
}

FILEPATH_PATTERN = re.compile(r":filepath@([a-z][\-\w]*):", re.I)

# Pattern to detect blob types
BLOB_TYPES = re.compile(r"^(tiny|small|medium|long|)blob$", re.I)


# =============================================================================
# Column Type Migration (Phase 2)
# =============================================================================

# Mapping from MySQL native types to DataJoint core types.
#
# Every value here must be a type that `declare.match_type` accepts: the values are
# written into column comments as `:<value>:` markers and read back by
# `heading.Heading._init_from_database`. DataJoint 2.0 provides no unsigned integer
# types, so unsigned columns widen to the next signed type that holds their full range.
# See `test_migrate_core_types_are_declarable` for the assertion that enforces this.
NATIVE_TO_CORE_TYPE = {
    # Unsigned integers widen to the next signed type. bigint unsigned is the one case
    # with no lossless target: values above 2**63-1 do not fit in int64.
    "tinyint unsigned": "int16",
    "smallint unsigned": "int32",
    "mediumint unsigned": "int32",
    "int unsigned": "int64",
    "bigint unsigned": "int64",
    # Signed integers
    "tinyint": "int8",
    "smallint": "int16",
    "mediumint": "int32",  # int24 is not a core type
    "int": "int32",
    "bigint": "int64",
    # Floats
    "float": "float32",
    "double": "float64",
    # Blobs (all map to <blob>)
    "tinyblob": "<blob>",
    "blob": "<blob>",
    "mediumblob": "<blob>",
    "longblob": "<blob>",
}


def analyze_columns(schema: Schema) -> dict:
    """
    Analyze a schema to find columns that need type labels in comments.

    This identifies columns that:

    1. Use native MySQL types that should be labeled with core types
    2. Are blob columns without codec markers
    3. Use external storage (requiring Phase 3-4 migration)

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to analyze.

    Returns
    -------
    dict
        Dict with keys:

        - needs_migration: list of columns needing type labels
        - already_migrated: list of columns with existing type labels
        - external_storage: list of columns requiring Phase 3-4

        Each column entry has: table, column, native_type, core_type, comment

    Examples
    --------
    >>> import datajoint as dj
    >>> from datajoint.migrate import analyze_columns
    >>> schema = dj.Schema('my_database')
    >>> result = analyze_columns(schema)
    >>> for col in result['needs_migration']:
    ...     print(f"{col['table']}.{col['column']}: {col['native_type']} → {col['core_type']}")
    """
    connection = schema.connection

    result = {
        "needs_migration": [],
        "already_migrated": [],
        "external_storage": [],
    }

    # Get all tables in the schema (excluding hidden tables)
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE '~%%'
    """
    tables = connection.query(tables_query, args=(schema.database,)).fetchall()

    for (table_name,) in tables:
        # Get all columns for this table
        columns_query = """
            SELECT COLUMN_NAME, COLUMN_TYPE, DATA_TYPE, COLUMN_COMMENT, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
        """
        columns = connection.query(columns_query, args=(schema.database, table_name)).fetchall()

        for column_name, column_type, data_type, comment, is_nullable in columns:
            comment = comment or ""

            # Check if column already has a type label (starts with :type:)
            has_label = comment.startswith(":")

            # Check for external storage patterns (requires Phase 3-4)
            is_external = bool(
                EXTERNAL_PATTERNS["blob"].search(comment)
                or EXTERNAL_PATTERNS["attach"].search(comment)
                or FILEPATH_PATTERN.search(comment)
            )

            col_info = {
                "table": f"{schema.database}.{table_name}",
                "column": column_name,
                "native_type": column_type,
                "comment": comment,
                "is_nullable": is_nullable == "YES",
            }

            if is_external:
                # External storage - needs Phase 3-4
                col_info["core_type"] = None
                col_info["reason"] = "external_storage"
                result["external_storage"].append(col_info)
            elif has_label:
                # Already has type label
                col_info["core_type"] = comment.split(":")[1] if ":" in comment else None
                result["already_migrated"].append(col_info)
            else:
                # Check if this type needs migration
                # Normalize column_type for lookup (remove size specifiers for some types)
                lookup_type = column_type.lower()

                # Handle blob types
                if BLOB_TYPES.match(data_type):
                    col_info["core_type"] = "<blob>"
                    result["needs_migration"].append(col_info)
                # Handle numeric types
                elif lookup_type in NATIVE_TO_CORE_TYPE:
                    col_info["core_type"] = NATIVE_TO_CORE_TYPE[lookup_type]
                    if lookup_type == "bigint unsigned":
                        logger.warning(
                            f"Column `{col_info['table']}`.`{col_info['column']}` is "
                            "`bigint unsigned` and will be labeled `int64`. DataJoint 2.0 "
                            "provides no unsigned integer types. The physical column and "
                            "stored data are unchanged by migration, but `describe()` will "
                            "report `int64`, so a describe->recreate->copy round trip "
                            "produces a signed column: values above 2**63-1 do not survive "
                            "that copy. Verify the stored range before migrating; if the "
                            "full unsigned range is in use, declare the attribute as "
                            "decimal(20,0), which holds it losslessly."
                        )
                    result["needs_migration"].append(col_info)
                # Types that don't need migration (varchar, date, datetime, json, etc.)
                # are silently skipped

    return result


def migrate_columns(
    schema: Schema,
    dry_run: bool = True,
) -> dict:
    """
    Add type labels to column comments for Phase 2 migration.

    This updates column comments to include type labels, enabling
    DataJoint 2.0 to recognize column types without relying on
    native MySQL types.

    Migrates:

    - Numeric types: int unsigned → :int64:, smallint → :int16:, etc.
    - Blob types: longblob → :<blob>:

    Does NOT migrate external storage columns (external-*, attach@*,
    filepath@*) - those require Phase 3-4.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to migrate.
    dry_run : bool, optional
        If True, only preview changes without applying. Default True.

    Returns
    -------
    dict
        Dict with keys:

        - columns_analyzed: total columns checked
        - columns_migrated: number of columns updated
        - columns_skipped: number already migrated or external
        - sql_statements: list of SQL executed (or to be executed)
        - details: per-column results

    Examples
    --------
    >>> from datajoint.migrate import migrate_columns
    >>> # Preview
    >>> result = migrate_columns(schema, dry_run=True)
    >>> print(f"Would migrate {len(result['sql_statements'])} columns")
    >>> # Apply
    >>> result = migrate_columns(schema, dry_run=False)
    >>> print(f"Migrated {result['columns_migrated']} columns")
    """
    analysis = analyze_columns(schema)
    connection = schema.connection

    result = {
        "columns_analyzed": (
            len(analysis["needs_migration"]) + len(analysis["already_migrated"]) + len(analysis["external_storage"])
        ),
        "columns_migrated": 0,
        "columns_skipped": len(analysis["already_migrated"]) + len(analysis["external_storage"]),
        "sql_statements": [],
        "details": [],
    }

    for col in analysis["needs_migration"]:
        # Parse table name
        db_name, table_name = col["table"].split(".")

        # Build new comment with type label
        old_comment = col["comment"]
        type_label = col["core_type"]
        new_comment = f":{type_label}:{old_comment}"

        # Escape for SQL
        new_comment_escaped = new_comment.replace("\\", "\\\\").replace("'", "\\'")

        # Generate ALTER TABLE statement
        not_null = "" if col["is_nullable"] else " NOT NULL"
        sql = (
            f"ALTER TABLE `{db_name}`.`{table_name}` "
            f"MODIFY COLUMN `{col['column']}` {col['native_type']}{not_null} "
            f"COMMENT '{new_comment_escaped}'"
        )
        result["sql_statements"].append(sql)

        detail = {
            "table": col["table"],
            "column": col["column"],
            "native_type": col["native_type"],
            "core_type": type_label,
            "status": "pending",
        }

        if dry_run:
            logger.info(f"Would migrate {col['table']}.{col['column']}: {col['native_type']} → {type_label}")
            detail["status"] = "dry_run"
        else:
            try:
                connection.query(sql)
                result["columns_migrated"] += 1
                detail["status"] = "migrated"
                logger.info(f"Migrated {col['table']}.{col['column']}: {col['native_type']} → {type_label}")
            except Exception as e:
                detail["status"] = "error"
                detail["error"] = str(e)
                logger.error(f"Failed to migrate {col['table']}.{col['column']}: {e}")
                raise DataJointError(f"Migration failed: {e}") from e

        result["details"].append(detail)

    if dry_run:
        logger.info(f"Dry run: would migrate {len(result['sql_statements'])} columns")
    else:
        logger.info(f"Migrated {result['columns_migrated']} columns")

    return result


# Legacy function name for backward compatibility
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
    >>> schema = dj.Schema('my_database')
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
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT, IS_NULLABLE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND DATA_TYPE IN ('tinyblob', 'blob', 'mediumblob', 'longblob')
        """

        columns = connection.query(columns_query, args=(schema.database, table_name)).fetchall()

        for column_name, column_type, comment, is_nullable in columns:
            # Check if comment already has a codec type (starts with :type:)
            has_codec = comment and comment.startswith(":")

            results.append(
                {
                    "table_name": f"{schema.database}.{table_name}",
                    "column_name": column_name,
                    "column_type": column_type,
                    "current_comment": comment or "",
                    "needs_migration": not has_codec,
                    "is_nullable": is_nullable == "YES",
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
        not_null = "" if col.get("is_nullable", True) else " NOT NULL"
        sql = (
            f"ALTER TABLE `{db_name}`.`{table_name}` "
            f"MODIFY COLUMN `{col['column_name']}` {col['column_type']}{not_null} "
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


def _job_metadata_ddl(adapter) -> list[tuple[str, str, str]]:
    """Compile the job-metadata columns the way a fresh declaration does.

    Returns ``(name, column_sql, comment)`` per column.  Going through
    ``compile_attribute`` rather than a hand-written string is what makes a
    retrofitted column identical to a declared one: the same backend type
    mapping -- ``datetime(3)`` on MySQL, ``timestamp(3)`` on PostgreSQL -- and
    the same ``:type:`` marker.
    """
    from .declare import JOB_METADATA_DEFINITION, compile_attribute

    return [
        (name, column_sql, comment)
        for name, column_sql, _store, comment in (
            compile_attribute(line, in_key=False, foreign_key_sql=[], context={}, adapter=adapter)
            for line in JOB_METADATA_DEFINITION
        )
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
    """Whether a table name denotes a Computed or Imported table."""
    from .user_tables import Computed, Imported, is_tier

    return is_tier(table_name, Computed) or is_tier(table_name, Imported)


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
    from .schemas import _Schema
    from .table import Table

    result = {
        "tables_analyzed": 0,
        "tables_modified": 0,
        "columns_added": 0,
        "details": [],
    }

    # Determine tables to process
    if isinstance(target, _Schema):
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
        adapter = connection.adapter
        existing_columns = _get_existing_columns(connection, database, table_name)
        columns_to_add = [column for column in _job_metadata_ddl(adapter) if column[0] not in existing_columns]

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

        qualified = f"{adapter.quote_identifier(database)}.{adapter.quote_identifier(table_name)}"
        for col_name, column_sql, comment in columns_to_add:
            statements = [f"ALTER TABLE {qualified} ADD COLUMN {column_sql}"]
            # PostgreSQL keeps comments out of the column definition, so without
            # this the retrofitted column reads back with no original_type.
            comment_ddl = adapter.column_comment_ddl(qualified, col_name, comment)
            if comment_ddl:
                statements.append(comment_ddl)
            table_detail["sql_statements"].extend(statements)

            if not dry_run:
                try:
                    for statement in statements:
                        connection.query(statement)
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


# =============================================================================
# External Storage Migration (Phase 6)
# =============================================================================


def _find_external_columns(schema: Schema) -> list[dict]:
    """
    Find columns using 0.x external storage format.

    Returns list of dicts with column info and detected store name.
    """
    connection = schema.connection
    results = []

    # Get all tables (excluding hidden tables)
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE '~%%'
    """
    tables = connection.query(tables_query, args=(schema.database,)).fetchall()

    for (table_name,) in tables:
        # Find BINARY(16) columns (0.x external storage format)
        columns_query = """
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND DATA_TYPE = 'binary'
            AND CHARACTER_MAXIMUM_LENGTH = 16
        """
        columns = connection.query(columns_query, args=(schema.database, table_name)).fetchall()

        for column_name, column_type, comment in columns:
            comment = comment or ""

            # Check for external blob pattern
            blob_match = EXTERNAL_PATTERNS["blob"].search(comment)
            if blob_match:
                store_name = blob_match.group(1) or "external"
                results.append(
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "column_type": column_type,
                        "comment": comment,
                        "store_name": store_name,
                        "external_type": "blob",
                    }
                )
                continue

            # Check for external attach pattern
            attach_match = EXTERNAL_PATTERNS["attach"].search(comment)
            if attach_match:
                store_name = attach_match.group(1) or "external"
                results.append(
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "column_type": column_type,
                        "comment": comment,
                        "store_name": store_name,
                        "external_type": "attach",
                    }
                )

    return results


def _find_filepath_columns(schema: Schema) -> list[dict]:
    """
    Find columns using 0.x filepath format.

    Returns list of dicts with column info and detected store name.
    """
    connection = schema.connection
    results = []

    # Get all tables (excluding hidden tables)
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE '~%%'
    """
    tables = connection.query(tables_query, args=(schema.database,)).fetchall()

    for (table_name,) in tables:
        # Find BINARY(16) columns with :filepath@store: in comment (0.x filepath format)
        columns_query = """
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND DATA_TYPE = 'binary'
            AND CHARACTER_MAXIMUM_LENGTH = 16
            AND COLUMN_COMMENT LIKE '%%:filepath%%'
        """
        columns = connection.query(columns_query, args=(schema.database, table_name)).fetchall()

        for column_name, column_type, comment in columns:
            comment = comment or ""
            match = FILEPATH_PATTERN.search(comment)
            if match:
                store_name = match.group(1) or "external"
                results.append(
                    {
                        "table_name": table_name,
                        "column_name": column_name,
                        "column_type": column_type,
                        "comment": comment,
                        "store_name": store_name,
                    }
                )

    return results


# In 0.14, each in-store column held a 16-byte MD5 digest in a BINARY(16) column,
# and the store's `~external_<store>` table tracked one row per digest:
#
# - a blob: the digest of its serialized bytes, stored at
#   `{database}/{subfolders}/{hex digest}`;
# - an attachment: the digest of its name, a NUL byte, and its contents, stored
#   (contents only) at `{database}/{subfolders}/{hex digest}.{attachment_name}`;
# - a filepath: the digest of its relative path, stored at `filepath`;
# - a blob migrated from 0.11: stored at `filepath`, with the digest of its
#   serialized bytes in `contents_hash`.
#
# Subfolders come from the store's `subfolding` setting, (2, 2) unless set.
LEGACY_SUBFOLDING = (2, 2)


def _legacy_subfolding(store_name: str, config=None) -> tuple[int, ...]:
    """
    Return the subfolding 0.14 used for a store.

    This is the store's own ``subfolding`` setting, or 0.14's default of (2, 2)
    when it has none. The raw setting is read because 2.x defaults to no
    subfolding. ``config`` defaults to the global settings.
    """
    if config is None:
        from .settings import config

    spec = config.stores.get(store_name)
    if isinstance(spec, Mapping) and "subfolding" in spec:
        return tuple(spec["subfolding"] or ())
    return LEGACY_SUBFOLDING


def _legacy_hash(digest: bytes) -> str:
    """
    Return the 2.x hash for a 0.14 content digest.

    Both versions take the MD5 digest of the same bytes. 2.x writes it in base32,
    as `hash_registry.compute_hash` does, so reads check migrated values as they
    check any other.
    """
    return base64.b32encode(digest).decode("ascii").rstrip("=").lower()


def _legacy_path(
    database: str,
    digest: bytes,
    subfolding: tuple[int, ...],
    attachment_name: str | None = None,
) -> str:
    """Return the store-relative path of a 0.14 blob or, given its name, attachment."""
    from .hash_registry import _subfold

    hex_digest = digest.hex()
    name = hex_digest if attachment_name is None else f"{hex_digest}.{attachment_name}"
    return "/".join((database, *_subfold(hex_digest, subfolding), name))


def _plan_legacy_references(
    connection,
    database: str,
    table_name: str,
    column_name: str,
    kind: str,
    store_name: str,
    dest_store: str | None = None,
) -> tuple[dict[bytes, dict], list[str]]:
    """
    Build the 2.x metadata for each 0.14 reference in one column.

    Parameters
    ----------
    connection : Connection
        Database connection.
    database, table_name, column_name : str
        The column holding 0.14 BINARY(16) references.
    kind : str
        ``"blob"``, ``"attach"``, or ``"filepath"``.
    store_name : str
        The 0.14 store; its references are tracked in ``~external_<store_name>``.
    dest_store : str, optional
        The 2.x store the metadata names. Defaults to ``store_name``. Files are
        looked up through this store, so it must point at the 0.14 location.

    Returns
    -------
    tuple[dict[bytes, dict], list[str]]
        The metadata for each referenced digest, and a description of each
        reference that cannot be converted: one with no tracking row, or whose
        file is not in the store.
    """
    from .hash_registry import get_store_backend

    dest_store = dest_store or store_name
    config = getattr(connection, "_config", None)  # the config reads resolve stores with
    backend = get_store_backend(dest_store, config=config)
    subfolding = _legacy_subfolding(dest_store, config)

    rows = connection.query(
        f"""
        SELECT DISTINCT t.`{column_name}`, e.hash, e.size, e.attachment_name,
               e.filepath, e.contents_hash, e.timestamp
        FROM `{database}`.`{table_name}` t
        LEFT JOIN `{database}`.`~external_{store_name}` e ON e.hash = t.`{column_name}`
        WHERE t.`{column_name}` IS NOT NULL
        """
    ).fetchall()

    metadata: dict[bytes, dict] = {}
    problems: list[str] = []
    for reference, tracked, size, attachment_name, filepath, contents_hash, timestamp in rows:
        reference = bytes(reference)
        label = f"{reference.hex()} in {database}.{table_name}.{column_name}"
        if tracked is None:
            problems.append(f"{label}: no row in ~external_{store_name}")
            continue

        if kind == "filepath":
            path, digest = filepath, None
        elif kind == "attach":
            path = None if attachment_name is None else _legacy_path(database, reference, subfolding, attachment_name)
            digest = reference
        elif filepath:  # a blob migrated from 0.11
            path, digest = filepath, contents_hash
        else:
            path, digest = _legacy_path(database, reference, subfolding), reference
        if path is None or (kind != "filepath" and digest is None):
            problems.append(f"{label}: the tracking row in ~external_{store_name} is incomplete")
            continue

        try:
            found = backend.exists(path)
        except DataJointError as error:
            problems.append(f"{label}: {error}")
            continue
        if not found:
            problems.append(f"{label}: no file at {path} in store '{dest_store}'")
            continue

        if kind == "filepath":
            metadata[reference] = {
                "path": path,
                "store": dest_store,
                "size": size,
                "is_dir": False,
                "timestamp": timestamp.isoformat() if timestamp else None,
            }
        else:
            metadata[reference] = {
                "hash": _legacy_hash(bytes(digest)),
                "path": path,
                "schema": database,
                "store": dest_store,
                "size": size,
            }
            if kind == "attach":
                metadata[reference]["attachment_name"] = attachment_name
    return metadata, problems


def _refuse_problems(where: str, problems: list[str]) -> None:
    """Raise if any reference cannot be converted. Called before anything is changed."""
    if problems:
        listed = "\n  ".join(problems[:10])
        more = f"\n  ... and {len(problems) - 10} more" if len(problems) > 10 else ""
        raise DataJointError(
            f"Cannot migrate {where}: {len(problems)} reference(s) cannot be converted. "
            f"Nothing was changed.\n  {listed}{more}"
        )


def _plan_columns(connection, database: str, columns: list[dict], dry_run: bool, kind: str | None = None) -> dict:
    """
    Plan the conversion of each column not yet migrated, keyed by (table, column).

    Every reference in every column is looked up before anything is changed, so
    that one missing file stops the migration with the schema unchanged. A dry
    run reports the problems instead of raising.
    """
    plans = {}
    for col in columns:
        table_name, column_name = col["table_name"], col["column_name"]
        exists = connection.query(
            """
            SELECT COUNT(*) FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
            """,
            args=(database, table_name, f"{column_name}_v2"),
        ).fetchone()[0]
        if not exists:
            plans[table_name, column_name] = _plan_legacy_references(
                connection, database, table_name, column_name, kind or col["external_type"], col["store_name"]
            )
    if not dry_run:
        _refuse_problems(database, [problem for _, problems in plans.values() for problem in problems])
    return plans


def _add_converted_column(
    connection,
    database: str,
    table_name: str,
    column_name: str,
    new_column: str,
    comment: str,
    metadata: dict[bytes, dict],
) -> int:
    """
    Add ``new_column`` as JSON and fill it with the metadata for each reference.

    If any reference is left unconverted, or any step fails, the new column is
    dropped again so that a later run starts over. Returns the number of rows
    converted.
    """
    table = f"`{database}`.`{table_name}`"
    connection.query(f"ALTER TABLE {table} ADD COLUMN `{new_column}` JSON COMMENT '{comment}'")
    try:
        with connection.transaction:
            for reference, value in metadata.items():
                connection.query(
                    f"UPDATE {table} SET `{new_column}` = %s WHERE `{column_name}` = %s",
                    args=(json.dumps(value), reference),
                )
        unconverted = connection.query(
            f"SELECT COUNT(*) FROM {table} WHERE `{column_name}` IS NOT NULL AND `{new_column}` IS NULL"
        ).fetchone()[0]
        if unconverted:
            raise DataJointError(f"{unconverted} row(s) of {database}.{table_name}.{column_name} were not converted")
    except Exception:
        connection.query(f"ALTER TABLE {table} DROP COLUMN `{new_column}`")
        raise
    return connection.query(f"SELECT COUNT(*) FROM {table} WHERE `{new_column}` IS NOT NULL").fetchone()[0]


def migrate_external(
    schema: Schema,
    dry_run: bool = True,
    finalize: bool = False,
) -> dict:
    """
    Migrate external storage columns from 0.x to 2.0 format.

    This migration uses a safe, multi-step approach:

    1. **Initial run** (dry_run=False): Adds new `<column>_v2` columns with JSON
       type and copies data from the old columns, converting UUID references to
       JSON metadata.

    2. **Verification**: You verify all data is accessible via DataJoint 2.0.

    3. **Finalize** (dry_run=False, finalize=True): Renames columns (old → `_v1`,
       new → original name).

    This allows 0.x and 2.0 to coexist during migration and provides a rollback
    path if issues are discovered.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to migrate.
    dry_run : bool, optional
        If True, only preview changes without applying. Default True.
    finalize : bool, optional
        If True, rename migrated columns to original names and drop old columns.
        Only run after verifying migration succeeded. Default False.

    Returns
    -------
    dict
        Migration results with keys:

        - columns_found: Number of external columns found
        - columns_migrated: Number of columns processed
        - rows_migrated: Number of rows with data converted
        - details: Per-column migration details

    Examples
    --------
    >>> from datajoint.migration import migrate_external
    >>>
    >>> # Step 1: Preview
    >>> result = migrate_external(schema, dry_run=True)
    >>> print(f"Found {result['columns_found']} columns to migrate")
    >>>
    >>> # Step 2: Run migration (adds new columns)
    >>> result = migrate_external(schema, dry_run=False)
    >>> print(f"Migrated {result['rows_migrated']} rows")
    >>>
    >>> # Step 3: Verify data is accessible via DataJoint 2.0
    >>> # ... manual verification ...
    >>>
    >>> # Step 4: Finalize (rename columns)
    >>> result = migrate_external(schema, dry_run=False, finalize=True)

    Notes
    -----
    The migration reads the hidden `~external_<store>` tables and finds each
    file where 0.14 stored it. Configure each store under its 0.14 name, with
    its 0.14 location and, if 0.14 set one, its `subfolding`.

    Every referenced file is looked up before anything is changed. If a
    reference has no tracking row or its file is missing, the migration raises
    and changes nothing; a dry run lists these references under `problems`.
    """
    columns = _find_external_columns(schema)
    connection = schema.connection
    database = schema.database

    result = {
        "columns_found": len(columns),
        "columns_migrated": 0,
        "rows_migrated": 0,
        "details": [],
    }

    if not columns:
        logger.info(f"No external columns found in {database}")
        return result

    plans = {} if finalize else _plan_columns(connection, database, columns, dry_run)

    for col in columns:
        table_name = col["table_name"]
        column_name = col["column_name"]
        store_name = col["store_name"]
        external_type = col["external_type"]
        old_comment = col["comment"]

        detail = {
            "table": f"{database}.{table_name}",
            "column": column_name,
            "store": store_name,
            "type": external_type,
            "status": "pending",
            "rows": 0,
        }

        # Build new comment: the 2.x codec marker, then the rest of the comment
        codec = "blob" if external_type == "blob" else "attach"
        new_comment = EXTERNAL_PATTERNS[external_type].sub("", old_comment).strip()
        new_comment = f":<{codec}@{store_name}>:{new_comment}"

        new_column = f"{column_name}_v2"

        if finalize:
            # Finalize: rename columns
            detail["action"] = "finalize"

            if dry_run:
                logger.info(
                    f"Would finalize {database}.{table_name}.{column_name}: "
                    f"rename {column_name} → {column_name}_v1, "
                    f"{new_column} → {column_name}"
                )
                detail["status"] = "dry_run"
            else:
                try:
                    # Rename old column to _v1
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"CHANGE COLUMN `{column_name}` `{column_name}_v1` "
                        f"{col['column_type']} COMMENT 'legacy 0.x'"
                    )
                    connection.query(sql)

                    # Rename new column to original name
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"CHANGE COLUMN `{new_column}` `{column_name}` "
                        f"JSON COMMENT '{new_comment}'"
                    )
                    connection.query(sql)

                    detail["status"] = "finalized"
                    result["columns_migrated"] += 1
                    logger.info(f"Finalized {database}.{table_name}.{column_name}")
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(f"Failed to finalize {table_name}.{column_name}: {e}")
                    raise DataJointError(f"Finalize failed: {e}") from e
        else:
            # Initial migration: add new column and copy data
            detail["action"] = "migrate"

            # Skip columns whose _v2 column already exists
            if (table_name, column_name) not in plans:
                detail["status"] = "already_migrated"
                logger.info(f"Column {new_column} already exists, skipping")
                result["details"].append(detail)
                continue
            metadata, problems = plans[table_name, column_name]

            if dry_run:
                # Count rows that would be migrated
                count_sql = f"""
                    SELECT COUNT(*) FROM `{database}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                """
                count = connection.query(count_sql).fetchone()[0]
                detail["rows"] = count
                detail["problems"] = problems
                detail["status"] = "dry_run"
                logger.info(f"Would migrate {database}.{table_name}.{column_name}: " f"{count} rows, store={store_name}")
                for problem in problems:
                    logger.warning(f"Cannot migrate {problem}")
            else:
                try:
                    count = _add_converted_column(
                        connection, database, table_name, column_name, new_column, new_comment, metadata
                    )
                    detail["rows"] = count
                    detail["status"] = "migrated"
                    result["columns_migrated"] += 1
                    result["rows_migrated"] += count

                    logger.info(f"Migrated {database}.{table_name}.{column_name}: " f"{count} rows")
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(f"Failed to migrate {table_name}.{column_name}: {e}")
                    raise DataJointError(f"Migration failed: {e}") from e

        result["details"].append(detail)

    return result


# =============================================================================
# Store Configuration and Integrity Checks
# =============================================================================


def check_store_configuration(schema: Schema) -> dict:
    """
    Verify external stores are properly configured.

    Checks that all external storage stores referenced in the schema's
    tables are configured in settings and accessible.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to check.

    Returns
    -------
    dict
        Dict with keys:

        - stores_configured: list of store names with valid config
        - stores_missing: list of stores referenced but not configured
        - stores_unreachable: list of stores that failed connection test
        - details: per-store details

    Examples
    --------
    >>> from datajoint.migrate import check_store_configuration
    >>> result = check_store_configuration(schema)
    >>> if result['stores_missing']:
    ...     print(f"Missing stores: {result['stores_missing']}")
    """
    from .settings import config
    import os

    result = {
        "stores_configured": [],
        "stores_missing": [],
        "stores_unreachable": [],
        "details": [],
    }

    # Find all external columns and their store names
    external_cols = _find_external_columns(schema)
    filepath_cols = _find_filepath_columns(schema)

    # Collect unique store names
    store_names = set()
    for col in external_cols + filepath_cols:
        store_names.add(col["store_name"])

    # Also check ~external_* tables for store names
    connection = schema.connection
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_NAME LIKE '~external_%%'
    """
    external_tables = connection.query(tables_query, args=(schema.database,)).fetchall()
    for (table_name,) in external_tables:
        # Extract store name from ~external_<store>
        store_name = table_name[10:]  # Remove "~external_" prefix
        if store_name:
            store_names.add(store_name)

    stores_config = config.get("stores", {})

    for store_name in store_names:
        detail = {
            "store": store_name,
            "status": "unknown",
            "location": None,
            "protocol": None,
        }

        if store_name not in stores_config:
            result["stores_missing"].append(store_name)
            detail["status"] = "missing"
            result["details"].append(detail)
            continue

        store_config = stores_config[store_name]
        detail["location"] = store_config.get("location")
        detail["protocol"] = store_config.get("protocol", "file")

        # Test accessibility
        protocol = detail["protocol"]
        location = detail["location"]

        if protocol == "file":
            # Check if local path exists
            if location and os.path.exists(location):
                result["stores_configured"].append(store_name)
                detail["status"] = "configured"
            else:
                result["stores_unreachable"].append(store_name)
                detail["status"] = "unreachable"
                detail["error"] = f"Path does not exist: {location}"
        elif protocol in ("s3", "minio"):
            # For S3/MinIO, we can't easily test without boto3
            # Mark as configured if it has required keys
            if location and store_config.get("access_key"):
                result["stores_configured"].append(store_name)
                detail["status"] = "configured"
            else:
                result["stores_missing"].append(store_name)
                detail["status"] = "incomplete"
                detail["error"] = "Missing location or access_key"
        else:
            # Unknown protocol, assume configured if location set
            if location:
                result["stores_configured"].append(store_name)
                detail["status"] = "configured"
            else:
                result["stores_missing"].append(store_name)
                detail["status"] = "incomplete"

        result["details"].append(detail)

    return result


def verify_external_integrity(schema: Schema, store_name: str = None) -> dict:
    """
    Check that all external references point to existing files.

    Verifies integrity of external storage by checking that each
    reference in the ~external_* tables points to an accessible file.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to check.
    store_name : str, optional
        Specific store to check. If None, checks all stores.

    Returns
    -------
    dict
        Dict with keys:

        - total_references: count of external entries
        - valid: count with accessible files
        - missing: list of entries with inaccessible files
        - stores_checked: list of store names checked

    Examples
    --------
    >>> from datajoint.migrate import verify_external_integrity
    >>> result = verify_external_integrity(schema)
    >>> if result['missing']:
    ...     print(f"Missing files: {len(result['missing'])}")
    ...     for entry in result['missing'][:5]:
    ...         print(f"  {entry['filepath']}")

    Notes
    -----
    For S3/MinIO stores, this function does not verify file existence
    (would require network calls). Only local file stores are fully verified.
    """
    from .settings import config
    import os

    result = {
        "total_references": 0,
        "valid": 0,
        "missing": [],
        "stores_checked": [],
    }

    connection = schema.connection
    stores_config = config.get("stores", {})

    # Find ~external_* tables
    if store_name:
        external_tables = [(f"~external_{store_name}",)]
    else:
        tables_query = """
            SELECT TABLE_NAME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME LIKE '~external_%%'
        """
        external_tables = connection.query(tables_query, args=(schema.database,)).fetchall()

    for (table_name,) in external_tables:
        # Extract store name
        current_store = table_name[10:]  # Remove "~external_" prefix
        result["stores_checked"].append(current_store)

        store_config = stores_config.get(current_store, {})
        protocol = store_config.get("protocol", "file")
        location = store_config.get("location", "")

        # Only verify local files
        if protocol != "file":
            logger.info(f"Skipping {current_store}: non-local protocol ({protocol})")
            continue

        # Query external table for all entries
        try:
            entries_query = f"""
                SELECT hash, filepath, size, attachment_name
                FROM `{schema.database}`.`{table_name}`
            """
            entries = connection.query(entries_query).fetchall()
        except Exception as e:
            logger.warning(f"Could not read {table_name}: {e}")
            continue

        subfolding = _legacy_subfolding(current_store, config)
        for digest, filepath, size, attachment_name in entries:
            result["total_references"] += 1

            # Filepaths (and blobs migrated from 0.11) record their path; blobs and
            # attachments are stored at a path derived from their hash.
            relative_path = filepath or _legacy_path(schema.database, bytes(digest), subfolding, attachment_name)
            if location:
                full_path = os.path.join(location, relative_path)
            else:
                full_path = relative_path

            if os.path.exists(full_path):
                result["valid"] += 1
            else:
                result["missing"].append(
                    {
                        "store": current_store,
                        "hash": bytes(digest).hex().upper(),
                        "filepath": relative_path,
                        "full_path": full_path,
                        "expected_size": size,
                    }
                )

    return result


def rebuild_lineage(schema: Schema, dry_run: bool = True) -> dict:
    """
    Rebuild ~lineage table from current table definitions.

    Use after schema changes or to repair corrupted lineage data.
    The lineage table tracks foreign key relationships for semantic matching.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to rebuild lineage for.
    dry_run : bool, optional
        If True, only preview changes without applying. Default True.

    Returns
    -------
    dict
        Dict with keys:

        - tables_analyzed: number of tables in schema
        - lineage_entries: number of lineage entries created
        - status: 'dry_run', 'rebuilt', or 'error'

    Examples
    --------
    >>> from datajoint.migrate import rebuild_lineage
    >>> result = rebuild_lineage(schema, dry_run=True)
    >>> print(f"Would create {result['lineage_entries']} lineage entries")
    >>> result = rebuild_lineage(schema, dry_run=False)
    >>> print(f"Rebuilt lineage: {result['status']}")

    Notes
    -----
    This function wraps schema.rebuild_lineage() with dry_run support
    and additional reporting.
    """
    result = {
        "tables_analyzed": 0,
        "lineage_entries": 0,
        "status": "pending",
    }

    connection = schema.connection

    # Count tables in schema
    tables_query = """
        SELECT COUNT(*)
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        AND TABLE_TYPE = 'BASE TABLE'
        AND TABLE_NAME NOT LIKE '~%%'
    """
    result["tables_analyzed"] = connection.query(tables_query, args=(schema.database,)).fetchone()[0]

    if dry_run:
        # Estimate lineage entries (count foreign key relationships)
        fk_query = """
            SELECT COUNT(*)
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
            AND REFERENCED_TABLE_NAME IS NOT NULL
        """
        result["lineage_entries"] = connection.query(fk_query, args=(schema.database,)).fetchone()[0]
        result["status"] = "dry_run"
        logger.info(
            f"Dry run: would rebuild lineage for {result['tables_analyzed']} tables "
            f"with ~{result['lineage_entries']} foreign key relationships"
        )
        return result

    try:
        # Call schema's rebuild_lineage method if available
        if hasattr(schema, "rebuild_lineage"):
            schema.rebuild_lineage()
        else:
            # Manual rebuild for older schemas
            logger.warning("schema.rebuild_lineage() not available, attempting manual rebuild")
            _rebuild_lineage_manual(schema)

        # Count actual lineage entries created
        lineage_query = f"""
            SELECT COUNT(*)
            FROM `{schema.database}`.`~lineage`
        """
        try:
            result["lineage_entries"] = connection.query(lineage_query).fetchone()[0]
        except Exception:
            result["lineage_entries"] = 0

        result["status"] = "rebuilt"
        logger.info(f"Rebuilt lineage: {result['lineage_entries']} entries")
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        logger.error(f"Failed to rebuild lineage: {e}")
        raise DataJointError(f"Lineage rebuild failed: {e}") from e

    return result


def _rebuild_lineage_manual(schema: Schema):
    """Manual lineage rebuild for schemas without rebuild_lineage method."""
    connection = schema.connection
    database = schema.database

    # Create lineage table if it doesn't exist
    create_sql = f"""
        CREATE TABLE IF NOT EXISTS `{database}`.`~lineage` (
            `child` varchar(64) NOT NULL,
            `parent` varchar(64) NOT NULL,
            `attribute` varchar(64) NOT NULL,
            PRIMARY KEY (`child`, `parent`, `attribute`)
        )
    """
    connection.query(create_sql)

    # Clear existing entries
    connection.query(f"DELETE FROM `{database}`.`~lineage`")

    # Populate from foreign key relationships
    insert_sql = f"""
        INSERT INTO `{database}`.`~lineage` (child, parent, attribute)
        SELECT DISTINCT
            TABLE_NAME as child,
            REFERENCED_TABLE_NAME as parent,
            COLUMN_NAME as attribute
        FROM information_schema.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = %s
        AND REFERENCED_TABLE_NAME IS NOT NULL
    """
    connection.query(insert_sql, args=(database,))


def migrate_filepath(
    schema: Schema,
    dry_run: bool = True,
    finalize: bool = False,
) -> dict:
    """
    Migrate filepath columns from 0.x to 2.0 format.

    Same multi-step approach as migrate_external:

    1. **Initial run** (dry_run=False): Adds new `<column>_v2` columns with JSON type
    2. **Verification**: Verify files accessible via DataJoint 2.0
    3. **Finalize** (dry_run=False, finalize=True): Rename columns (old → `_v1`)

    As with migrate_external, every referenced file is looked up first, and the
    migration raises and changes nothing if one is missing.

    Parameters
    ----------
    schema : Schema
        The DataJoint schema to migrate.
    dry_run : bool, optional
        If True, only preview changes. Default True.
    finalize : bool, optional
        If True, finalize migration. Default False.

    Returns
    -------
    dict
        Migration results (same format as migrate_external).

    Examples
    --------
    >>> from datajoint.migration import migrate_filepath
    >>>
    >>> # Preview
    >>> result = migrate_filepath(schema, dry_run=True)
    >>>
    >>> # Run migration
    >>> result = migrate_filepath(schema, dry_run=False)
    >>>
    >>> # Finalize after verification
    >>> result = migrate_filepath(schema, dry_run=False, finalize=True)
    """
    columns = _find_filepath_columns(schema)
    connection = schema.connection
    database = schema.database

    result = {
        "columns_found": len(columns),
        "columns_migrated": 0,
        "rows_migrated": 0,
        "details": [],
    }

    if not columns:
        logger.info(f"No filepath columns found in {database}")
        return result

    plans = {} if finalize else _plan_columns(connection, database, columns, dry_run, kind="filepath")

    for col in columns:
        table_name = col["table_name"]
        column_name = col["column_name"]
        store_name = col["store_name"]
        old_comment = col["comment"]

        detail = {
            "table": f"{database}.{table_name}",
            "column": column_name,
            "store": store_name,
            "status": "pending",
            "rows": 0,
        }

        # Build new comment: the 2.x codec marker, then the rest of the comment
        new_comment = FILEPATH_PATTERN.sub("", old_comment).strip()
        new_comment = f":<filepath@{store_name}>:{new_comment}"

        new_column = f"{column_name}_v2"

        if finalize:
            detail["action"] = "finalize"

            if dry_run:
                logger.info(f"Would finalize {database}.{table_name}.{column_name}")
                detail["status"] = "dry_run"
            else:
                try:
                    # Rename old column to _v1
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"CHANGE COLUMN `{column_name}` `{column_name}_v1` "
                        f"{col['column_type']} COMMENT 'legacy 0.x'"
                    )
                    connection.query(sql)

                    # Rename new column to original name
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"CHANGE COLUMN `{new_column}` `{column_name}` "
                        f"JSON COMMENT '{new_comment}'"
                    )
                    connection.query(sql)

                    detail["status"] = "finalized"
                    result["columns_migrated"] += 1
                    logger.info(f"Finalized {database}.{table_name}.{column_name}")
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(f"Failed to finalize: {e}")
                    raise DataJointError(f"Finalize failed: {e}") from e
        else:
            detail["action"] = "migrate"

            # Skip columns whose _v2 column already exists
            if (table_name, column_name) not in plans:
                detail["status"] = "already_migrated"
                result["details"].append(detail)
                continue
            metadata, problems = plans[table_name, column_name]

            if dry_run:
                count_sql = f"""
                    SELECT COUNT(*) FROM `{database}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                """
                count = connection.query(count_sql).fetchone()[0]
                detail["rows"] = count
                detail["problems"] = problems
                detail["status"] = "dry_run"
                logger.info(f"Would migrate {database}.{table_name}.{column_name}: " f"{count} rows")
                for problem in problems:
                    logger.warning(f"Cannot migrate {problem}")
            else:
                try:
                    count = _add_converted_column(
                        connection, database, table_name, column_name, new_column, new_comment, metadata
                    )
                    detail["rows"] = count
                    detail["status"] = "migrated"
                    result["columns_migrated"] += 1
                    result["rows_migrated"] += count

                    logger.info(f"Migrated {database}.{table_name}.{column_name}: " f"{count} rows")
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(f"Failed to migrate: {e}")
                    raise DataJointError(f"Migration failed: {e}") from e

        result["details"].append(detail)

    return result


# =============================================================================
# Parallel Schema Migration (0.14.6 → 2.0)
# =============================================================================


def create_parallel_schema(
    source: str,
    dest: str,
    copy_data: bool = False,
    connection=None,
) -> dict:
    """
    Create a parallel _v20 schema for migration testing.

    This creates a copy of a production schema (source) into a test schema (dest)
    for safely testing DataJoint 2.0 migration without affecting production.

    Parameters
    ----------
    source : str
        Production schema name (e.g., 'my_pipeline')
    dest : str
        Test schema name (e.g., 'my_pipeline_v20')
    copy_data : bool, optional
        If True, copy all table data. If False (default), create empty tables.
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - tables_created: int - number of tables created
        - data_copied: bool - whether data was copied
        - tables: list - list of table names created

    Examples
    --------
    >>> from datajoint.migrate import create_parallel_schema
    >>> result = create_parallel_schema('my_pipeline', 'my_pipeline_v20')
    >>> print(f"Created {result['tables_created']} tables")

    See Also
    --------
    copy_table_data : Copy data between schemas
    """
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    logger.info(f"Creating parallel schema: {source} → {dest}")

    # Create destination schema if not exists
    connection.query(f"CREATE DATABASE IF NOT EXISTS `{dest}`")

    # Get all tables from source schema
    tables_query = """
        SELECT TABLE_NAME
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s
        ORDER BY TABLE_NAME
    """
    tables = [row[0] for row in connection.query(tables_query, args=(source,)).fetchall()]

    result = {
        "tables_created": 0,
        "data_copied": copy_data,
        "tables": [],
    }

    for table in tables:
        # Get CREATE TABLE statement from source
        create_stmt = connection.query(f"SHOW CREATE TABLE `{source}`.`{table}`").fetchone()[1]

        # Replace schema name in CREATE statement
        create_stmt = create_stmt.replace(f"CREATE TABLE `{table}`", f"CREATE TABLE `{dest}`.`{table}`")

        # Create table in destination
        connection.query(create_stmt)

        result["tables_created"] += 1
        result["tables"].append(table)

        # Copy data if requested
        if copy_data:
            connection.query(f"INSERT INTO `{dest}`.`{table}` SELECT * FROM `{source}`.`{table}`")

        logger.info(f"Created {dest}.{table}")

    logger.info(f"Created {result['tables_created']} tables in {dest}")

    return result


def copy_table_data(
    source_schema: str,
    dest_schema: str,
    table: str,
    limit: int | None = None,
    where_clause: str | None = None,
    connection=None,
) -> dict:
    """
    Copy data from production table to test table.

    Parameters
    ----------
    source_schema : str
        Production schema name
    dest_schema : str
        Test schema name (_v20)
    table : str
        Table name
    limit : int, optional
        Maximum number of rows to copy
    where_clause : str, optional
        SQL WHERE clause for filtering (without 'WHERE' keyword)
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - rows_copied: int - number of rows copied
        - time_taken: float - seconds elapsed

    Examples
    --------
    >>> # Copy all data
    >>> result = copy_table_data('my_pipeline', 'my_pipeline_v20', 'Mouse')

    >>> # Copy sample
    >>> result = copy_table_data(
    ...     'my_pipeline', 'my_pipeline_v20', 'Session',
    ...     limit=100,
    ...     where_clause="session_date >= '2024-01-01'"
    ... )
    """
    import time
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    start_time = time.time()

    # Build query
    query = f"INSERT INTO `{dest_schema}`.`{table}` SELECT * FROM `{source_schema}`.`{table}`"

    if where_clause:
        query += f" WHERE {where_clause}"

    if limit:
        query += f" LIMIT {limit}"

    # Execute copy
    connection.query(query)

    # Get row count
    count_query = f"SELECT COUNT(*) FROM `{dest_schema}`.`{table}`"
    rows_copied = connection.query(count_query).fetchone()[0]

    time_taken = time.time() - start_time

    logger.info(f"Copied {rows_copied} rows from {source_schema}.{table} to {dest_schema}.{table} in {time_taken:.2f}s")

    return {
        "rows_copied": rows_copied,
        "time_taken": time_taken,
    }


def compare_query_results(
    prod_schema: str,
    test_schema: str,
    table: str,
    tolerance: float = 1e-6,
    connection=None,
) -> dict:
    """
    Compare query results between production and test schemas.

    Parameters
    ----------
    prod_schema : str
        Production schema name
    test_schema : str
        Test schema name (_v20)
    table : str
        Table name to compare
    tolerance : float, optional
        Tolerance for floating-point comparison. Default 1e-6.
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - match: bool - whether all rows match
        - row_count: int - number of rows compared
        - discrepancies: list - list of mismatches (if any)

    Examples
    --------
    >>> result = compare_query_results('my_pipeline', 'my_pipeline_v20', 'neuron')
    >>> if result['match']:
    ...     print(f"✓ All {result['row_count']} rows match")
    """
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    # Get row counts
    prod_count = connection.query(f"SELECT COUNT(*) FROM `{prod_schema}`.`{table}`").fetchone()[0]
    test_count = connection.query(f"SELECT COUNT(*) FROM `{test_schema}`.`{table}`").fetchone()[0]

    result = {
        "match": True,
        "row_count": prod_count,
        "discrepancies": [],
    }

    if prod_count != test_count:
        result["match"] = False
        result["discrepancies"].append(f"Row count mismatch: prod={prod_count}, test={test_count}")
        return result

    # Get column info
    columns_query = """
        SELECT COLUMN_NAME, DATA_TYPE
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        ORDER BY ORDINAL_POSITION
    """
    columns = connection.query(columns_query, args=(prod_schema, table)).fetchall()

    # Compare data row by row (for small tables) or checksums (for large tables)
    if prod_count <= 10000:
        # Row-by-row comparison for small tables
        prod_data = connection.query(f"SELECT * FROM `{prod_schema}`.`{table}` ORDER BY 1").fetchall()
        test_data = connection.query(f"SELECT * FROM `{test_schema}`.`{table}` ORDER BY 1").fetchall()

        for i, (prod_row, test_row) in enumerate(zip(prod_data, test_data)):
            for j, (col_name, col_type) in enumerate(columns):
                prod_val = prod_row[j]
                test_val = test_row[j]

                # Handle NULL
                if prod_val is None and test_val is None:
                    continue
                if prod_val is None or test_val is None:
                    result["match"] = False
                    result["discrepancies"].append(f"Row {i}, {col_name}: NULL mismatch")
                    continue

                # Handle floating-point comparison
                if col_type in ("float", "double", "decimal"):
                    if abs(float(prod_val) - float(test_val)) > tolerance:
                        result["match"] = False
                        result["discrepancies"].append(f"Row {i}, {col_name}: {prod_val} != {test_val} (diff > {tolerance})")
                else:
                    if prod_val != test_val:
                        result["match"] = False
                        result["discrepancies"].append(f"Row {i}, {col_name}: {prod_val} != {test_val}")
    else:
        # Checksum comparison for large tables
        checksum_query = f"CHECKSUM TABLE `{{schema}}`.`{table}`"
        prod_checksum = connection.query(checksum_query.format(schema=prod_schema)).fetchone()[1]
        test_checksum = connection.query(checksum_query.format(schema=test_schema)).fetchone()[1]

        if prod_checksum != test_checksum:
            result["match"] = False
            result["discrepancies"].append(f"Checksum mismatch: prod={prod_checksum}, test={test_checksum}")

    return result


def backup_schema(
    schema: str,
    backup_name: str,
    connection=None,
) -> dict:
    """
    Create full backup of a schema.

    Parameters
    ----------
    schema : str
        Schema name to backup
    backup_name : str
        Backup schema name (e.g., 'my_pipeline_backup_20250114')
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - tables_backed_up: int
        - rows_backed_up: int
        - backup_location: str

    Examples
    --------
    >>> result = backup_schema('my_pipeline', 'my_pipeline_backup_20250114')
    >>> print(f"Backed up {result['tables_backed_up']} tables")
    """
    result = create_parallel_schema(
        source=schema,
        dest=backup_name,
        copy_data=True,
        connection=connection,
    )

    # Count total rows
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    total_rows = 0
    for table in result["tables"]:
        count = connection.query(f"SELECT COUNT(*) FROM `{backup_name}`.`{table}`").fetchone()[0]
        total_rows += count

    return {
        "tables_backed_up": result["tables_created"],
        "rows_backed_up": total_rows,
        "backup_location": backup_name,
    }


def restore_schema(
    backup: str,
    dest: str,
    connection=None,
) -> dict:
    """
    Restore schema from backup.

    Parameters
    ----------
    backup : str
        Backup schema name
    dest : str
        Destination schema name
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - tables_restored: int
        - rows_restored: int

    Examples
    --------
    >>> restore_schema('my_pipeline_backup_20250114', 'my_pipeline')
    """
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    # Drop destination if exists
    connection.query(f"DROP DATABASE IF EXISTS `{dest}`")

    # Copy backup to destination
    result = create_parallel_schema(
        source=backup,
        dest=dest,
        copy_data=True,
        connection=connection,
    )

    # Count total rows
    total_rows = 0
    for table in result["tables"]:
        count = connection.query(f"SELECT COUNT(*) FROM `{dest}`.`{table}`").fetchone()[0]
        total_rows += count

    return {
        "tables_restored": result["tables_created"],
        "rows_restored": total_rows,
    }


def verify_schema_v20(
    schema: str,
    connection=None,
) -> dict:
    """
    Verify schema is fully migrated to DataJoint 2.0.

    Parameters
    ----------
    schema : str
        Schema name to verify
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - compatible: bool - True if fully compatible with 2.0
        - blob_markers: bool - All blob columns have :<blob>: markers
        - lineage_exists: bool - ~lineage table exists
        - issues: list - List of compatibility issues found

    Examples
    --------
    >>> result = verify_schema_v20('my_pipeline')
    >>> if result['compatible']:
    ...     print("✓ Schema fully migrated to 2.0")
    """
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    result = {
        "compatible": True,
        "blob_markers": True,
        "lineage_exists": False,
        "issues": [],
    }

    # Check for lineage table
    lineage_check = connection.query(
        """
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = '~lineage'
        """,
        args=(schema,),
    ).fetchone()[0]

    result["lineage_exists"] = lineage_check > 0

    # Check blob column markers
    columns_query = """
        SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND COLUMN_TYPE LIKE '%%blob'
    """
    blob_columns = connection.query(columns_query, args=(schema,)).fetchall()

    for table, column, col_type, comment in blob_columns:
        if not comment.startswith(":<blob"):
            result["blob_markers"] = False
            result["issues"].append(f"{table}.{column}: Missing :<blob>: marker in comment")

    # Overall compatibility
    if result["issues"]:
        result["compatible"] = False

    return result


def migrate_external_pointers_v2(
    schema: str,
    table: str,
    attribute: str,
    source_store: str,
    dest_store: str,
    copy_files: bool = False,
    connection=None,
) -> dict:
    """
    Migrate external storage pointers from 0.14.6 to 2.0 format.

    Converts the 16-byte 0.14 references in one column to JSON metadata,
    in place. File copying (``copy_files``) is not implemented yet.

    This is useful when copying production data to _v2 schemas and you need
    to access external storage attributes but don't want to move the files yet.

    Parameters
    ----------
    schema : str
        Schema name (e.g., 'my_pipeline_v2')
    table : str
        Table name
    attribute : str
        External attribute name (e.g., 'signal')
    source_store : str
        0.14.6 store name (e.g., 'external-raw')
    dest_store : str
        2.0 store name (e.g., 'raw')
    copy_files : bool, optional
        If True, copy blob files to new location.
        If False (default), JSON points to existing files.
    connection : Connection, optional
        Database connection. If None, uses default connection.

    Returns
    -------
    dict
        - rows_migrated: int - number of pointers migrated
        - files_copied: int - number of files copied (if copy_files=True)
        - errors: list - always empty; a reference that cannot be converted
          raises before any row is updated

    Examples
    --------
    >>> # Migrate pointers without moving files
    >>> result = migrate_external_pointers_v2(
    ...     schema='my_pipeline_v2',
    ...     table='recording',
    ...     attribute='signal',
    ...     source_store='external-raw',
    ...     dest_store='raw',
    ...     copy_files=False
    ... )
    >>> print(f"Migrated {result['rows_migrated']} pointers")

    Notes
    -----
    This function:
    1. Reads the BINARY(16) references in the column
    2. Looks each one up in the ~external_{source_store} table
    3. Finds its file where 0.14 stored it, through ``dest_store``
    4. Updates the column with JSON metadata

    It writes the same metadata as ``migrate_external``:
    ``{hash, path, schema, store, size}`` for blobs and attachments (attachments
    also record ``attachment_name``) and ``{path, store, size, is_dir, timestamp}``
    for filepaths.

    The column must already be able to hold the JSON metadata. A column still in
    the 0.14 layout (BINARY(16)) is refused; ``migrate_external`` and
    ``migrate_filepath`` convert such columns in place. Every reference is looked
    up before any row is updated: if one has no tracking row or its file is
    missing, the function raises and changes nothing.
    """
    from . import conn as get_conn

    if connection is None:
        connection = get_conn()

    logger.info(f"Migrating external pointers: {schema}.{table}.{attribute} " f"({source_store} → {dest_store})")

    # Get source store specification (0.14.6)
    # Note: This assumes old external table exists
    external_table = f"~external_{source_store}"

    # Check if external tracking table exists
    check_query = """
        SELECT COUNT(*) FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
    """
    exists = connection.query(check_query, args=(schema, external_table)).fetchone()[0]

    if not exists:
        raise DataJointError(
            f"External tracking table {schema}.{external_table} not found. "
            f"Cannot migrate external pointers from 0.14.6 format."
        )

    result = {
        "rows_migrated": 0,
        "files_copied": 0,
        "errors": [],
    }

    # Find the column and what its 0.14 type was
    column = connection.query(
        """
        SELECT DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_COMMENT
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
        """,
        args=(schema, table, attribute),
    ).fetchone()
    if column is None:
        raise DataJointError(f"Attribute {attribute} not found in {schema}.{table}")
    data_type, max_length, comment = column

    patterns = {**EXTERNAL_PATTERNS, "filepath": FILEPATH_PATTERN}
    kind = next((name for name, pattern in patterns.items() if pattern.search(comment or "")), None)
    if kind is None:
        raise DataJointError(
            f"{schema}.{table}.{attribute} has no 0.14 in-store type (blob@, attach@, or filepath@) in its comment."
        )
    if data_type.lower() == "binary" and max_length == 16:
        raise DataJointError(
            f"{schema}.{table}.{attribute} is a 0.14 BINARY(16) column and cannot hold JSON metadata. "
            f"Use migrate_external or migrate_filepath to convert it in place."
        )

    metadata, problems = _plan_legacy_references(connection, schema, table, attribute, kind, source_store, dest_store)
    _refuse_problems(f"{schema}.{table}.{attribute}", problems)

    if copy_files:
        logger.warning("File copying not yet implemented in migrate_external_pointers_v2")

    with connection.transaction:
        for reference, value in metadata.items():
            cursor = connection.query(
                f"UPDATE `{schema}`.`{table}` SET `{attribute}` = %s WHERE `{attribute}` = %s",
                args=(json.dumps(value), reference),
            )
            result["rows_migrated"] += cursor.rowcount

    logger.info(f"Migrated {result['rows_migrated']} external pointers for {schema}.{table}.{attribute}")

    return result
