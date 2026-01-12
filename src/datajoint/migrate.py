"""
Migration utilities for DataJoint schema updates.

This module provides tools for migrating existing schemas to use the new
Codec system, particularly for upgrading blob columns to use
explicit `<blob>` type declarations.
"""

from __future__ import annotations

import json
import logging
import re
from typing import TYPE_CHECKING

from .errors import DataJointError

if TYPE_CHECKING:
    from .schemas import Schema

logger = logging.getLogger(__name__.split(".")[0])

# Patterns for detecting 0.x external storage columns
EXTERNAL_PATTERNS = {
    "blob": re.compile(r":external(?:-([a-zA-Z_][a-zA-Z0-9_]*))?:", re.I),
    "attach": re.compile(r":external-attach(?:-([a-zA-Z_][a-zA-Z0-9_]*))?:", re.I),
}

FILEPATH_PATTERN = re.compile(r":filepath(?:-([a-zA-Z_][a-zA-Z0-9_]*))?:", re.I)

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
        columns = connection.query(
            columns_query, args=(schema.database, table_name)
        ).fetchall()

        for column_name, column_type, comment in columns:
            comment = comment or ""

            # Check for external blob pattern
            blob_match = EXTERNAL_PATTERNS["blob"].search(comment)
            if blob_match:
                store_name = blob_match.group(1) or "external"
                results.append({
                    "table_name": table_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "comment": comment,
                    "store_name": store_name,
                    "external_type": "blob",
                })
                continue

            # Check for external attach pattern
            attach_match = EXTERNAL_PATTERNS["attach"].search(comment)
            if attach_match:
                store_name = attach_match.group(1) or "external"
                results.append({
                    "table_name": table_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "comment": comment,
                    "store_name": store_name,
                    "external_type": "attach",
                })

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
        # Find VARCHAR columns with :filepath: in comment
        columns_query = """
            SELECT COLUMN_NAME, COLUMN_TYPE, COLUMN_COMMENT
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME = %s
            AND DATA_TYPE = 'varchar'
            AND COLUMN_COMMENT LIKE '%%:filepath%%'
        """
        columns = connection.query(
            columns_query, args=(schema.database, table_name)
        ).fetchall()

        for column_name, column_type, comment in columns:
            comment = comment or ""
            match = FILEPATH_PATTERN.search(comment)
            if match:
                store_name = match.group(1) or "external"
                results.append({
                    "table_name": table_name,
                    "column_name": column_name,
                    "column_type": column_type,
                    "comment": comment,
                    "store_name": store_name,
                })

    return results


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

    3. **Finalize** (finalize=True): Renames columns (old → `_v1`, new → original
       name) and optionally drops the old columns.

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
    >>> # Step 4: Finalize (rename columns, drop old)
    >>> result = migrate_external(schema, finalize=True)

    Notes
    -----
    The migration reads from the hidden `~external_<store>` tables to build
    JSON metadata. Ensure store configuration in datajoint.json matches the
    paths stored in these tables.
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

        # Build new comment
        codec = "blob" if external_type == "blob" else "attach"
        # Remove old :external...: pattern from comment
        new_comment = EXTERNAL_PATTERNS[external_type].sub("", old_comment).strip()
        new_comment = f":{codec}@{store_name}: {new_comment}".strip()

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

            # Check if _v2 column already exists
            existing = connection.query(
                """
                SELECT COLUMN_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                """,
                args=(database, table_name, new_column),
            ).fetchone()

            if existing:
                detail["status"] = "already_migrated"
                logger.info(f"Column {new_column} already exists, skipping")
                result["details"].append(detail)
                continue

            if dry_run:
                # Count rows that would be migrated
                count_sql = f"""
                    SELECT COUNT(*) FROM `{database}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                """
                count = connection.query(count_sql).fetchone()[0]
                detail["rows"] = count
                detail["status"] = "dry_run"
                logger.info(
                    f"Would migrate {database}.{table_name}.{column_name}: "
                    f"{count} rows, store={store_name}"
                )
            else:
                try:
                    # Add new JSON column
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"ADD COLUMN `{new_column}` JSON "
                        f"COMMENT '{new_comment}'"
                    )
                    connection.query(sql)

                    # Copy and convert data from old column
                    # Query the external table for metadata
                    external_table = f"~external_{store_name}"

                    # Get store config for URL building
                    from .settings import config
                    store_config = config.get("stores", {}).get(store_name, {})
                    protocol = store_config.get("protocol", "file")
                    location = store_config.get("location", "")

                    # Update rows with JSON metadata
                    update_sql = f"""
                        UPDATE `{database}`.`{table_name}` t
                        JOIN `{database}`.`{external_table}` e
                        ON t.`{column_name}` = e.hash
                        SET t.`{new_column}` = JSON_OBJECT(
                            'url', CONCAT('{protocol}://', '{location}/', e.filepath),
                            'size', e.size,
                            'hash', HEX(e.hash)
                        )
                        WHERE t.`{column_name}` IS NOT NULL
                    """
                    connection.query(update_sql)

                    # Count migrated rows
                    count_sql = f"""
                        SELECT COUNT(*) FROM `{database}`.`{table_name}`
                        WHERE `{new_column}` IS NOT NULL
                    """
                    count = connection.query(count_sql).fetchone()[0]
                    detail["rows"] = count
                    detail["status"] = "migrated"
                    result["columns_migrated"] += 1
                    result["rows_migrated"] += count

                    logger.info(
                        f"Migrated {database}.{table_name}.{column_name}: "
                        f"{count} rows"
                    )
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(
                        f"Failed to migrate {table_name}.{column_name}: {e}"
                    )
                    raise DataJointError(f"Migration failed: {e}") from e

        result["details"].append(detail)

    return result


def migrate_filepath(
    schema: Schema,
    dry_run: bool = True,
    finalize: bool = False,
) -> dict:
    """
    Migrate filepath columns from 0.x to 2.0 format.

    Same multi-step approach as migrate_external:

    1. **Initial run**: Adds new `<column>_v2` columns with JSON type
    2. **Verification**: Verify files accessible via DataJoint 2.0
    3. **Finalize**: Rename columns and drop old

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
    >>> result = migrate_filepath(schema, finalize=True)
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

        # Build new comment
        new_comment = FILEPATH_PATTERN.sub("", old_comment).strip()
        new_comment = f":filepath@{store_name}: {new_comment}".strip()

        new_column = f"{column_name}_v2"

        if finalize:
            detail["action"] = "finalize"

            if dry_run:
                logger.info(
                    f"Would finalize {database}.{table_name}.{column_name}"
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
                    logger.error(f"Failed to finalize: {e}")
                    raise DataJointError(f"Finalize failed: {e}") from e
        else:
            detail["action"] = "migrate"

            # Check if _v2 column already exists
            existing = connection.query(
                """
                SELECT COLUMN_NAME FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s AND COLUMN_NAME = %s
                """,
                args=(database, table_name, new_column),
            ).fetchone()

            if existing:
                detail["status"] = "already_migrated"
                result["details"].append(detail)
                continue

            if dry_run:
                count_sql = f"""
                    SELECT COUNT(*) FROM `{database}`.`{table_name}`
                    WHERE `{column_name}` IS NOT NULL
                """
                count = connection.query(count_sql).fetchone()[0]
                detail["rows"] = count
                detail["status"] = "dry_run"
                logger.info(
                    f"Would migrate {database}.{table_name}.{column_name}: "
                    f"{count} rows"
                )
            else:
                try:
                    # Get store config
                    from .settings import config
                    store_config = config.get("stores", {}).get(store_name, {})
                    protocol = store_config.get("protocol", "file")
                    location = store_config.get("location", "")

                    # Add new JSON column
                    sql = (
                        f"ALTER TABLE `{database}`.`{table_name}` "
                        f"ADD COLUMN `{new_column}` JSON "
                        f"COMMENT '{new_comment}'"
                    )
                    connection.query(sql)

                    # Convert filepath to JSON with URL
                    update_sql = f"""
                        UPDATE `{database}`.`{table_name}`
                        SET `{new_column}` = JSON_OBJECT(
                            'url', CONCAT('{protocol}://', '{location}/', `{column_name}`)
                        )
                        WHERE `{column_name}` IS NOT NULL
                    """
                    connection.query(update_sql)

                    count_sql = f"""
                        SELECT COUNT(*) FROM `{database}`.`{table_name}`
                        WHERE `{new_column}` IS NOT NULL
                    """
                    count = connection.query(count_sql).fetchone()[0]
                    detail["rows"] = count
                    detail["status"] = "migrated"
                    result["columns_migrated"] += 1
                    result["rows_migrated"] += count

                    logger.info(
                        f"Migrated {database}.{table_name}.{column_name}: "
                        f"{count} rows"
                    )
                except Exception as e:
                    detail["status"] = "error"
                    detail["error"] = str(e)
                    logger.error(f"Failed to migrate: {e}")
                    raise DataJointError(f"Migration failed: {e}") from e

        result["details"].append(detail)

    return result
