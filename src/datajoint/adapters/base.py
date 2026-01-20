"""
Abstract base class for database backend adapters.

This module defines the interface that all database adapters must implement
to support multiple database backends (MySQL, PostgreSQL, etc.) in DataJoint.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class DatabaseAdapter(ABC):
    """
    Abstract base class for database backend adapters.

    Adapters provide database-specific implementations for SQL generation,
    type mapping, error translation, and connection management.
    """

    # =========================================================================
    # Connection Management
    # =========================================================================

    @abstractmethod
    def connect(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        **kwargs: Any,
    ) -> Any:
        """
        Establish database connection.

        Parameters
        ----------
        host : str
            Database server hostname.
        port : int
            Database server port.
        user : str
            Username for authentication.
        password : str
            Password for authentication.
        **kwargs : Any
            Additional backend-specific connection parameters.

        Returns
        -------
        Any
            Database connection object (backend-specific).
        """
        ...

    @abstractmethod
    def close(self, connection: Any) -> None:
        """
        Close the database connection.

        Parameters
        ----------
        connection : Any
            Database connection object to close.
        """
        ...

    @abstractmethod
    def ping(self, connection: Any) -> bool:
        """
        Check if connection is alive.

        Parameters
        ----------
        connection : Any
            Database connection object to check.

        Returns
        -------
        bool
            True if connection is alive, False otherwise.
        """
        ...

    @abstractmethod
    def get_connection_id(self, connection: Any) -> int:
        """
        Get the current connection/backend process ID.

        Parameters
        ----------
        connection : Any
            Database connection object.

        Returns
        -------
        int
            Connection or process ID.
        """
        ...

    @property
    @abstractmethod
    def default_port(self) -> int:
        """
        Default port for this database backend.

        Returns
        -------
        int
            Default port number (3306 for MySQL, 5432 for PostgreSQL).
        """
        ...

    @property
    @abstractmethod
    def backend(self) -> str:
        """
        Backend identifier string.

        Returns
        -------
        str
            Backend name: 'mysql' or 'postgresql'.
        """
        ...

    @abstractmethod
    def get_cursor(self, connection: Any, as_dict: bool = False) -> Any:
        """
        Get a cursor from the database connection.

        Parameters
        ----------
        connection : Any
            Database connection object.
        as_dict : bool, optional
            If True, return cursor that yields rows as dictionaries.
            If False, return cursor that yields rows as tuples.
            Default False.

        Returns
        -------
        Any
            Database cursor object (backend-specific).
        """
        ...

    # =========================================================================
    # SQL Syntax
    # =========================================================================

    @abstractmethod
    def quote_identifier(self, name: str) -> str:
        """
        Quote an identifier (table/column name) for this backend.

        Parameters
        ----------
        name : str
            Identifier to quote.

        Returns
        -------
        str
            Quoted identifier (e.g., `name` for MySQL, "name" for PostgreSQL).
        """
        ...

    @abstractmethod
    def quote_string(self, value: str) -> str:
        """
        Quote a string literal for this backend.

        Parameters
        ----------
        value : str
            String value to quote.

        Returns
        -------
        str
            Quoted string literal with proper escaping.
        """
        ...

    @property
    @abstractmethod
    def parameter_placeholder(self) -> str:
        """
        Parameter placeholder style for this backend.

        Returns
        -------
        str
            Placeholder string (e.g., '%s' for MySQL/psycopg2, '?' for SQLite).
        """
        ...

    # =========================================================================
    # Type Mapping
    # =========================================================================

    @abstractmethod
    def core_type_to_sql(self, core_type: str) -> str:
        """
        Convert a DataJoint core type to backend SQL type.

        Parameters
        ----------
        core_type : str
            DataJoint core type (e.g., 'int64', 'float32', 'uuid').

        Returns
        -------
        str
            Backend SQL type (e.g., 'bigint', 'float', 'binary(16)').

        Raises
        ------
        ValueError
            If core_type is not a valid DataJoint core type.
        """
        ...

    @abstractmethod
    def sql_type_to_core(self, sql_type: str) -> str | None:
        """
        Convert a backend SQL type to DataJoint core type (if mappable).

        Parameters
        ----------
        sql_type : str
            Backend SQL type.

        Returns
        -------
        str or None
            DataJoint core type if mappable, None otherwise.
        """
        ...

    # =========================================================================
    # DDL Generation
    # =========================================================================

    @abstractmethod
    def create_schema_sql(self, schema_name: str) -> str:
        """
        Generate CREATE SCHEMA/DATABASE statement.

        Parameters
        ----------
        schema_name : str
            Name of schema/database to create.

        Returns
        -------
        str
            CREATE SCHEMA/DATABASE SQL statement.
        """
        ...

    @abstractmethod
    def drop_schema_sql(self, schema_name: str, if_exists: bool = True) -> str:
        """
        Generate DROP SCHEMA/DATABASE statement.

        Parameters
        ----------
        schema_name : str
            Name of schema/database to drop.
        if_exists : bool, optional
            Include IF EXISTS clause. Default True.

        Returns
        -------
        str
            DROP SCHEMA/DATABASE SQL statement.
        """
        ...

    @abstractmethod
    def create_table_sql(
        self,
        table_name: str,
        columns: list[dict[str, Any]],
        primary_key: list[str],
        foreign_keys: list[dict[str, Any]],
        indexes: list[dict[str, Any]],
        comment: str | None = None,
    ) -> str:
        """
        Generate CREATE TABLE statement.

        Parameters
        ----------
        table_name : str
            Name of table to create.
        columns : list[dict]
            Column definitions with keys: name, type, nullable, default, comment.
        primary_key : list[str]
            List of primary key column names.
        foreign_keys : list[dict]
            Foreign key definitions with keys: columns, ref_table, ref_columns.
        indexes : list[dict]
            Index definitions with keys: columns, unique.
        comment : str, optional
            Table comment.

        Returns
        -------
        str
            CREATE TABLE SQL statement.
        """
        ...

    @abstractmethod
    def drop_table_sql(self, table_name: str, if_exists: bool = True) -> str:
        """
        Generate DROP TABLE statement.

        Parameters
        ----------
        table_name : str
            Name of table to drop.
        if_exists : bool, optional
            Include IF EXISTS clause. Default True.

        Returns
        -------
        str
            DROP TABLE SQL statement.
        """
        ...

    @abstractmethod
    def alter_table_sql(
        self,
        table_name: str,
        add_columns: list[dict[str, Any]] | None = None,
        drop_columns: list[str] | None = None,
        modify_columns: list[dict[str, Any]] | None = None,
    ) -> str:
        """
        Generate ALTER TABLE statement.

        Parameters
        ----------
        table_name : str
            Name of table to alter.
        add_columns : list[dict], optional
            Columns to add with keys: name, type, nullable, default, comment.
        drop_columns : list[str], optional
            Column names to drop.
        modify_columns : list[dict], optional
            Columns to modify with keys: name, type, nullable, default, comment.

        Returns
        -------
        str
            ALTER TABLE SQL statement.
        """
        ...

    @abstractmethod
    def add_comment_sql(
        self,
        object_type: str,
        object_name: str,
        comment: str,
    ) -> str | None:
        """
        Generate comment statement (may be None if embedded in CREATE).

        Parameters
        ----------
        object_type : str
            Type of object ('table', 'column').
        object_name : str
            Fully qualified object name.
        comment : str
            Comment text.

        Returns
        -------
        str or None
            COMMENT statement, or None if comments are inline in CREATE.
        """
        ...

    # =========================================================================
    # DML Generation
    # =========================================================================

    @abstractmethod
    def insert_sql(
        self,
        table_name: str,
        columns: list[str],
        on_duplicate: str | None = None,
    ) -> str:
        """
        Generate INSERT statement.

        Parameters
        ----------
        table_name : str
            Name of table to insert into.
        columns : list[str]
            Column names to insert.
        on_duplicate : str, optional
            Duplicate handling: 'ignore', 'replace', 'update', or None.

        Returns
        -------
        str
            INSERT SQL statement with parameter placeholders.
        """
        ...

    @abstractmethod
    def update_sql(
        self,
        table_name: str,
        set_columns: list[str],
        where_columns: list[str],
    ) -> str:
        """
        Generate UPDATE statement.

        Parameters
        ----------
        table_name : str
            Name of table to update.
        set_columns : list[str]
            Column names to set.
        where_columns : list[str]
            Column names for WHERE clause.

        Returns
        -------
        str
            UPDATE SQL statement with parameter placeholders.
        """
        ...

    @abstractmethod
    def delete_sql(self, table_name: str) -> str:
        """
        Generate DELETE statement (WHERE clause added separately).

        Parameters
        ----------
        table_name : str
            Name of table to delete from.

        Returns
        -------
        str
            DELETE SQL statement without WHERE clause.
        """
        ...

    @abstractmethod
    def upsert_on_duplicate_sql(
        self,
        table_name: str,
        columns: list[str],
        primary_key: list[str],
        num_rows: int,
    ) -> str:
        """
        Generate INSERT ... ON DUPLICATE KEY UPDATE (MySQL) or
        INSERT ... ON CONFLICT ... DO UPDATE (PostgreSQL) statement.

        Parameters
        ----------
        table_name : str
            Fully qualified table name (with quotes).
        columns : list[str]
            Column names to insert (unquoted).
        primary_key : list[str]
            Primary key column names (unquoted) for conflict detection.
        num_rows : int
            Number of rows to insert (for generating placeholders).

        Returns
        -------
        str
            Upsert SQL statement with placeholders.

        Examples
        --------
        MySQL:
            INSERT INTO `table` (a, b, c) VALUES (%s, %s, %s), (%s, %s, %s)
            ON DUPLICATE KEY UPDATE a = VALUES(a), b = VALUES(b), c = VALUES(c)

        PostgreSQL:
            INSERT INTO "table" (a, b, c) VALUES (%s, %s, %s), (%s, %s, %s)
            ON CONFLICT (a) DO UPDATE SET b = EXCLUDED.b, c = EXCLUDED.c
        """
        ...

    @abstractmethod
    def skip_duplicates_clause(
        self,
        full_table_name: str,
        primary_key: list[str],
    ) -> str:
        """
        Generate clause to skip duplicate key insertions.

        For MySQL: ON DUPLICATE KEY UPDATE pk=table.pk (no-op update)
        For PostgreSQL: ON CONFLICT (pk_cols) DO NOTHING

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (with quotes).
        primary_key : list[str]
            Primary key column names (unquoted).

        Returns
        -------
        str
            SQL clause to append to INSERT statement.
        """
        ...

    @property
    def supports_inline_indexes(self) -> bool:
        """
        Whether this backend supports inline INDEX in CREATE TABLE.

        MySQL supports inline index definitions in CREATE TABLE.
        PostgreSQL requires separate CREATE INDEX statements.

        Returns
        -------
        bool
            True for MySQL, False for PostgreSQL.
        """
        return True  # Default for MySQL, override in PostgreSQL

    def create_index_ddl(
        self,
        full_table_name: str,
        columns: list[str],
        unique: bool = False,
        index_name: str | None = None,
    ) -> str:
        """
        Generate CREATE INDEX statement.

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (with quotes).
        columns : list[str]
            Column names to index (unquoted).
        unique : bool, optional
            If True, create a unique index.
        index_name : str, optional
            Custom index name. If None, auto-generate from table/columns.

        Returns
        -------
        str
            CREATE INDEX SQL statement.
        """
        quoted_cols = ", ".join(self.quote_identifier(col) for col in columns)
        # Generate index name from table and columns if not provided
        if index_name is None:
            # Extract table name from full_table_name for index naming
            table_part = full_table_name.split(".")[-1].strip('`"')
            col_part = "_".join(columns)[:30]  # Truncate for long column lists
            index_name = f"idx_{table_part}_{col_part}"
        unique_clause = "UNIQUE " if unique else ""
        return f"CREATE {unique_clause}INDEX {self.quote_identifier(index_name)} ON {full_table_name} ({quoted_cols})"

    # =========================================================================
    # Introspection
    # =========================================================================

    @abstractmethod
    def list_schemas_sql(self) -> str:
        """
        Generate query to list all schemas/databases.

        Returns
        -------
        str
            SQL query to list schemas.
        """
        ...

    @abstractmethod
    def list_tables_sql(self, schema_name: str) -> str:
        """
        Generate query to list tables in a schema.

        Parameters
        ----------
        schema_name : str
            Name of schema to list tables from.

        Returns
        -------
        str
            SQL query to list tables.
        """
        ...

    @abstractmethod
    def get_table_info_sql(self, schema_name: str, table_name: str) -> str:
        """
        Generate query to get table metadata (comment, engine, etc.).

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query to get table info.
        """
        ...

    @abstractmethod
    def get_columns_sql(self, schema_name: str, table_name: str) -> str:
        """
        Generate query to get column definitions.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query to get column definitions.
        """
        ...

    @abstractmethod
    def get_primary_key_sql(self, schema_name: str, table_name: str) -> str:
        """
        Generate query to get primary key columns.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query to get primary key columns.
        """
        ...

    @abstractmethod
    def get_foreign_keys_sql(self, schema_name: str, table_name: str) -> str:
        """
        Generate query to get foreign key constraints.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query to get foreign key constraints.
        """
        ...

    @abstractmethod
    def get_constraint_info_sql(self, constraint_name: str, schema_name: str, table_name: str) -> str:
        """
        Generate query to get foreign key constraint details from information_schema.

        Used during cascade delete to determine FK columns when error message
        doesn't provide full details.

        Parameters
        ----------
        constraint_name : str
            Name of the foreign key constraint.
        schema_name : str
            Schema/database name of the child table.
        table_name : str
            Name of the child table.

        Returns
        -------
        str
            SQL query that returns rows with columns:
            - fk_attrs: foreign key column name in child table
            - parent: parent table name (quoted, with schema)
            - pk_attrs: referenced column name in parent table
        """
        ...

    @abstractmethod
    def parse_foreign_key_error(self, error_message: str) -> dict[str, str | list[str] | None] | None:
        """
        Parse a foreign key violation error message to extract constraint details.

        Used during cascade delete to identify which child table is preventing
        deletion and what columns are involved.

        Parameters
        ----------
        error_message : str
            The error message from a foreign key constraint violation.

        Returns
        -------
        dict or None
            Dictionary with keys if successfully parsed:
            - child: child table name (quoted with schema if available)
            - name: constraint name (quoted)
            - fk_attrs: list of foreign key column names (may be None if not in message)
            - parent: parent table name (quoted, may be None if not in message)
            - pk_attrs: list of parent key column names (may be None if not in message)

            Returns None if error message doesn't match FK violation pattern.

        Examples
        --------
        MySQL error:
            "Cannot delete or update a parent row: a foreign key constraint fails
            (`schema`.`child`, CONSTRAINT `fk_name` FOREIGN KEY (`child_col`)
            REFERENCES `parent` (`parent_col`))"

        PostgreSQL error:
            "update or delete on table \"parent\" violates foreign key constraint
            \"child_parent_id_fkey\" on table \"child\"
            DETAIL:  Key (parent_id)=(1) is still referenced from table \"child\"."
        """
        ...

    @abstractmethod
    def get_indexes_sql(self, schema_name: str, table_name: str) -> str:
        """
        Generate query to get index definitions.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query to get index definitions.
        """
        ...

    @abstractmethod
    def parse_column_info(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Parse a column info row into standardized format.

        Parameters
        ----------
        row : dict
            Raw column info row from database introspection query.

        Returns
        -------
        dict
            Standardized column info with keys: name, type, nullable,
            default, comment, etc.
        """
        ...

    # =========================================================================
    # Transactions
    # =========================================================================

    @abstractmethod
    def start_transaction_sql(self, isolation_level: str | None = None) -> str:
        """
        Generate START TRANSACTION statement.

        Parameters
        ----------
        isolation_level : str, optional
            Transaction isolation level.

        Returns
        -------
        str
            START TRANSACTION SQL statement.
        """
        ...

    @abstractmethod
    def commit_sql(self) -> str:
        """
        Generate COMMIT statement.

        Returns
        -------
        str
            COMMIT SQL statement.
        """
        ...

    @abstractmethod
    def rollback_sql(self) -> str:
        """
        Generate ROLLBACK statement.

        Returns
        -------
        str
            ROLLBACK SQL statement.
        """
        ...

    # =========================================================================
    # Functions and Expressions
    # =========================================================================

    @abstractmethod
    def current_timestamp_expr(self, precision: int | None = None) -> str:
        """
        Expression for current timestamp.

        Parameters
        ----------
        precision : int, optional
            Fractional seconds precision (0-6).

        Returns
        -------
        str
            SQL expression for current timestamp.
        """
        ...

    @abstractmethod
    def interval_expr(self, value: int, unit: str) -> str:
        """
        Expression for time interval.

        Parameters
        ----------
        value : int
            Interval value.
        unit : str
            Time unit ('second', 'minute', 'hour', 'day', etc.).

        Returns
        -------
        str
            SQL expression for interval (e.g., 'INTERVAL 5 SECOND' for MySQL,
            "INTERVAL '5 seconds'" for PostgreSQL).
        """
        ...

    @abstractmethod
    def json_path_expr(self, column: str, path: str, return_type: str | None = None) -> str:
        """
        Generate JSON path extraction expression.

        Parameters
        ----------
        column : str
            Column name containing JSON data.
        path : str
            JSON path (e.g., 'field' or 'nested.field').
        return_type : str, optional
            Return type specification (MySQL-specific).

        Returns
        -------
        str
            Database-specific JSON extraction SQL expression.

        Examples
        --------
        MySQL: json_value(`column`, _utf8mb4'$.path' returning type)
        PostgreSQL: jsonb_extract_path_text("column", 'path_part1', 'path_part2')
        """
        ...

    # =========================================================================
    # DDL Generation
    # =========================================================================

    @abstractmethod
    def format_column_definition(
        self,
        name: str,
        sql_type: str,
        nullable: bool = False,
        default: str | None = None,
        comment: str | None = None,
    ) -> str:
        """
        Format a column definition for DDL.

        Parameters
        ----------
        name : str
            Column name.
        sql_type : str
            SQL type (already backend-specific, e.g., 'bigint', 'varchar(255)').
        nullable : bool, optional
            Whether column is nullable. Default False.
        default : str | None, optional
            Default value expression (e.g., 'NULL', '"value"', 'CURRENT_TIMESTAMP').
        comment : str | None, optional
            Column comment.

        Returns
        -------
        str
            Formatted column definition (without trailing comma).

        Examples
        --------
        MySQL: `name` bigint NOT NULL COMMENT "user ID"
        PostgreSQL: "name" bigint NOT NULL
        """
        ...

    @abstractmethod
    def table_options_clause(self, comment: str | None = None) -> str:
        """
        Generate table options clause (ENGINE, etc.) for CREATE TABLE.

        Parameters
        ----------
        comment : str | None, optional
            Table-level comment.

        Returns
        -------
        str
            Table options clause (e.g., 'ENGINE=InnoDB, COMMENT "..."' for MySQL).

        Examples
        --------
        MySQL: ENGINE=InnoDB, COMMENT "experiment sessions"
        PostgreSQL: (empty string, comments handled separately)
        """
        ...

    @abstractmethod
    def table_comment_ddl(self, full_table_name: str, comment: str) -> str | None:
        """
        Generate DDL for table-level comment (if separate from CREATE TABLE).

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (quoted).
        comment : str
            Table comment.

        Returns
        -------
        str or None
            DDL statement for table comment, or None if handled inline.

        Examples
        --------
        MySQL: None (inline)
        PostgreSQL: COMMENT ON TABLE "schema"."table" IS 'comment text'
        """
        ...

    @abstractmethod
    def column_comment_ddl(self, full_table_name: str, column_name: str, comment: str) -> str | None:
        """
        Generate DDL for column-level comment (if separate from CREATE TABLE).

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (quoted).
        column_name : str
            Column name (unquoted).
        comment : str
            Column comment.

        Returns
        -------
        str or None
            DDL statement for column comment, or None if handled inline.

        Examples
        --------
        MySQL: None (inline)
        PostgreSQL: COMMENT ON COLUMN "schema"."table"."column" IS 'comment text'
        """
        ...

    @abstractmethod
    def enum_type_ddl(self, type_name: str, values: list[str]) -> str | None:
        """
        Generate DDL for enum type definition (if needed before CREATE TABLE).

        Parameters
        ----------
        type_name : str
            Enum type name.
        values : list[str]
            Enum values.

        Returns
        -------
        str or None
            DDL statement for enum type, or None if handled inline.

        Examples
        --------
        MySQL: None (inline enum('val1', 'val2'))
        PostgreSQL: CREATE TYPE "type_name" AS ENUM ('val1', 'val2')
        """
        ...

    @abstractmethod
    def job_metadata_columns(self) -> list[str]:
        """
        Return job metadata column definitions for Computed/Imported tables.

        Returns
        -------
        list[str]
            List of column definition strings (fully formatted with quotes).

        Examples
        --------
        MySQL:
            ["`_job_start_time` datetime(3) DEFAULT NULL",
             "`_job_duration` float DEFAULT NULL",
             "`_job_version` varchar(64) DEFAULT ''"]
        PostgreSQL:
            ['"_job_start_time" timestamp DEFAULT NULL',
             '"_job_duration" real DEFAULT NULL',
             '"_job_version" varchar(64) DEFAULT \'\'']
        """
        ...

    # =========================================================================
    # Error Translation
    # =========================================================================

    @abstractmethod
    def translate_error(self, error: Exception, query: str = "") -> Exception:
        """
        Translate backend-specific error to DataJoint error.

        Parameters
        ----------
        error : Exception
            Backend-specific exception.

        Returns
        -------
        Exception
            DataJoint exception or original error if no mapping exists.
        """
        ...

    # =========================================================================
    # Native Type Validation
    # =========================================================================

    @abstractmethod
    def validate_native_type(self, type_str: str) -> bool:
        """
        Check if a native type string is valid for this backend.

        Parameters
        ----------
        type_str : str
            Native type string to validate.

        Returns
        -------
        bool
            True if valid for this backend, False otherwise.
        """
        ...
