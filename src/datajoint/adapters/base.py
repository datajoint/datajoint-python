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

    # =========================================================================
    # Error Translation
    # =========================================================================

    @abstractmethod
    def translate_error(self, error: Exception) -> Exception:
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
