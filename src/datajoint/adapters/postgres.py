"""
PostgreSQL database adapter for DataJoint.

This module provides PostgreSQL-specific implementations for SQL generation,
type mapping, error translation, and connection management.
"""

from __future__ import annotations

from typing import Any

try:
    import psycopg2 as client
    from psycopg2 import sql
except ImportError:
    client = None  # type: ignore
    sql = None  # type: ignore

from .. import errors
from .base import DatabaseAdapter

# Core type mapping: DataJoint core types → PostgreSQL types
CORE_TYPE_MAP = {
    "int64": "bigint",
    "int32": "integer",
    "int16": "smallint",
    "int8": "smallint",  # PostgreSQL lacks tinyint; semantically equivalent
    "float32": "real",
    "float64": "double precision",
    "bool": "boolean",
    "uuid": "uuid",  # Native UUID support
    "bytes": "bytea",
    "json": "jsonb",  # Using jsonb for better performance
    "date": "date",
    # datetime, char, varchar, decimal, enum require parameters - handled in method
}

# Reverse mapping: PostgreSQL types → DataJoint core types (for introspection)
SQL_TO_CORE_MAP = {
    "bigint": "int64",
    "integer": "int32",
    "smallint": "int16",
    "real": "float32",
    "double precision": "float64",
    "boolean": "bool",
    "uuid": "uuid",
    "bytea": "bytes",
    "jsonb": "json",
    "json": "json",
    "date": "date",
}


class PostgreSQLAdapter(DatabaseAdapter):
    """PostgreSQL database adapter implementation."""

    def __init__(self) -> None:
        """Initialize PostgreSQL adapter."""
        if client is None:
            raise ImportError(
                "psycopg2 is required for PostgreSQL support. " "Install it with: pip install 'datajoint[postgres]'"
            )

    # =========================================================================
    # Connection Management
    # =========================================================================

    def connect(
        self,
        host: str,
        port: int,
        user: str,
        password: str,
        **kwargs: Any,
    ) -> Any:
        """
        Establish PostgreSQL connection.

        Parameters
        ----------
        host : str
            PostgreSQL server hostname.
        port : int
            PostgreSQL server port.
        user : str
            Username for authentication.
        password : str
            Password for authentication.
        **kwargs : Any
            Additional PostgreSQL-specific parameters:
            - dbname: Database name
            - sslmode: SSL mode ('disable', 'allow', 'prefer', 'require')
            - connect_timeout: Connection timeout in seconds

        Returns
        -------
        psycopg2.connection
            PostgreSQL connection object.
        """
        dbname = kwargs.get("dbname", "postgres")  # Default to postgres database
        sslmode = kwargs.get("sslmode", "prefer")
        connect_timeout = kwargs.get("connect_timeout", 10)

        return client.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            sslmode=sslmode,
            connect_timeout=connect_timeout,
        )

    def close(self, connection: Any) -> None:
        """Close the PostgreSQL connection."""
        connection.close()

    def ping(self, connection: Any) -> bool:
        """
        Check if PostgreSQL connection is alive.

        Returns
        -------
        bool
            True if connection is alive.
        """
        try:
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            return True
        except Exception:
            return False

    def get_connection_id(self, connection: Any) -> int:
        """
        Get PostgreSQL backend process ID.

        Returns
        -------
        int
            PostgreSQL pg_backend_pid().
        """
        cursor = connection.cursor()
        cursor.execute("SELECT pg_backend_pid()")
        return cursor.fetchone()[0]

    @property
    def default_port(self) -> int:
        """PostgreSQL default port 5432."""
        return 5432

    def get_cursor(self, connection: Any, as_dict: bool = False) -> Any:
        """
        Get a cursor from PostgreSQL connection.

        Parameters
        ----------
        connection : Any
            psycopg2 connection object.
        as_dict : bool, optional
            If True, return Real DictCursor that yields rows as dictionaries.
            If False, return standard cursor that yields rows as tuples.
            Default False.

        Returns
        -------
        Any
            psycopg2 cursor object.
        """
        import psycopg2.extras

        if as_dict:
            return connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return connection.cursor()

    # =========================================================================
    # SQL Syntax
    # =========================================================================

    def quote_identifier(self, name: str) -> str:
        """
        Quote identifier with double quotes for PostgreSQL.

        Parameters
        ----------
        name : str
            Identifier to quote.

        Returns
        -------
        str
            Double-quoted identifier: "name"
        """
        return f'"{name}"'

    def quote_string(self, value: str) -> str:
        """
        Quote string literal for PostgreSQL with escaping.

        Parameters
        ----------
        value : str
            String value to quote.

        Returns
        -------
        str
            Quoted and escaped string literal.
        """
        # Escape single quotes by doubling them (PostgreSQL standard)
        escaped = value.replace("'", "''")
        return f"'{escaped}'"

    @property
    def parameter_placeholder(self) -> str:
        """PostgreSQL/psycopg2 uses %s placeholders."""
        return "%s"

    # =========================================================================
    # Type Mapping
    # =========================================================================

    def core_type_to_sql(self, core_type: str) -> str:
        """
        Convert DataJoint core type to PostgreSQL type.

        Parameters
        ----------
        core_type : str
            DataJoint core type, possibly with parameters:
            - int64, float32, bool, uuid, bytes, json, date
            - datetime or datetime(n) → timestamp(n)
            - char(n), varchar(n)
            - decimal(p,s) → numeric(p,s)
            - enum('a','b','c') → requires CREATE TYPE

        Returns
        -------
        str
            PostgreSQL SQL type.

        Raises
        ------
        ValueError
            If core_type is not recognized.
        """
        # Handle simple types without parameters
        if core_type in CORE_TYPE_MAP:
            return CORE_TYPE_MAP[core_type]

        # Handle parametrized types
        if core_type.startswith("datetime"):
            # datetime or datetime(precision) → timestamp or timestamp(precision)
            if "(" in core_type:
                # Extract precision: datetime(3) → timestamp(3)
                precision = core_type[core_type.index("(") : core_type.index(")") + 1]
                return f"timestamp{precision}"
            return "timestamp"

        if core_type.startswith("char("):
            # char(n)
            return core_type

        if core_type.startswith("varchar("):
            # varchar(n)
            return core_type

        if core_type.startswith("decimal("):
            # decimal(precision, scale) → numeric(precision, scale)
            params = core_type[7:]  # Remove "decimal"
            return f"numeric{params}"

        if core_type.startswith("enum("):
            # Enum requires special handling - caller must use CREATE TYPE
            # Return the type name pattern (will be replaced by caller)
            return "{{enum_type_name}}"  # Placeholder for CREATE TYPE

        raise ValueError(f"Unknown core type: {core_type}")

    def sql_type_to_core(self, sql_type: str) -> str | None:
        """
        Convert PostgreSQL type to DataJoint core type (if mappable).

        Parameters
        ----------
        sql_type : str
            PostgreSQL SQL type.

        Returns
        -------
        str or None
            DataJoint core type if mappable, None otherwise.
        """
        # Normalize type string (lowercase, strip spaces)
        sql_type_lower = sql_type.lower().strip()

        # Direct mapping
        if sql_type_lower in SQL_TO_CORE_MAP:
            return SQL_TO_CORE_MAP[sql_type_lower]

        # Handle parametrized types
        if sql_type_lower.startswith("timestamp"):
            # timestamp(n) → datetime(n)
            if "(" in sql_type_lower:
                precision = sql_type_lower[sql_type_lower.index("(") : sql_type_lower.index(")") + 1]
                return f"datetime{precision}"
            return "datetime"

        if sql_type_lower.startswith("char("):
            return sql_type  # Keep size

        if sql_type_lower.startswith("varchar("):
            return sql_type  # Keep size

        if sql_type_lower.startswith("numeric("):
            # numeric(p,s) → decimal(p,s)
            params = sql_type_lower[7:]  # Remove "numeric"
            return f"decimal{params}"

        # Not a mappable core type
        return None

    # =========================================================================
    # DDL Generation
    # =========================================================================

    def create_schema_sql(self, schema_name: str) -> str:
        """
        Generate CREATE SCHEMA statement for PostgreSQL.

        Parameters
        ----------
        schema_name : str
            Schema name.

        Returns
        -------
        str
            CREATE SCHEMA SQL.
        """
        return f"CREATE SCHEMA {self.quote_identifier(schema_name)}"

    def drop_schema_sql(self, schema_name: str, if_exists: bool = True) -> str:
        """
        Generate DROP SCHEMA statement for PostgreSQL.

        Parameters
        ----------
        schema_name : str
            Schema name.
        if_exists : bool
            Include IF EXISTS clause.

        Returns
        -------
        str
            DROP SCHEMA SQL.
        """
        if_exists_clause = "IF EXISTS " if if_exists else ""
        return f"DROP SCHEMA {if_exists_clause}{self.quote_identifier(schema_name)} CASCADE"

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
        Generate CREATE TABLE statement for PostgreSQL.

        Parameters
        ----------
        table_name : str
            Fully qualified table name (schema.table).
        columns : list[dict]
            Column defs: [{name, type, nullable, default, comment}, ...]
        primary_key : list[str]
            Primary key column names.
        foreign_keys : list[dict]
            FK defs: [{columns, ref_table, ref_columns}, ...]
        indexes : list[dict]
            Index defs: [{columns, unique}, ...]
        comment : str, optional
            Table comment (added via separate COMMENT ON statement).

        Returns
        -------
        str
            CREATE TABLE SQL statement (comments via separate COMMENT ON).
        """
        lines = []

        # Column definitions
        for col in columns:
            col_name = self.quote_identifier(col["name"])
            col_type = col["type"]
            nullable = "NULL" if col.get("nullable", False) else "NOT NULL"
            default = f" DEFAULT {col['default']}" if "default" in col else ""
            # PostgreSQL comments are via COMMENT ON, not inline
            lines.append(f"{col_name} {col_type} {nullable}{default}")

        # Primary key
        if primary_key:
            pk_cols = ", ".join(self.quote_identifier(col) for col in primary_key)
            lines.append(f"PRIMARY KEY ({pk_cols})")

        # Foreign keys
        for fk in foreign_keys:
            fk_cols = ", ".join(self.quote_identifier(col) for col in fk["columns"])
            ref_cols = ", ".join(self.quote_identifier(col) for col in fk["ref_columns"])
            lines.append(
                f"FOREIGN KEY ({fk_cols}) REFERENCES {fk['ref_table']} ({ref_cols}) " f"ON UPDATE CASCADE ON DELETE RESTRICT"
            )

        # Indexes - PostgreSQL creates indexes separately via CREATE INDEX
        # (handled by caller after table creation)

        # Assemble CREATE TABLE (no ENGINE in PostgreSQL)
        table_def = ",\n  ".join(lines)
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {table_def}\n)"

    def drop_table_sql(self, table_name: str, if_exists: bool = True) -> str:
        """Generate DROP TABLE statement for PostgreSQL."""
        if_exists_clause = "IF EXISTS " if if_exists else ""
        return f"DROP TABLE {if_exists_clause}{table_name} CASCADE"

    def alter_table_sql(
        self,
        table_name: str,
        add_columns: list[dict[str, Any]] | None = None,
        drop_columns: list[str] | None = None,
        modify_columns: list[dict[str, Any]] | None = None,
    ) -> str:
        """
        Generate ALTER TABLE statement for PostgreSQL.

        Parameters
        ----------
        table_name : str
            Table name.
        add_columns : list[dict], optional
            Columns to add.
        drop_columns : list[str], optional
            Column names to drop.
        modify_columns : list[dict], optional
            Columns to modify.

        Returns
        -------
        str
            ALTER TABLE SQL statement.
        """
        clauses = []

        if add_columns:
            for col in add_columns:
                col_name = self.quote_identifier(col["name"])
                col_type = col["type"]
                nullable = "NULL" if col.get("nullable", False) else "NOT NULL"
                clauses.append(f"ADD COLUMN {col_name} {col_type} {nullable}")

        if drop_columns:
            for col_name in drop_columns:
                clauses.append(f"DROP COLUMN {self.quote_identifier(col_name)}")

        if modify_columns:
            # PostgreSQL requires ALTER COLUMN ... TYPE ... for type changes
            for col in modify_columns:
                col_name = self.quote_identifier(col["name"])
                col_type = col["type"]
                nullable = col.get("nullable", False)
                clauses.append(f"ALTER COLUMN {col_name} TYPE {col_type}")
                if nullable:
                    clauses.append(f"ALTER COLUMN {col_name} DROP NOT NULL")
                else:
                    clauses.append(f"ALTER COLUMN {col_name} SET NOT NULL")

        return f"ALTER TABLE {table_name} {', '.join(clauses)}"

    def add_comment_sql(
        self,
        object_type: str,
        object_name: str,
        comment: str,
    ) -> str | None:
        """
        Generate COMMENT ON statement for PostgreSQL.

        Parameters
        ----------
        object_type : str
            'table' or 'column'.
        object_name : str
            Fully qualified object name.
        comment : str
            Comment text.

        Returns
        -------
        str
            COMMENT ON statement.
        """
        comment_type = object_type.upper()
        return f"COMMENT ON {comment_type} {object_name} IS {self.quote_string(comment)}"

    # =========================================================================
    # DML Generation
    # =========================================================================

    def insert_sql(
        self,
        table_name: str,
        columns: list[str],
        on_duplicate: str | None = None,
    ) -> str:
        """
        Generate INSERT statement for PostgreSQL.

        Parameters
        ----------
        table_name : str
            Table name.
        columns : list[str]
            Column names.
        on_duplicate : str, optional
            'ignore' or 'update' (PostgreSQL uses ON CONFLICT).

        Returns
        -------
        str
            INSERT SQL with placeholders.
        """
        cols = ", ".join(self.quote_identifier(col) for col in columns)
        placeholders = ", ".join([self.parameter_placeholder] * len(columns))

        base_insert = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

        if on_duplicate == "ignore":
            return f"{base_insert} ON CONFLICT DO NOTHING"
        elif on_duplicate == "update":
            # ON CONFLICT (pk_cols) DO UPDATE SET col=EXCLUDED.col
            # Caller must provide constraint name or columns
            updates = ", ".join(f"{self.quote_identifier(col)}=EXCLUDED.{self.quote_identifier(col)}" for col in columns)
            return f"{base_insert} ON CONFLICT DO UPDATE SET {updates}"
        else:
            return base_insert

    def update_sql(
        self,
        table_name: str,
        set_columns: list[str],
        where_columns: list[str],
    ) -> str:
        """Generate UPDATE statement for PostgreSQL."""
        set_clause = ", ".join(f"{self.quote_identifier(col)} = {self.parameter_placeholder}" for col in set_columns)
        where_clause = " AND ".join(f"{self.quote_identifier(col)} = {self.parameter_placeholder}" for col in where_columns)
        return f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

    def delete_sql(self, table_name: str) -> str:
        """Generate DELETE statement for PostgreSQL (WHERE added separately)."""
        return f"DELETE FROM {table_name}"

    # =========================================================================
    # Introspection
    # =========================================================================

    def list_schemas_sql(self) -> str:
        """Query to list all schemas in PostgreSQL."""
        return (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
        )

    def list_tables_sql(self, schema_name: str) -> str:
        """Query to list tables in a schema."""
        return (
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_type = 'BASE TABLE'"
        )

    def get_table_info_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get table metadata."""
        return (
            f"SELECT * FROM information_schema.tables "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)}"
        )

    def get_columns_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get column definitions."""
        return (
            f"SELECT column_name, data_type, is_nullable, column_default, "
            f"character_maximum_length, numeric_precision, numeric_scale "
            f"FROM information_schema.columns "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)} "
            f"ORDER BY ordinal_position"
        )

    def get_primary_key_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get primary key columns."""
        return (
            f"SELECT column_name FROM information_schema.key_column_usage "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)} "
            f"AND constraint_name IN ("
            f"  SELECT constraint_name FROM information_schema.table_constraints "
            f"  WHERE table_schema = {self.quote_string(schema_name)} "
            f"  AND table_name = {self.quote_string(table_name)} "
            f"  AND constraint_type = 'PRIMARY KEY'"
            f") "
            f"ORDER BY ordinal_position"
        )

    def get_foreign_keys_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get foreign key constraints."""
        return (
            f"SELECT kcu.constraint_name, kcu.column_name, "
            f"ccu.table_name AS foreign_table_name, ccu.column_name AS foreign_column_name "
            f"FROM information_schema.key_column_usage AS kcu "
            f"JOIN information_schema.constraint_column_usage AS ccu "
            f"  ON kcu.constraint_name = ccu.constraint_name "
            f"WHERE kcu.table_schema = {self.quote_string(schema_name)} "
            f"AND kcu.table_name = {self.quote_string(table_name)} "
            f"AND kcu.constraint_name IN ("
            f"  SELECT constraint_name FROM information_schema.table_constraints "
            f"  WHERE table_schema = {self.quote_string(schema_name)} "
            f"  AND table_name = {self.quote_string(table_name)} "
            f"  AND constraint_type = 'FOREIGN KEY'"
            f") "
            f"ORDER BY kcu.constraint_name, kcu.ordinal_position"
        )

    def get_indexes_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get index definitions."""
        return (
            f"SELECT indexname, indexdef FROM pg_indexes "
            f"WHERE schemaname = {self.quote_string(schema_name)} "
            f"AND tablename = {self.quote_string(table_name)}"
        )

    def parse_column_info(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Parse PostgreSQL column info into standardized format.

        Parameters
        ----------
        row : dict
            Row from information_schema.columns query.

        Returns
        -------
        dict
            Standardized column info with keys:
            name, type, nullable, default, comment
        """
        return {
            "name": row["column_name"],
            "type": row["data_type"],
            "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"],
            "comment": None,  # PostgreSQL stores comments separately
        }

    # =========================================================================
    # Transactions
    # =========================================================================

    def start_transaction_sql(self, isolation_level: str | None = None) -> str:
        """Generate BEGIN statement for PostgreSQL."""
        if isolation_level:
            return f"BEGIN ISOLATION LEVEL {isolation_level}"
        return "BEGIN"

    def commit_sql(self) -> str:
        """Generate COMMIT statement."""
        return "COMMIT"

    def rollback_sql(self) -> str:
        """Generate ROLLBACK statement."""
        return "ROLLBACK"

    # =========================================================================
    # Functions and Expressions
    # =========================================================================

    def current_timestamp_expr(self, precision: int | None = None) -> str:
        """
        CURRENT_TIMESTAMP expression for PostgreSQL.

        Parameters
        ----------
        precision : int, optional
            Fractional seconds precision (0-6).

        Returns
        -------
        str
            CURRENT_TIMESTAMP or CURRENT_TIMESTAMP(n).
        """
        if precision is not None:
            return f"CURRENT_TIMESTAMP({precision})"
        return "CURRENT_TIMESTAMP"

    def interval_expr(self, value: int, unit: str) -> str:
        """
        INTERVAL expression for PostgreSQL.

        Parameters
        ----------
        value : int
            Interval value.
        unit : str
            Time unit (singular: 'second', 'minute', 'hour', 'day').

        Returns
        -------
        str
            INTERVAL 'n units' (e.g., "INTERVAL '5 seconds'").
        """
        # PostgreSQL uses plural unit names and quotes
        unit_plural = unit.lower() + "s" if not unit.endswith("s") else unit.lower()
        return f"INTERVAL '{value} {unit_plural}'"

    # =========================================================================
    # Error Translation
    # =========================================================================

    def translate_error(self, error: Exception) -> Exception:
        """
        Translate PostgreSQL error to DataJoint exception.

        Parameters
        ----------
        error : Exception
            PostgreSQL exception (typically psycopg2 error).

        Returns
        -------
        Exception
            DataJoint exception or original error.
        """
        if not hasattr(error, "pgcode"):
            return error

        pgcode = error.pgcode

        # PostgreSQL error code mapping
        # Reference: https://www.postgresql.org/docs/current/errcodes-appendix.html
        match pgcode:
            # Integrity constraint violations
            case "23505":  # unique_violation
                return errors.DuplicateError(str(error))
            case "23503":  # foreign_key_violation
                return errors.IntegrityError(str(error))
            case "23502":  # not_null_violation
                return errors.MissingAttributeError(str(error))

            # Syntax errors
            case "42601":  # syntax_error
                return errors.QuerySyntaxError(str(error), "")

            # Undefined errors
            case "42P01":  # undefined_table
                return errors.MissingTableError(str(error), "")
            case "42703":  # undefined_column
                return errors.UnknownAttributeError(str(error))

            # Connection errors
            case "08006" | "08003" | "08000":  # connection_failure
                return errors.LostConnectionError(str(error))
            case "57P01":  # admin_shutdown
                return errors.LostConnectionError(str(error))

            # Access errors
            case "42501":  # insufficient_privilege
                return errors.AccessError("Insufficient privileges.", str(error), "")

            # All other errors pass through unchanged
            case _:
                return error

    # =========================================================================
    # Native Type Validation
    # =========================================================================

    def validate_native_type(self, type_str: str) -> bool:
        """
        Check if a native PostgreSQL type string is valid.

        Parameters
        ----------
        type_str : str
            Type string to validate.

        Returns
        -------
        bool
            True if valid PostgreSQL type.
        """
        type_lower = type_str.lower().strip()

        # PostgreSQL native types (simplified validation)
        valid_types = {
            # Integer types
            "smallint",
            "integer",
            "int",
            "bigint",
            "smallserial",
            "serial",
            "bigserial",
            # Floating point
            "real",
            "double precision",
            "numeric",
            "decimal",
            # String types
            "char",
            "varchar",
            "text",
            # Binary
            "bytea",
            # Boolean
            "boolean",
            "bool",
            # Temporal types
            "date",
            "time",
            "timetz",
            "timestamp",
            "timestamptz",
            "interval",
            # UUID
            "uuid",
            # JSON
            "json",
            "jsonb",
            # Network types
            "inet",
            "cidr",
            "macaddr",
            # Geometric types
            "point",
            "line",
            "lseg",
            "box",
            "path",
            "polygon",
            "circle",
            # Other
            "money",
            "xml",
        }

        # Extract base type (before parentheses or brackets)
        base_type = type_lower.split("(")[0].split("[")[0].strip()

        return base_type in valid_types

    # =========================================================================
    # PostgreSQL-Specific Enum Handling
    # =========================================================================

    def create_enum_type_sql(
        self,
        schema: str,
        table: str,
        column: str,
        values: list[str],
    ) -> str:
        """
        Generate CREATE TYPE statement for PostgreSQL enum.

        Parameters
        ----------
        schema : str
            Schema name.
        table : str
            Table name.
        column : str
            Column name.
        values : list[str]
            Enum values.

        Returns
        -------
        str
            CREATE TYPE ... AS ENUM statement.
        """
        type_name = f"{schema}_{table}_{column}_enum"
        quoted_values = ", ".join(self.quote_string(v) for v in values)
        return f"CREATE TYPE {self.quote_identifier(type_name)} AS ENUM ({quoted_values})"

    def drop_enum_type_sql(self, schema: str, table: str, column: str) -> str:
        """
        Generate DROP TYPE statement for PostgreSQL enum.

        Parameters
        ----------
        schema : str
            Schema name.
        table : str
            Table name.
        column : str
            Column name.

        Returns
        -------
        str
            DROP TYPE statement.
        """
        type_name = f"{schema}_{table}_{column}_enum"
        return f"DROP TYPE IF EXISTS {self.quote_identifier(type_name)} CASCADE"
