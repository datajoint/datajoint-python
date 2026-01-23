"""
PostgreSQL database adapter for DataJoint.

This module provides PostgreSQL-specific implementations for SQL generation,
type mapping, error translation, and connection management.
"""

from __future__ import annotations

import re
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
            - use_tls: bool or dict - DataJoint's SSL parameter (converted to sslmode)
            - connect_timeout: Connection timeout in seconds

        Returns
        -------
        psycopg2.connection
            PostgreSQL connection object.
        """
        dbname = kwargs.get("dbname", "postgres")  # Default to postgres database
        connect_timeout = kwargs.get("connect_timeout", 10)

        # Handle use_tls parameter (from DataJoint Connection)
        # Convert to PostgreSQL's sslmode
        use_tls = kwargs.get("use_tls")
        if "sslmode" in kwargs:
            # Explicit sslmode takes precedence
            sslmode = kwargs["sslmode"]
        elif use_tls is False:
            # use_tls=False → disable SSL
            sslmode = "disable"
        elif use_tls is True or isinstance(use_tls, dict):
            # use_tls=True or dict → require SSL
            sslmode = "require"
        else:
            # use_tls=None (default) → prefer SSL but allow fallback
            sslmode = "prefer"

        conn = client.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=dbname,
            sslmode=sslmode,
            connect_timeout=connect_timeout,
        )
        # DataJoint manages transactions explicitly via start_transaction()
        # Set autocommit=True to avoid implicit transactions
        conn.autocommit = True

        # Register numpy type adapters so numpy types can be used directly in queries
        self._register_numpy_adapters()

        return conn

    def _register_numpy_adapters(self) -> None:
        """
        Register psycopg2 adapters for numpy types.

        This allows numpy scalar types (bool_, int64, float64, etc.) to be used
        directly in queries without explicit conversion to Python native types.
        """
        try:
            import numpy as np
            from psycopg2.extensions import register_adapter, AsIs

            # Numpy bool type
            register_adapter(np.bool_, lambda x: AsIs(str(bool(x)).upper()))

            # Numpy integer types
            for np_type in (np.int8, np.int16, np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64):
                register_adapter(np_type, lambda x: AsIs(int(x)))

            # Numpy float types
            for np_ftype in (np.float16, np.float32, np.float64):
                register_adapter(np_ftype, lambda x: AsIs(repr(float(x))))

        except ImportError:
            pass  # numpy not available

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

    @property
    def backend(self) -> str:
        """Backend identifier: 'postgresql'."""
        return "postgresql"

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

    def get_master_table_name(self, part_table: str) -> str | None:
        """Extract master table name from part table (PostgreSQL double-quote format)."""
        import re

        # PostgreSQL format: "schema"."master__part"
        match = re.match(r'(?P<master>"\w+"."#?\w+)__\w+"', part_table)
        return match["master"] + '"' if match else None

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
            # PostgreSQL requires CREATE TYPE for enums
            # Extract enum values and generate a deterministic type name
            enum_match = re.match(r"enum\s*\((.+)\)", core_type, re.I)
            if enum_match:
                # Parse enum values: enum('M','F') -> ['M', 'F']
                values_str = enum_match.group(1)
                # Split by comma, handling quoted values
                values = [v.strip().strip("'\"") for v in values_str.split(",")]
                # Generate a deterministic type name based on values
                # Use a hash to keep name reasonable length
                import hashlib

                value_hash = hashlib.md5("_".join(sorted(values)).encode()).hexdigest()[:8]
                type_name = f"enum_{value_hash}"
                # Track this enum type for CREATE TYPE DDL
                if not hasattr(self, "_pending_enum_types"):
                    self._pending_enum_types = {}
                self._pending_enum_types[type_name] = values
                # Return schema-qualified type reference using placeholder
                # {database} will be replaced with actual schema name in table.py
                return '"{database}".' + self.quote_identifier(type_name)
            return "text"  # Fallback if parsing fails

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

    def upsert_on_duplicate_sql(
        self,
        table_name: str,
        columns: list[str],
        primary_key: list[str],
        num_rows: int,
    ) -> str:
        """Generate INSERT ... ON CONFLICT ... DO UPDATE statement for PostgreSQL."""
        # Build column list
        col_list = ", ".join(columns)

        # Build placeholders for VALUES
        placeholders = ", ".join(["(%s)" % ", ".join(["%s"] * len(columns))] * num_rows)

        # Build conflict target (primary key columns)
        conflict_cols = ", ".join(primary_key)

        # Build UPDATE clause (non-PK columns only)
        non_pk_columns = [col for col in columns if col not in primary_key]
        update_clauses = ", ".join(f"{col} = EXCLUDED.{col}" for col in non_pk_columns)

        return f"""
        INSERT INTO {table_name} ({col_list})
        VALUES {placeholders}
        ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_clauses}
        """

    def skip_duplicates_clause(
        self,
        full_table_name: str,
        primary_key: list[str],
    ) -> str:
        """
        Generate clause to skip duplicate key insertions for PostgreSQL.

        Uses ON CONFLICT (pk_cols) DO NOTHING to skip duplicates without
        raising an error.

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (with quotes). Unused but kept for
            API compatibility with MySQL adapter.
        primary_key : list[str]
            Primary key column names (unquoted).

        Returns
        -------
        str
            PostgreSQL ON CONFLICT DO NOTHING clause.
        """
        pk_cols = ", ".join(self.quote_identifier(pk) for pk in primary_key)
        return f" ON CONFLICT ({pk_cols}) DO NOTHING"

    @property
    def supports_inline_indexes(self) -> bool:
        """
        PostgreSQL does not support inline INDEX in CREATE TABLE.

        Returns False to indicate indexes must be created separately
        with CREATE INDEX statements.
        """
        return False

    @property
    def boolean_true_literal(self) -> str:
        """
        Return the SQL literal for boolean TRUE.

        PostgreSQL uses native boolean type with TRUE literal.

        Returns
        -------
        str
            SQL literal for boolean true value.
        """
        return "TRUE"

    # =========================================================================
    # Introspection
    # =========================================================================

    def list_schemas_sql(self) -> str:
        """Query to list all schemas in PostgreSQL."""
        return (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('pg_catalog', 'information_schema')"
        )

    def list_tables_sql(self, schema_name: str, pattern: str | None = None) -> str:
        """Query to list tables in a schema."""
        sql = (
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_type = 'BASE TABLE'"
        )
        if pattern:
            sql += f" AND table_name LIKE '{pattern}'"
        return sql

    def get_table_info_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get table metadata including table comment."""
        schema_str = self.quote_string(schema_name)
        table_str = self.quote_string(table_name)
        regclass_expr = f"({schema_str} || '.' || {table_str})::regclass"
        return (
            f"SELECT t.*, obj_description({regclass_expr}, 'pg_class') as table_comment "
            f"FROM information_schema.tables t "
            f"WHERE t.table_schema = {schema_str} "
            f"AND t.table_name = {table_str}"
        )

    def get_columns_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get column definitions including comments."""
        # Use col_description() to retrieve column comments stored via COMMENT ON COLUMN
        # The regclass cast allows using schema.table notation to get the OID
        schema_str = self.quote_string(schema_name)
        table_str = self.quote_string(table_name)
        regclass_expr = f"({schema_str} || '.' || {table_str})::regclass"
        return (
            f"SELECT c.column_name, c.data_type, c.udt_name, c.is_nullable, c.column_default, "
            f"c.character_maximum_length, c.numeric_precision, c.numeric_scale, "
            f"col_description({regclass_expr}, c.ordinal_position) as column_comment "
            f"FROM information_schema.columns c "
            f"WHERE c.table_schema = {schema_str} "
            f"AND c.table_name = {table_str} "
            f"ORDER BY c.ordinal_position"
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

    def get_constraint_info_sql(self, constraint_name: str, schema_name: str, table_name: str) -> str:
        """
        Query to get FK constraint details from information_schema.

        Returns matched pairs of (fk_column, parent_table, pk_column) for each
        column in the foreign key constraint, ordered by position.
        """
        return (
            "SELECT "
            "  kcu.column_name as fk_attrs, "
            "  '\"' || ccu.table_schema || '\".\"' || ccu.table_name || '\"' as parent, "
            "  ccu.column_name as pk_attrs "
            "FROM information_schema.key_column_usage AS kcu "
            "JOIN information_schema.referential_constraints AS rc "
            "  ON kcu.constraint_name = rc.constraint_name "
            "  AND kcu.constraint_schema = rc.constraint_schema "
            "JOIN information_schema.key_column_usage AS ccu "
            "  ON rc.unique_constraint_name = ccu.constraint_name "
            "  AND rc.unique_constraint_schema = ccu.constraint_schema "
            "  AND kcu.ordinal_position = ccu.ordinal_position "
            "WHERE kcu.constraint_name = %s "
            "  AND kcu.table_schema = %s "
            "  AND kcu.table_name = %s "
            "ORDER BY kcu.ordinal_position"
        )

    def parse_foreign_key_error(self, error_message: str) -> dict[str, str | list[str] | None] | None:
        """
        Parse PostgreSQL foreign key violation error message.

        PostgreSQL FK error format:
        'update or delete on table "X" violates foreign key constraint "Y" on table "Z"'
        Where:
        - "X" is the referenced table (being deleted/updated)
        - "Z" is the referencing table (has the FK, needs cascade delete)
        """
        import re

        pattern = re.compile(
            r'.*table "(?P<referenced_table>[^"]+)" violates foreign key constraint '
            r'"(?P<name>[^"]+)" on table "(?P<referencing_table>[^"]+)"'
        )

        match = pattern.match(error_message)
        if not match:
            return None

        result = match.groupdict()

        # The child is the referencing table (the one with the FK that needs cascade delete)
        # The parent is the referenced table (the one being deleted)
        # The error doesn't include schema, so we return unqualified names
        child = f'"{result["referencing_table"]}"'
        parent = f'"{result["referenced_table"]}"'

        return {
            "child": child,
            "name": f'"{result["name"]}"',
            "fk_attrs": None,  # Not in error message, will need constraint query
            "parent": parent,
            "pk_attrs": None,  # Not in error message, will need constraint query
        }

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
            Row from information_schema.columns query with col_description() join.

        Returns
        -------
        dict
            Standardized column info with keys:
            name, type, nullable, default, comment, key, extra
        """
        # For user-defined types (enums), use udt_name instead of data_type
        # PostgreSQL reports enums as "USER-DEFINED" in data_type
        data_type = row["data_type"]
        if data_type == "USER-DEFINED":
            data_type = row["udt_name"]

        # Reconstruct parametrized types that PostgreSQL splits into separate fields
        char_max_len = row.get("character_maximum_length")
        num_precision = row.get("numeric_precision")
        num_scale = row.get("numeric_scale")

        if data_type == "character" and char_max_len is not None:
            # char(n) - PostgreSQL reports as "character" with length in separate field
            data_type = f"char({char_max_len})"
        elif data_type == "character varying" and char_max_len is not None:
            # varchar(n)
            data_type = f"varchar({char_max_len})"
        elif data_type == "numeric" and num_precision is not None:
            # numeric(p,s) - reconstruct decimal type
            if num_scale is not None and num_scale > 0:
                data_type = f"decimal({num_precision},{num_scale})"
            else:
                data_type = f"decimal({num_precision})"

        return {
            "name": row["column_name"],
            "type": data_type,
            "nullable": row["is_nullable"] == "YES",
            "default": row["column_default"],
            "comment": row.get("column_comment"),  # Retrieved via col_description()
            "key": "",  # PostgreSQL key info retrieved separately
            "extra": "",  # PostgreSQL doesn't have auto_increment in same way
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

    def current_user_expr(self) -> str:
        """PostgreSQL current user expression."""
        return "current_user"

    def json_path_expr(self, column: str, path: str, return_type: str | None = None) -> str:
        """
        Generate PostgreSQL jsonb_extract_path_text() expression.

        Parameters
        ----------
        column : str
            Column name containing JSON data.
        path : str
            JSON path (e.g., 'field' or 'nested.field').
        return_type : str, optional
            Return type specification for casting (e.g., 'float', 'decimal(10,2)').

        Returns
        -------
        str
            PostgreSQL jsonb_extract_path_text() expression, with optional cast.

        Examples
        --------
        >>> adapter.json_path_expr('data', 'field')
        'jsonb_extract_path_text("data", \\'field\\')'
        >>> adapter.json_path_expr('data', 'nested.field')
        'jsonb_extract_path_text("data", \\'nested\\', \\'field\\')'
        >>> adapter.json_path_expr('data', 'value', 'float')
        'jsonb_extract_path_text("data", \\'value\\')::float'
        """
        quoted_col = self.quote_identifier(column)
        # Split path by '.' for nested access, handling array notation
        path_parts = []
        for part in path.split("."):
            # Handle array access like field[0]
            if "[" in part:
                base, rest = part.split("[", 1)
                path_parts.append(base)
                # Extract array indices
                indices = rest.rstrip("]").split("][")
                path_parts.extend(indices)
            else:
                path_parts.append(part)
        path_args = ", ".join(f"'{part}'" for part in path_parts)
        expr = f"jsonb_extract_path_text({quoted_col}, {path_args})"
        # Add cast if return type specified
        if return_type:
            # Map DataJoint types to PostgreSQL types
            pg_type = return_type.lower()
            if pg_type in ("unsigned", "signed"):
                pg_type = "integer"
            elif pg_type == "double":
                pg_type = "double precision"
            expr = f"({expr})::{pg_type}"
        return expr

    def translate_expression(self, expr: str) -> str:
        """
        Translate SQL expression for PostgreSQL compatibility.

        Converts MySQL-specific functions to PostgreSQL equivalents:
        - GROUP_CONCAT(col) → STRING_AGG(col::text, ',')
        - GROUP_CONCAT(col SEPARATOR 'sep') → STRING_AGG(col::text, 'sep')

        Parameters
        ----------
        expr : str
            SQL expression that may contain function calls.

        Returns
        -------
        str
            Translated expression for PostgreSQL.
        """
        import re

        # GROUP_CONCAT(col) → STRING_AGG(col::text, ',')
        # GROUP_CONCAT(col SEPARATOR 'sep') → STRING_AGG(col::text, 'sep')
        def replace_group_concat(match):
            inner = match.group(1).strip()
            # Check for SEPARATOR clause
            sep_match = re.match(r"(.+?)\s+SEPARATOR\s+(['\"])(.+?)\2", inner, re.IGNORECASE)
            if sep_match:
                col = sep_match.group(1).strip()
                sep = sep_match.group(3)
                return f"STRING_AGG({col}::text, '{sep}')"
            else:
                return f"STRING_AGG({inner}::text, ',')"

        expr = re.sub(r"GROUP_CONCAT\s*\((.+?)\)", replace_group_concat, expr, flags=re.IGNORECASE)

        # Replace simple functions FIRST before complex patterns
        # CURDATE() → CURRENT_DATE
        expr = re.sub(r"CURDATE\s*\(\s*\)", "CURRENT_DATE", expr, flags=re.IGNORECASE)

        # NOW() → CURRENT_TIMESTAMP
        expr = re.sub(r"\bNOW\s*\(\s*\)", "CURRENT_TIMESTAMP", expr, flags=re.IGNORECASE)

        # YEAR(date) → EXTRACT(YEAR FROM date)::int
        expr = re.sub(r"\bYEAR\s*\(\s*([^)]+)\s*\)", r"EXTRACT(YEAR FROM \1)::int", expr, flags=re.IGNORECASE)

        # MONTH(date) → EXTRACT(MONTH FROM date)::int
        expr = re.sub(r"\bMONTH\s*\(\s*([^)]+)\s*\)", r"EXTRACT(MONTH FROM \1)::int", expr, flags=re.IGNORECASE)

        # DAY(date) → EXTRACT(DAY FROM date)::int
        expr = re.sub(r"\bDAY\s*\(\s*([^)]+)\s*\)", r"EXTRACT(DAY FROM \1)::int", expr, flags=re.IGNORECASE)

        # TIMESTAMPDIFF(YEAR, d1, d2) → EXTRACT(YEAR FROM AGE(d2, d1))::int
        # Use a more robust regex that handles the comma-separated arguments
        def replace_timestampdiff(match):
            unit = match.group(1).upper()
            date1 = match.group(2).strip()
            date2 = match.group(3).strip()
            if unit == "YEAR":
                return f"EXTRACT(YEAR FROM AGE({date2}, {date1}))::int"
            elif unit == "MONTH":
                return f"(EXTRACT(YEAR FROM AGE({date2}, {date1})) * 12 + EXTRACT(MONTH FROM AGE({date2}, {date1})))::int"
            elif unit == "DAY":
                return f"({date2}::date - {date1}::date)"
            else:
                return f"EXTRACT({unit} FROM AGE({date2}, {date1}))::int"

        # Match TIMESTAMPDIFF with proper argument parsing
        # The arguments are: unit, date1, date2 - we need to handle identifiers and CURRENT_DATE
        expr = re.sub(
            r"TIMESTAMPDIFF\s*\(\s*(\w+)\s*,\s*([^,]+)\s*,\s*([^)]+)\s*\)",
            replace_timestampdiff,
            expr,
            flags=re.IGNORECASE,
        )

        # SUM(expr='value') → SUM((expr='value')::int) for PostgreSQL boolean handling
        # This handles patterns like SUM(sex='F') which produce boolean in PostgreSQL
        def replace_sum_comparison(match):
            inner = match.group(1).strip()
            # Check if inner contains a comparison operator
            if re.search(r"[=<>!]", inner) and not inner.startswith("("):
                return f"SUM(({inner})::int)"
            return match.group(0)  # Return unchanged if no comparison

        expr = re.sub(r"\bSUM\s*\(\s*([^)]+)\s*\)", replace_sum_comparison, expr, flags=re.IGNORECASE)

        return expr

    # =========================================================================
    # DDL Generation
    # =========================================================================

    def format_column_definition(
        self,
        name: str,
        sql_type: str,
        nullable: bool = False,
        default: str | None = None,
        comment: str | None = None,
    ) -> str:
        """
        Format a column definition for PostgreSQL DDL.

        Examples
        --------
        >>> adapter.format_column_definition('user_id', 'bigint', nullable=False, comment='user ID')
        '"user_id" bigint NOT NULL'
        """
        parts = [self.quote_identifier(name), sql_type]
        if default:
            parts.append(default)
        elif not nullable:
            parts.append("NOT NULL")
        # Note: PostgreSQL comments handled separately via COMMENT ON
        return " ".join(parts)

    def table_options_clause(self, comment: str | None = None) -> str:
        """
        Generate PostgreSQL table options clause (empty - no ENGINE in PostgreSQL).

        Examples
        --------
        >>> adapter.table_options_clause('test table')
        ''
        >>> adapter.table_options_clause()
        ''
        """
        return ""  # PostgreSQL uses COMMENT ON TABLE separately

    def table_comment_ddl(self, full_table_name: str, comment: str) -> str | None:
        """
        Generate COMMENT ON TABLE statement for PostgreSQL.

        Examples
        --------
        >>> adapter.table_comment_ddl('"schema"."table"', 'test comment')
        'COMMENT ON TABLE "schema"."table" IS \\'test comment\\''
        """
        # Escape single quotes by doubling them
        escaped_comment = comment.replace("'", "''")
        return f"COMMENT ON TABLE {full_table_name} IS '{escaped_comment}'"

    def column_comment_ddl(self, full_table_name: str, column_name: str, comment: str) -> str | None:
        """
        Generate COMMENT ON COLUMN statement for PostgreSQL.

        Examples
        --------
        >>> adapter.column_comment_ddl('"schema"."table"', 'column', 'test comment')
        'COMMENT ON COLUMN "schema"."table"."column" IS \\'test comment\\''
        """
        quoted_col = self.quote_identifier(column_name)
        # Escape single quotes by doubling them (PostgreSQL string literal syntax)
        escaped_comment = comment.replace("'", "''")
        return f"COMMENT ON COLUMN {full_table_name}.{quoted_col} IS '{escaped_comment}'"

    def enum_type_ddl(self, type_name: str, values: list[str]) -> str | None:
        """
        Generate CREATE TYPE statement for PostgreSQL enum.

        Examples
        --------
        >>> adapter.enum_type_ddl('status_type', ['active', 'inactive'])
        'CREATE TYPE "status_type" AS ENUM (\\'active\\', \\'inactive\\')'
        """
        quoted_values = ", ".join(f"'{v}'" for v in values)
        return f"CREATE TYPE {self.quote_identifier(type_name)} AS ENUM ({quoted_values})"

    def get_pending_enum_ddl(self, schema_name: str) -> list[str]:
        """
        Get DDL statements for pending enum types and clear the pending list.

        PostgreSQL requires CREATE TYPE statements before using enum types in
        column definitions. This method returns DDL for enum types accumulated
        during type conversion and clears the pending list.

        Parameters
        ----------
        schema_name : str
            Schema name to qualify enum type names.

        Returns
        -------
        list[str]
            List of CREATE TYPE statements (if any pending).
        """
        ddl_statements = []
        if hasattr(self, "_pending_enum_types") and self._pending_enum_types:
            for type_name, values in self._pending_enum_types.items():
                # Generate CREATE TYPE with schema qualification
                quoted_type = f"{self.quote_identifier(schema_name)}.{self.quote_identifier(type_name)}"
                quoted_values = ", ".join(f"'{v}'" for v in values)
                ddl_statements.append(f"CREATE TYPE {quoted_type} AS ENUM ({quoted_values})")
            self._pending_enum_types = {}
        return ddl_statements

    def job_metadata_columns(self) -> list[str]:
        """
        Return PostgreSQL-specific job metadata column definitions.

        Examples
        --------
        >>> adapter.job_metadata_columns()
        ['"_job_start_time" timestamp DEFAULT NULL',
         '"_job_duration" real DEFAULT NULL',
         '"_job_version" varchar(64) DEFAULT \\'\\'']
        """
        return [
            '"_job_start_time" timestamp DEFAULT NULL',
            '"_job_duration" real DEFAULT NULL',
            "\"_job_version\" varchar(64) DEFAULT ''",
        ]

    # =========================================================================
    # Error Translation
    # =========================================================================

    def translate_error(self, error: Exception, query: str = "") -> Exception:
        """
        Translate PostgreSQL error to DataJoint exception.

        Parameters
        ----------
        error : Exception
            PostgreSQL exception (typically psycopg2 error).
        query : str, optional
            SQL query that caused the error (for context).

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

    def get_table_enum_types_sql(self, schema_name: str, table_name: str) -> str:
        """
        Query to get enum types used by a table's columns.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        str
            SQL query that returns enum type names (schema-qualified).
        """
        return f"""
            SELECT DISTINCT
                n.nspname || '.' || t.typname as enum_type
            FROM pg_catalog.pg_type t
            JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_catalog.pg_attribute a ON a.atttypid = t.oid
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace cn ON cn.oid = c.relnamespace
            WHERE t.typtype = 'e'
            AND cn.nspname = {self.quote_string(schema_name)}
            AND c.relname = {self.quote_string(table_name)}
        """

    def drop_enum_types_for_table(self, schema_name: str, table_name: str) -> list[str]:
        """
        Generate DROP TYPE statements for all enum types used by a table.

        Parameters
        ----------
        schema_name : str
            Schema name.
        table_name : str
            Table name.

        Returns
        -------
        list[str]
            List of DROP TYPE IF EXISTS statements.
        """
        # Returns list of DDL statements - caller should execute query first
        # to get actual enum types, then call this with results
        return []  # Placeholder - actual implementation requires query execution

    def drop_enum_type_ddl(self, enum_type_name: str) -> str:
        """
        Generate DROP TYPE IF EXISTS statement for a PostgreSQL enum.

        Parameters
        ----------
        enum_type_name : str
            Fully qualified enum type name (schema.typename).

        Returns
        -------
        str
            DROP TYPE IF EXISTS statement with CASCADE.
        """
        # Split schema.typename and quote each part
        parts = enum_type_name.split(".")
        if len(parts) == 2:
            qualified_name = f"{self.quote_identifier(parts[0])}.{self.quote_identifier(parts[1])}"
        else:
            qualified_name = self.quote_identifier(enum_type_name)
        return f"DROP TYPE IF EXISTS {qualified_name} CASCADE"
