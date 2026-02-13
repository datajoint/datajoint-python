"""
MySQL database adapter for DataJoint.

This module provides MySQL-specific implementations for SQL generation,
type mapping, error translation, and connection management.
"""

from __future__ import annotations

from typing import Any

import pymysql as client

from .. import errors
from .base import DatabaseAdapter

# Core type mapping: DataJoint core types → MySQL types
CORE_TYPE_MAP = {
    "int64": "bigint",
    "int32": "int",
    "int16": "smallint",
    "int8": "tinyint",
    "float32": "float",
    "float64": "double",
    "bool": "tinyint",
    "uuid": "binary(16)",
    "bytes": "longblob",
    "json": "json",
    "date": "date",
    # datetime, char, varchar, decimal, enum require parameters - handled in method
}

# Reverse mapping: MySQL types → DataJoint core types (for introspection)
SQL_TO_CORE_MAP = {
    "bigint": "int64",
    "int": "int32",
    "smallint": "int16",
    "tinyint": "int8",  # Could be bool, need context
    "float": "float32",
    "double": "float64",
    "binary(16)": "uuid",
    "longblob": "bytes",
    "json": "json",
    "date": "date",
}


class MySQLAdapter(DatabaseAdapter):
    """MySQL database adapter implementation."""

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
        Establish MySQL connection.

        Parameters
        ----------
        host : str
            MySQL server hostname.
        port : int
            MySQL server port.
        user : str
            Username for authentication.
        password : str
            Password for authentication.
        **kwargs : Any
            Additional MySQL-specific parameters:
            - ssl: TLS/SSL configuration dict (deprecated, use use_tls)
            - use_tls: bool or dict - DataJoint's SSL parameter (preferred)
            - charset: Character set (default from kwargs)

        Returns
        -------
        pymysql.Connection
            MySQL connection object.
        """
        # Handle both ssl (old) and use_tls (new) parameter names
        ssl_config = kwargs.get("use_tls", kwargs.get("ssl"))
        # Convert boolean True to dict for PyMySQL (PyMySQL expects dict or SSLContext)
        if ssl_config is True:
            ssl_config = {}  # Enable SSL with default settings
        charset = kwargs.get("charset", "")

        # Prepare connection parameters
        conn_params = {
            "host": host,
            "port": port,
            "user": user,
            "passwd": password,
            "sql_mode": "NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO,"
            "STRICT_ALL_TABLES,NO_ENGINE_SUBSTITUTION,ONLY_FULL_GROUP_BY",
            "charset": charset,
            "autocommit": True,  # DataJoint manages transactions explicitly
        }

        # Handle SSL configuration
        if ssl_config is False:
            # Explicitly disable SSL
            conn_params["ssl_disabled"] = True
        elif ssl_config is not None:
            # Enable SSL with config dict (can be empty for defaults)
            conn_params["ssl"] = ssl_config
            # Explicitly enable SSL by setting ssl_disabled=False
            conn_params["ssl_disabled"] = False

        return client.connect(**conn_params)

    def close(self, connection: Any) -> None:
        """Close the MySQL connection."""
        connection.close()

    def ping(self, connection: Any) -> bool:
        """
        Check if MySQL connection is alive.

        Returns
        -------
        bool
            True if connection is alive.
        """
        try:
            connection.ping(reconnect=False)
            return True
        except Exception:
            return False

    def get_connection_id(self, connection: Any) -> int:
        """
        Get MySQL connection ID.

        Returns
        -------
        int
            MySQL connection_id().
        """
        cursor = connection.cursor()
        cursor.execute("SELECT connection_id()")
        return cursor.fetchone()[0]

    @property
    def default_port(self) -> int:
        """MySQL default port 3306."""
        return 3306

    @property
    def backend(self) -> str:
        """Backend identifier: 'mysql'."""
        return "mysql"

    def get_cursor(self, connection: Any, as_dict: bool = False) -> Any:
        """
        Get a cursor from MySQL connection.

        Parameters
        ----------
        connection : Any
            pymysql connection object.
        as_dict : bool, optional
            If True, return DictCursor that yields rows as dictionaries.
            If False, return standard Cursor that yields rows as tuples.
            Default False.

        Returns
        -------
        Any
            pymysql cursor object.
        """
        import pymysql

        cursor_class = pymysql.cursors.DictCursor if as_dict else pymysql.cursors.Cursor
        return connection.cursor(cursor=cursor_class)

    # =========================================================================
    # SQL Syntax
    # =========================================================================

    def quote_identifier(self, name: str) -> str:
        """
        Quote identifier with backticks for MySQL.

        Parameters
        ----------
        name : str
            Identifier to quote.

        Returns
        -------
        str
            Backtick-quoted identifier: `name`
        """
        return f"`{name}`"

    def quote_string(self, value: str) -> str:
        """
        Quote string literal for MySQL with escaping.

        Parameters
        ----------
        value : str
            String value to quote.

        Returns
        -------
        str
            Quoted and escaped string literal.
        """
        # Use pymysql's escape_string for proper escaping
        escaped = client.converters.escape_string(value)
        return f"'{escaped}'"

    def get_master_table_name(self, part_table: str) -> str | None:
        """Extract master table name from part table (MySQL backtick format)."""
        import re

        # MySQL format: `schema`.`master__part`
        match = re.match(r"(?P<master>`\w+`.`#?\w+)__\w+`", part_table)
        return match["master"] + "`" if match else None

    @property
    def parameter_placeholder(self) -> str:
        """MySQL/pymysql uses %s placeholders."""
        return "%s"

    # =========================================================================
    # Type Mapping
    # =========================================================================

    def core_type_to_sql(self, core_type: str) -> str:
        """
        Convert DataJoint core type to MySQL type.

        Parameters
        ----------
        core_type : str
            DataJoint core type, possibly with parameters:
            - int64, float32, bool, uuid, bytes, json, date
            - datetime or datetime(n)
            - char(n), varchar(n)
            - decimal(p,s)
            - enum('a','b','c')

        Returns
        -------
        str
            MySQL SQL type.

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
            # datetime or datetime(precision)
            return core_type  # MySQL supports datetime(n) directly

        if core_type.startswith("char("):
            # char(n)
            return core_type

        if core_type.startswith("varchar("):
            # varchar(n)
            return core_type

        if core_type.startswith("decimal("):
            # decimal(precision, scale)
            return core_type

        if core_type.startswith("enum("):
            # enum('value1', 'value2', ...)
            return core_type

        raise ValueError(f"Unknown core type: {core_type}")

    def sql_type_to_core(self, sql_type: str) -> str | None:
        """
        Convert MySQL type to DataJoint core type (if mappable).

        Parameters
        ----------
        sql_type : str
            MySQL SQL type.

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
        if sql_type_lower.startswith("datetime"):
            return sql_type  # Keep precision

        if sql_type_lower.startswith("char("):
            return sql_type  # Keep size

        if sql_type_lower.startswith("varchar("):
            return sql_type  # Keep size

        if sql_type_lower.startswith("decimal("):
            return sql_type  # Keep precision/scale

        if sql_type_lower.startswith("enum("):
            return sql_type  # Keep values

        # Not a mappable core type
        return None

    # =========================================================================
    # DDL Generation
    # =========================================================================

    def create_schema_sql(self, schema_name: str) -> str:
        """
        Generate CREATE DATABASE statement for MySQL.

        Parameters
        ----------
        schema_name : str
            Database name.

        Returns
        -------
        str
            CREATE DATABASE SQL.
        """
        return f"CREATE DATABASE {self.quote_identifier(schema_name)}"

    def drop_schema_sql(self, schema_name: str, if_exists: bool = True) -> str:
        """
        Generate DROP DATABASE statement for MySQL.

        Parameters
        ----------
        schema_name : str
            Database name.
        if_exists : bool
            Include IF EXISTS clause.

        Returns
        -------
        str
            DROP DATABASE SQL.
        """
        if_exists_clause = "IF EXISTS " if if_exists else ""
        return f"DROP DATABASE {if_exists_clause}{self.quote_identifier(schema_name)}"

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
        Generate CREATE TABLE statement for MySQL.

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
            Table comment.

        Returns
        -------
        str
            CREATE TABLE SQL statement.
        """
        lines = []

        # Column definitions
        for col in columns:
            col_name = self.quote_identifier(col["name"])
            col_type = col["type"]
            nullable = "NULL" if col.get("nullable", False) else "NOT NULL"
            default = f" DEFAULT {col['default']}" if "default" in col else ""
            col_comment = f" COMMENT {self.quote_string(col['comment'])}" if "comment" in col else ""
            lines.append(f"{col_name} {col_type} {nullable}{default}{col_comment}")

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

        # Indexes
        for idx in indexes:
            unique = "UNIQUE " if idx.get("unique", False) else ""
            idx_cols = ", ".join(self.quote_identifier(col) for col in idx["columns"])
            lines.append(f"{unique}INDEX ({idx_cols})")

        # Assemble CREATE TABLE
        table_def = ",\n  ".join(lines)
        comment_clause = f" COMMENT={self.quote_string(comment)}" if comment else ""
        return f"CREATE TABLE IF NOT EXISTS {table_name} (\n  {table_def}\n) ENGINE=InnoDB{comment_clause}"

    def drop_table_sql(self, table_name: str, if_exists: bool = True) -> str:
        """Generate DROP TABLE statement for MySQL."""
        if_exists_clause = "IF EXISTS " if if_exists else ""
        return f"DROP TABLE {if_exists_clause}{table_name}"

    def alter_table_sql(
        self,
        table_name: str,
        add_columns: list[dict[str, Any]] | None = None,
        drop_columns: list[str] | None = None,
        modify_columns: list[dict[str, Any]] | None = None,
    ) -> str:
        """
        Generate ALTER TABLE statement for MySQL.

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
                clauses.append(f"ADD {col_name} {col_type} {nullable}")

        if drop_columns:
            for col_name in drop_columns:
                clauses.append(f"DROP {self.quote_identifier(col_name)}")

        if modify_columns:
            for col in modify_columns:
                col_name = self.quote_identifier(col["name"])
                col_type = col["type"]
                nullable = "NULL" if col.get("nullable", False) else "NOT NULL"
                clauses.append(f"MODIFY {col_name} {col_type} {nullable}")

        return f"ALTER TABLE {table_name} {', '.join(clauses)}"

    def add_comment_sql(
        self,
        object_type: str,
        object_name: str,
        comment: str,
    ) -> str | None:
        """
        MySQL embeds comments in CREATE/ALTER, not separate statements.

        Returns None since comments are inline.
        """
        return None

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
        Generate INSERT statement for MySQL.

        Parameters
        ----------
        table_name : str
            Table name.
        columns : list[str]
            Column names.
        on_duplicate : str, optional
            'ignore', 'replace', or 'update'.

        Returns
        -------
        str
            INSERT SQL with placeholders.
        """
        cols = ", ".join(self.quote_identifier(col) for col in columns)
        placeholders = ", ".join([self.parameter_placeholder] * len(columns))

        if on_duplicate == "ignore":
            return f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({placeholders})"
        elif on_duplicate == "replace":
            return f"REPLACE INTO {table_name} ({cols}) VALUES ({placeholders})"
        elif on_duplicate == "update":
            # ON DUPLICATE KEY UPDATE col=VALUES(col)
            updates = ", ".join(f"{self.quote_identifier(col)}=VALUES({self.quote_identifier(col)})" for col in columns)
            return f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
        else:
            return f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

    def update_sql(
        self,
        table_name: str,
        set_columns: list[str],
        where_columns: list[str],
    ) -> str:
        """Generate UPDATE statement for MySQL."""
        set_clause = ", ".join(f"{self.quote_identifier(col)} = {self.parameter_placeholder}" for col in set_columns)
        where_clause = " AND ".join(f"{self.quote_identifier(col)} = {self.parameter_placeholder}" for col in where_columns)
        return f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

    def delete_sql(self, table_name: str) -> str:
        """Generate DELETE statement for MySQL (WHERE added separately)."""
        return f"DELETE FROM {table_name}"

    def upsert_on_duplicate_sql(
        self,
        table_name: str,
        columns: list[str],
        primary_key: list[str],
        num_rows: int,
    ) -> str:
        """Generate INSERT ... ON DUPLICATE KEY UPDATE statement for MySQL."""
        # Build column list
        col_list = ", ".join(columns)

        # Build placeholders for VALUES
        placeholders = ", ".join(["(%s)" % ", ".join(["%s"] * len(columns))] * num_rows)

        # Build UPDATE clause (all columns)
        update_clauses = ", ".join(f"{col} = VALUES({col})" for col in columns)

        return f"""
        INSERT INTO {table_name} ({col_list})
        VALUES {placeholders}
        ON DUPLICATE KEY UPDATE {update_clauses}
        """

    def skip_duplicates_clause(
        self,
        full_table_name: str,
        primary_key: list[str],
    ) -> str:
        """
        Generate clause to skip duplicate key insertions for MySQL.

        Uses ON DUPLICATE KEY UPDATE with a no-op update (pk=pk) to effectively
        skip duplicates without raising an error.

        Parameters
        ----------
        full_table_name : str
            Fully qualified table name (with quotes).
        primary_key : list[str]
            Primary key column names (unquoted).

        Returns
        -------
        str
            MySQL ON DUPLICATE KEY UPDATE clause.
        """
        quoted_pk = self.quote_identifier(primary_key[0])
        return f" ON DUPLICATE KEY UPDATE {quoted_pk}={full_table_name}.{quoted_pk}"

    # =========================================================================
    # Introspection
    # =========================================================================

    def list_schemas_sql(self) -> str:
        """Query to list all databases in MySQL."""
        return "SELECT schema_name FROM information_schema.schemata"

    def list_tables_sql(self, schema_name: str, pattern: str | None = None) -> str:
        """Query to list tables in a database."""
        sql = f"SHOW TABLES IN {self.quote_identifier(schema_name)}"
        if pattern:
            sql += f" LIKE '{pattern}'"
        return sql

    def get_table_info_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get table metadata (comment, engine, etc.)."""
        return (
            f"SELECT * FROM information_schema.tables "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)}"
        )

    def get_columns_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get column definitions."""
        return f"SHOW FULL COLUMNS FROM {self.quote_identifier(table_name)} IN {self.quote_identifier(schema_name)}"

    def get_primary_key_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get primary key columns."""
        return (
            f"SELECT COLUMN_NAME as column_name FROM information_schema.key_column_usage "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)} "
            f"AND constraint_name = 'PRIMARY' "
            f"ORDER BY ordinal_position"
        )

    def get_foreign_keys_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get foreign key constraints."""
        return (
            f"SELECT CONSTRAINT_NAME as constraint_name, COLUMN_NAME as column_name, "
            f"REFERENCED_TABLE_NAME as referenced_table_name, REFERENCED_COLUMN_NAME as referenced_column_name "
            f"FROM information_schema.key_column_usage "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)} "
            f"AND referenced_table_name IS NOT NULL "
            f"ORDER BY constraint_name, ordinal_position"
        )

    def get_constraint_info_sql(self, constraint_name: str, schema_name: str, table_name: str) -> str:
        """Query to get FK constraint details from information_schema."""
        return (
            "SELECT "
            "  COLUMN_NAME as fk_attrs, "
            "  CONCAT('`', REFERENCED_TABLE_SCHEMA, '`.`', REFERENCED_TABLE_NAME, '`') as parent, "
            "  REFERENCED_COLUMN_NAME as pk_attrs "
            "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
            "WHERE CONSTRAINT_NAME = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s"
        )

    def parse_foreign_key_error(self, error_message: str) -> dict[str, str | list[str] | None] | None:
        """Parse MySQL foreign key violation error message."""
        import re

        # MySQL FK error pattern with backticks
        pattern = re.compile(
            r"[\w\s:]*\((?P<child>`[^`]+`.`[^`]+`), "
            r"CONSTRAINT (?P<name>`[^`]+`) "
            r"(FOREIGN KEY \((?P<fk_attrs>[^)]+)\) "
            r"REFERENCES (?P<parent>`[^`]+`(\.`[^`]+`)?) \((?P<pk_attrs>[^)]+)\)[\s\w]+\))?"
        )

        match = pattern.match(error_message)
        if not match:
            return None

        result = match.groupdict()

        # Parse comma-separated FK attrs if present
        if result.get("fk_attrs"):
            result["fk_attrs"] = [col.strip("`") for col in result["fk_attrs"].split(",")]
        # Parse comma-separated PK attrs if present
        if result.get("pk_attrs"):
            result["pk_attrs"] = [col.strip("`") for col in result["pk_attrs"].split(",")]

        return result

    def get_indexes_sql(self, schema_name: str, table_name: str) -> str:
        """Query to get index definitions.

        Note: For MySQL 8.0+, EXPRESSION column contains the expression for
        functional indexes. COLUMN_NAME is NULL for such indexes.
        """
        return (
            f"SELECT INDEX_NAME as index_name, "
            f"COALESCE(COLUMN_NAME, CONCAT('(', EXPRESSION, ')')) as column_name, "
            f"NON_UNIQUE as non_unique, SEQ_IN_INDEX as seq_in_index "
            f"FROM information_schema.statistics "
            f"WHERE table_schema = {self.quote_string(schema_name)} "
            f"AND table_name = {self.quote_string(table_name)} "
            f"AND index_name != 'PRIMARY' "
            f"ORDER BY index_name, seq_in_index"
        )

    def parse_column_info(self, row: dict[str, Any]) -> dict[str, Any]:
        """
        Parse MySQL SHOW FULL COLUMNS output into standardized format.

        Parameters
        ----------
        row : dict
            Row from SHOW FULL COLUMNS query.

        Returns
        -------
        dict
            Standardized column info with keys:
            name, type, nullable, default, comment, key, extra
        """
        return {
            "name": row["Field"],
            "type": row["Type"],
            "nullable": row["Null"] == "YES",
            "default": row["Default"],
            "comment": row["Comment"],
            "key": row["Key"],  # PRI, UNI, MUL
            "extra": row["Extra"],  # auto_increment, etc.
        }

    # =========================================================================
    # Transactions
    # =========================================================================

    def start_transaction_sql(self, isolation_level: str | None = None) -> str:
        """Generate START TRANSACTION statement."""
        if isolation_level:
            return f"START TRANSACTION WITH CONSISTENT SNAPSHOT, {isolation_level}"
        return "START TRANSACTION WITH CONSISTENT SNAPSHOT"

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
        CURRENT_TIMESTAMP expression for MySQL.

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
        INTERVAL expression for MySQL.

        Parameters
        ----------
        value : int
            Interval value.
        unit : str
            Time unit (singular: 'second', 'minute', 'hour', 'day').

        Returns
        -------
        str
            INTERVAL n UNIT (e.g., 'INTERVAL 5 SECOND').
        """
        # MySQL uses singular unit names
        return f"INTERVAL {value} {unit.upper()}"

    def current_user_expr(self) -> str:
        """MySQL current user expression."""
        return "user()"

    def json_path_expr(self, column: str, path: str, return_type: str | None = None) -> str:
        """
        Generate MySQL json_value() expression.

        Parameters
        ----------
        column : str
            Column name containing JSON data.
        path : str
            JSON path (e.g., 'field' or 'nested.field').
        return_type : str, optional
            Return type specification (e.g., 'decimal(10,2)').

        Returns
        -------
        str
            MySQL json_value() expression.

        Examples
        --------
        >>> adapter.json_path_expr('data', 'field')
        "json_value(`data`, _utf8mb4'$.field')"
        >>> adapter.json_path_expr('data', 'value', 'decimal(10,2)')
        "json_value(`data`, _utf8mb4'$.value' returning decimal(10,2))"
        """
        quoted_col = self.quote_identifier(column)
        return_clause = f" returning {return_type}" if return_type else ""
        return f"json_value({quoted_col}, _utf8mb4'$.{path}'{return_clause})"

    def translate_expression(self, expr: str) -> str:
        """
        Translate SQL expression for MySQL compatibility.

        Converts PostgreSQL-specific functions to MySQL equivalents:
        - STRING_AGG(col, 'sep') → GROUP_CONCAT(col SEPARATOR 'sep')
        - STRING_AGG(col, ',') → GROUP_CONCAT(col)

        Parameters
        ----------
        expr : str
            SQL expression that may contain function calls.

        Returns
        -------
        str
            Translated expression for MySQL.
        """
        import re

        # STRING_AGG(col, 'sep') → GROUP_CONCAT(col SEPARATOR 'sep')
        def replace_string_agg(match):
            inner = match.group(1).strip()
            # Parse arguments: col, 'separator'
            # Handle both single and double quoted separators
            arg_match = re.match(r"(.+?)\s*,\s*(['\"])(.+?)\2", inner)
            if arg_match:
                col = arg_match.group(1).strip()
                sep = arg_match.group(3)
                # Remove ::text cast if present (PostgreSQL-specific)
                col = re.sub(r"::text$", "", col)
                if sep == ",":
                    return f"GROUP_CONCAT({col})"
                else:
                    return f"GROUP_CONCAT({col} SEPARATOR '{sep}')"
            else:
                # No separator found, just use the expression
                col = re.sub(r"::text$", "", inner)
                return f"GROUP_CONCAT({col})"

        expr = re.sub(r"STRING_AGG\s*\((.+?)\)", replace_string_agg, expr, flags=re.IGNORECASE)

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
        Format a column definition for MySQL DDL.

        Examples
        --------
        >>> adapter.format_column_definition('user_id', 'bigint', nullable=False, comment='user ID')
        "`user_id` bigint NOT NULL COMMENT \\"user ID\\""
        """
        parts = [self.quote_identifier(name), sql_type]
        if default:
            parts.append(default)  # e.g., "DEFAULT NULL" or "NOT NULL DEFAULT 5"
        elif not nullable:
            parts.append("NOT NULL")
        if comment:
            parts.append(f'COMMENT "{comment}"')
        return " ".join(parts)

    def table_options_clause(self, comment: str | None = None) -> str:
        """
        Generate MySQL table options clause.

        Examples
        --------
        >>> adapter.table_options_clause('test table')
        'ENGINE=InnoDB, COMMENT "test table"'
        >>> adapter.table_options_clause()
        'ENGINE=InnoDB'
        """
        clause = "ENGINE=InnoDB"
        if comment:
            clause += f', COMMENT "{comment}"'
        return clause

    def table_comment_ddl(self, full_table_name: str, comment: str) -> str | None:
        """
        MySQL uses inline COMMENT in CREATE TABLE, so no separate DDL needed.

        Examples
        --------
        >>> adapter.table_comment_ddl('`schema`.`table`', 'test comment')
        None
        """
        return None  # MySQL uses inline COMMENT

    def column_comment_ddl(self, full_table_name: str, column_name: str, comment: str) -> str | None:
        """
        MySQL uses inline COMMENT in column definitions, so no separate DDL needed.

        Examples
        --------
        >>> adapter.column_comment_ddl('`schema`.`table`', 'column', 'test comment')
        None
        """
        return None  # MySQL uses inline COMMENT

    def enum_type_ddl(self, type_name: str, values: list[str]) -> str | None:
        """
        MySQL uses inline enum type in column definition, so no separate DDL needed.

        Examples
        --------
        >>> adapter.enum_type_ddl('status_type', ['active', 'inactive'])
        None
        """
        return None  # MySQL uses inline enum

    def job_metadata_columns(self) -> list[str]:
        """
        Return MySQL-specific job metadata column definitions.

        Examples
        --------
        >>> adapter.job_metadata_columns()
        ["`_job_start_time` datetime(3) DEFAULT NULL",
         "`_job_duration` float DEFAULT NULL",
         "`_job_version` varchar(64) DEFAULT ''"]
        """
        return [
            "`_job_start_time` datetime(3) DEFAULT NULL",
            "`_job_duration` float DEFAULT NULL",
            "`_job_version` varchar(64) DEFAULT ''",
        ]

    # =========================================================================
    # Error Translation
    # =========================================================================

    def translate_error(self, error: Exception, query: str = "") -> Exception:
        """
        Translate MySQL error to DataJoint exception.

        Parameters
        ----------
        error : Exception
            MySQL exception (typically pymysql error).

        Returns
        -------
        Exception
            DataJoint exception or original error.
        """
        if not hasattr(error, "args") or len(error.args) == 0:
            return error

        err, *args = error.args

        match err:
            # Loss of connection errors
            case 0 | "(0, '')":
                return errors.LostConnectionError("Server connection lost due to an interface error.", *args)
            case 2006:
                return errors.LostConnectionError("Connection timed out", *args)
            case 2013:
                return errors.LostConnectionError("Server connection lost", *args)

            # Access errors
            case 1044 | 1142:
                query = args[0] if args else ""
                return errors.AccessError("Insufficient privileges.", args[0] if args else "", query)

            # Integrity errors
            case 1062:
                return errors.DuplicateError(*args)
            case 1217 | 1451 | 1452 | 3730:
                return errors.IntegrityError(*args)

            # Syntax errors
            case 1064:
                query = args[0] if args else ""
                return errors.QuerySyntaxError(args[0] if args else "", query)

            # Existence errors
            case 1146:
                query = args[0] if args else ""
                return errors.MissingTableError(args[0] if args else "", query)
            case 1364:
                return errors.MissingAttributeError(*args)
            case 1054:
                return errors.UnknownAttributeError(*args)

            # All other errors pass through unchanged
            case _:
                return error

    # =========================================================================
    # Native Type Validation
    # =========================================================================

    def validate_native_type(self, type_str: str) -> bool:
        """
        Check if a native MySQL type string is valid.

        Parameters
        ----------
        type_str : str
            Type string to validate.

        Returns
        -------
        bool
            True if valid MySQL type.
        """
        type_lower = type_str.lower().strip()

        # MySQL native types (simplified validation)
        valid_types = {
            # Integer types
            "tinyint",
            "smallint",
            "mediumint",
            "int",
            "integer",
            "bigint",
            # Floating point
            "float",
            "double",
            "real",
            "decimal",
            "numeric",
            # String types
            "char",
            "varchar",
            "binary",
            "varbinary",
            "tinyblob",
            "blob",
            "mediumblob",
            "longblob",
            "tinytext",
            "text",
            "mediumtext",
            "longtext",
            # Temporal types
            "date",
            "time",
            "datetime",
            "timestamp",
            "year",
            # Other
            "enum",
            "set",
            "json",
            "geometry",
        }

        # Extract base type (before parentheses)
        base_type = type_lower.split("(")[0].strip()

        return base_type in valid_types
