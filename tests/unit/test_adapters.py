"""
Unit tests for database adapters.

Tests adapter functionality without requiring actual database connections.
"""

import pytest

from datajoint.adapters import DatabaseAdapter, MySQLAdapter, PostgreSQLAdapter, get_adapter


class TestAdapterRegistry:
    """Test adapter registry and factory function."""

    def test_get_adapter_mysql(self):
        """Test getting MySQL adapter."""
        adapter = get_adapter("mysql")
        assert isinstance(adapter, MySQLAdapter)
        assert isinstance(adapter, DatabaseAdapter)

    def test_get_adapter_postgresql(self):
        """Test getting PostgreSQL adapter."""
        pytest.importorskip("psycopg2")
        adapter = get_adapter("postgresql")
        assert isinstance(adapter, PostgreSQLAdapter)
        assert isinstance(adapter, DatabaseAdapter)

    def test_get_adapter_postgres_alias(self):
        """Test 'postgres' alias for PostgreSQL."""
        pytest.importorskip("psycopg2")
        adapter = get_adapter("postgres")
        assert isinstance(adapter, PostgreSQLAdapter)

    def test_get_adapter_case_insensitive(self):
        """Test case-insensitive backend names."""
        assert isinstance(get_adapter("MySQL"), MySQLAdapter)
        # Only test PostgreSQL if psycopg2 is available
        try:
            pytest.importorskip("psycopg2")
            assert isinstance(get_adapter("POSTGRESQL"), PostgreSQLAdapter)
            assert isinstance(get_adapter("PoStGrEs"), PostgreSQLAdapter)
        except pytest.skip.Exception:
            pass  # Skip PostgreSQL tests if psycopg2 not available

    def test_get_adapter_invalid(self):
        """Test error on invalid backend name."""
        with pytest.raises(ValueError, match="Unknown database backend"):
            get_adapter("sqlite")


class TestMySQLAdapter:
    """Test MySQL adapter implementation."""

    @pytest.fixture
    def adapter(self):
        """MySQL adapter instance."""
        return MySQLAdapter()

    def test_default_port(self, adapter):
        """Test MySQL default port is 3306."""
        assert adapter.default_port == 3306

    def test_parameter_placeholder(self, adapter):
        """Test MySQL parameter placeholder is %s."""
        assert adapter.parameter_placeholder == "%s"

    def test_quote_identifier(self, adapter):
        """Test identifier quoting with backticks."""
        assert adapter.quote_identifier("table_name") == "`table_name`"
        assert adapter.quote_identifier("my_column") == "`my_column`"

    def test_quote_string(self, adapter):
        """Test string literal quoting."""
        assert "test" in adapter.quote_string("test")
        # Should handle escaping
        result = adapter.quote_string("It's a test")
        assert "It" in result

    def test_core_type_to_sql_simple(self, adapter):
        """Test core type mapping for simple types."""
        assert adapter.core_type_to_sql("int64") == "bigint"
        assert adapter.core_type_to_sql("int32") == "int"
        assert adapter.core_type_to_sql("int16") == "smallint"
        assert adapter.core_type_to_sql("int8") == "tinyint"
        assert adapter.core_type_to_sql("float32") == "float"
        assert adapter.core_type_to_sql("float64") == "double"
        assert adapter.core_type_to_sql("bool") == "tinyint"
        assert adapter.core_type_to_sql("uuid") == "binary(16)"
        assert adapter.core_type_to_sql("bytes") == "longblob"
        assert adapter.core_type_to_sql("json") == "json"
        assert adapter.core_type_to_sql("date") == "date"

    def test_core_type_to_sql_parametrized(self, adapter):
        """Test core type mapping for parametrized types."""
        assert adapter.core_type_to_sql("datetime") == "datetime"
        assert adapter.core_type_to_sql("datetime(3)") == "datetime(3)"
        assert adapter.core_type_to_sql("char(10)") == "char(10)"
        assert adapter.core_type_to_sql("varchar(255)") == "varchar(255)"
        assert adapter.core_type_to_sql("decimal(10,2)") == "decimal(10,2)"
        assert adapter.core_type_to_sql("enum('a','b','c')") == "enum('a','b','c')"

    def test_core_type_to_sql_invalid(self, adapter):
        """Test error on invalid core type."""
        with pytest.raises(ValueError, match="Unknown core type"):
            adapter.core_type_to_sql("invalid_type")

    def test_sql_type_to_core(self, adapter):
        """Test reverse type mapping."""
        assert adapter.sql_type_to_core("bigint") == "int64"
        assert adapter.sql_type_to_core("int") == "int32"
        assert adapter.sql_type_to_core("float") == "float32"
        assert adapter.sql_type_to_core("double") == "float64"
        assert adapter.sql_type_to_core("longblob") == "bytes"
        assert adapter.sql_type_to_core("datetime(3)") == "datetime(3)"
        # Unmappable types return None
        assert adapter.sql_type_to_core("mediumint") is None

    def test_create_schema_sql(self, adapter):
        """Test CREATE DATABASE statement."""
        sql = adapter.create_schema_sql("test_db")
        assert sql == "CREATE DATABASE `test_db`"

    def test_drop_schema_sql(self, adapter):
        """Test DROP DATABASE statement."""
        sql = adapter.drop_schema_sql("test_db")
        assert "DROP DATABASE" in sql
        assert "IF EXISTS" in sql
        assert "`test_db`" in sql

    def test_insert_sql_basic(self, adapter):
        """Test basic INSERT statement."""
        sql = adapter.insert_sql("users", ["id", "name"])
        assert sql == "INSERT INTO users (`id`, `name`) VALUES (%s, %s)"

    def test_insert_sql_ignore(self, adapter):
        """Test INSERT IGNORE statement."""
        sql = adapter.insert_sql("users", ["id", "name"], on_duplicate="ignore")
        assert "INSERT IGNORE" in sql

    def test_insert_sql_replace(self, adapter):
        """Test REPLACE INTO statement."""
        sql = adapter.insert_sql("users", ["id"], on_duplicate="replace")
        assert "REPLACE INTO" in sql

    def test_insert_sql_update(self, adapter):
        """Test INSERT ... ON DUPLICATE KEY UPDATE statement."""
        sql = adapter.insert_sql("users", ["id", "name"], on_duplicate="update")
        assert "INSERT INTO" in sql
        assert "ON DUPLICATE KEY UPDATE" in sql

    def test_update_sql(self, adapter):
        """Test UPDATE statement."""
        sql = adapter.update_sql("users", ["name"], ["id"])
        assert "UPDATE users SET" in sql
        assert "`name` = %s" in sql
        assert "WHERE" in sql
        assert "`id` = %s" in sql

    def test_delete_sql(self, adapter):
        """Test DELETE statement."""
        sql = adapter.delete_sql("users")
        assert sql == "DELETE FROM users"

    def test_current_timestamp_expr(self, adapter):
        """Test CURRENT_TIMESTAMP expression."""
        assert adapter.current_timestamp_expr() == "CURRENT_TIMESTAMP"
        assert adapter.current_timestamp_expr(3) == "CURRENT_TIMESTAMP(3)"

    def test_interval_expr(self, adapter):
        """Test INTERVAL expression."""
        assert adapter.interval_expr(5, "second") == "INTERVAL 5 SECOND"
        assert adapter.interval_expr(10, "minute") == "INTERVAL 10 MINUTE"

    def test_transaction_sql(self, adapter):
        """Test transaction statements."""
        assert "START TRANSACTION" in adapter.start_transaction_sql()
        assert adapter.commit_sql() == "COMMIT"
        assert adapter.rollback_sql() == "ROLLBACK"

    def test_validate_native_type(self, adapter):
        """Test native type validation."""
        assert adapter.validate_native_type("int")
        assert adapter.validate_native_type("bigint")
        assert adapter.validate_native_type("varchar(255)")
        assert adapter.validate_native_type("text")
        assert adapter.validate_native_type("json")
        assert not adapter.validate_native_type("invalid_type")


class TestPostgreSQLAdapter:
    """Test PostgreSQL adapter implementation."""

    @pytest.fixture
    def adapter(self):
        """PostgreSQL adapter instance."""
        # Skip if psycopg2 not installed
        pytest.importorskip("psycopg2")
        return PostgreSQLAdapter()

    def test_default_port(self, adapter):
        """Test PostgreSQL default port is 5432."""
        assert adapter.default_port == 5432

    def test_parameter_placeholder(self, adapter):
        """Test PostgreSQL parameter placeholder is %s."""
        assert adapter.parameter_placeholder == "%s"

    def test_quote_identifier(self, adapter):
        """Test identifier quoting with double quotes."""
        assert adapter.quote_identifier("table_name") == '"table_name"'
        assert adapter.quote_identifier("my_column") == '"my_column"'

    def test_quote_string(self, adapter):
        """Test string literal quoting."""
        assert adapter.quote_string("test") == "'test'"
        # PostgreSQL doubles single quotes for escaping
        assert adapter.quote_string("It's a test") == "'It''s a test'"

    def test_core_type_to_sql_simple(self, adapter):
        """Test core type mapping for simple types."""
        assert adapter.core_type_to_sql("int64") == "bigint"
        assert adapter.core_type_to_sql("int32") == "integer"
        assert adapter.core_type_to_sql("int16") == "smallint"
        assert adapter.core_type_to_sql("int8") == "smallint"  # No tinyint in PostgreSQL
        assert adapter.core_type_to_sql("float32") == "real"
        assert adapter.core_type_to_sql("float64") == "double precision"
        assert adapter.core_type_to_sql("bool") == "boolean"
        assert adapter.core_type_to_sql("uuid") == "uuid"
        assert adapter.core_type_to_sql("bytes") == "bytea"
        assert adapter.core_type_to_sql("json") == "jsonb"
        assert adapter.core_type_to_sql("date") == "date"

    def test_core_type_to_sql_parametrized(self, adapter):
        """Test core type mapping for parametrized types."""
        assert adapter.core_type_to_sql("datetime") == "timestamp"
        assert adapter.core_type_to_sql("datetime(3)") == "timestamp(3)"
        assert adapter.core_type_to_sql("char(10)") == "char(10)"
        assert adapter.core_type_to_sql("varchar(255)") == "varchar(255)"
        assert adapter.core_type_to_sql("decimal(10,2)") == "numeric(10,2)"

    def test_sql_type_to_core(self, adapter):
        """Test reverse type mapping."""
        assert adapter.sql_type_to_core("bigint") == "int64"
        assert adapter.sql_type_to_core("integer") == "int32"
        assert adapter.sql_type_to_core("real") == "float32"
        assert adapter.sql_type_to_core("double precision") == "float64"
        assert adapter.sql_type_to_core("boolean") == "bool"
        assert adapter.sql_type_to_core("uuid") == "uuid"
        assert adapter.sql_type_to_core("bytea") == "bytes"
        assert adapter.sql_type_to_core("jsonb") == "json"
        assert adapter.sql_type_to_core("timestamp") == "datetime"
        assert adapter.sql_type_to_core("timestamp(3)") == "datetime(3)"
        assert adapter.sql_type_to_core("numeric(10,2)") == "decimal(10,2)"

    def test_create_schema_sql(self, adapter):
        """Test CREATE SCHEMA statement."""
        sql = adapter.create_schema_sql("test_schema")
        assert sql == 'CREATE SCHEMA "test_schema"'

    def test_drop_schema_sql(self, adapter):
        """Test DROP SCHEMA statement."""
        sql = adapter.drop_schema_sql("test_schema")
        assert "DROP SCHEMA" in sql
        assert "IF EXISTS" in sql
        assert '"test_schema"' in sql
        assert "CASCADE" in sql

    def test_insert_sql_basic(self, adapter):
        """Test basic INSERT statement."""
        sql = adapter.insert_sql("users", ["id", "name"])
        assert sql == 'INSERT INTO users ("id", "name") VALUES (%s, %s)'

    def test_insert_sql_ignore(self, adapter):
        """Test INSERT ... ON CONFLICT DO NOTHING statement."""
        sql = adapter.insert_sql("users", ["id", "name"], on_duplicate="ignore")
        assert "INSERT INTO" in sql
        assert "ON CONFLICT DO NOTHING" in sql

    def test_insert_sql_update(self, adapter):
        """Test INSERT ... ON CONFLICT DO UPDATE statement."""
        sql = adapter.insert_sql("users", ["id", "name"], on_duplicate="update")
        assert "INSERT INTO" in sql
        assert "ON CONFLICT DO UPDATE" in sql
        assert "EXCLUDED" in sql

    def test_update_sql(self, adapter):
        """Test UPDATE statement."""
        sql = adapter.update_sql("users", ["name"], ["id"])
        assert "UPDATE users SET" in sql
        assert '"name" = %s' in sql
        assert "WHERE" in sql
        assert '"id" = %s' in sql

    def test_delete_sql(self, adapter):
        """Test DELETE statement."""
        sql = adapter.delete_sql("users")
        assert sql == "DELETE FROM users"

    def test_current_timestamp_expr(self, adapter):
        """Test CURRENT_TIMESTAMP expression."""
        assert adapter.current_timestamp_expr() == "CURRENT_TIMESTAMP"
        assert adapter.current_timestamp_expr(3) == "CURRENT_TIMESTAMP(3)"

    def test_interval_expr(self, adapter):
        """Test INTERVAL expression with PostgreSQL syntax."""
        assert adapter.interval_expr(5, "second") == "INTERVAL '5 seconds'"
        assert adapter.interval_expr(10, "minute") == "INTERVAL '10 minutes'"

    def test_transaction_sql(self, adapter):
        """Test transaction statements."""
        assert adapter.start_transaction_sql() == "BEGIN"
        assert adapter.commit_sql() == "COMMIT"
        assert adapter.rollback_sql() == "ROLLBACK"

    def test_validate_native_type(self, adapter):
        """Test native type validation."""
        assert adapter.validate_native_type("integer")
        assert adapter.validate_native_type("bigint")
        assert adapter.validate_native_type("varchar")
        assert adapter.validate_native_type("text")
        assert adapter.validate_native_type("jsonb")
        assert adapter.validate_native_type("uuid")
        assert adapter.validate_native_type("boolean")
        assert not adapter.validate_native_type("invalid_type")

    def test_enum_type_sql(self, adapter):
        """Test PostgreSQL enum type creation."""
        sql = adapter.create_enum_type_sql("myschema", "mytable", "status", ["pending", "complete"])
        assert "CREATE TYPE" in sql
        assert "myschema_mytable_status_enum" in sql
        assert "AS ENUM" in sql
        assert "'pending'" in sql
        assert "'complete'" in sql

    def test_drop_enum_type_sql(self, adapter):
        """Test PostgreSQL enum type dropping."""
        sql = adapter.drop_enum_type_sql("myschema", "mytable", "status")
        assert "DROP TYPE" in sql
        assert "IF EXISTS" in sql
        assert "myschema_mytable_status_enum" in sql
        assert "CASCADE" in sql


class TestAdapterInterface:
    """Test that adapters implement the full interface."""

    @pytest.mark.parametrize("backend", ["mysql", "postgresql"])
    def test_adapter_implements_interface(self, backend):
        """Test that adapter implements all abstract methods."""
        if backend == "postgresql":
            pytest.importorskip("psycopg2")

        adapter = get_adapter(backend)

        # Check that all abstract methods are implemented (not abstract)
        abstract_methods = [
            "connect",
            "close",
            "ping",
            "get_connection_id",
            "quote_identifier",
            "quote_string",
            "core_type_to_sql",
            "sql_type_to_core",
            "create_schema_sql",
            "drop_schema_sql",
            "create_table_sql",
            "drop_table_sql",
            "alter_table_sql",
            "add_comment_sql",
            "insert_sql",
            "update_sql",
            "delete_sql",
            "list_schemas_sql",
            "list_tables_sql",
            "get_table_info_sql",
            "get_columns_sql",
            "get_primary_key_sql",
            "get_foreign_keys_sql",
            "get_indexes_sql",
            "parse_column_info",
            "start_transaction_sql",
            "commit_sql",
            "rollback_sql",
            "current_timestamp_expr",
            "interval_expr",
            "translate_error",
            "validate_native_type",
        ]

        for method_name in abstract_methods:
            assert hasattr(adapter, method_name), f"Adapter missing method: {method_name}"
            method = getattr(adapter, method_name)
            assert callable(method), f"Adapter.{method_name} is not callable"

        # Check properties
        assert hasattr(adapter, "default_port")
        assert isinstance(adapter.default_port, int)
        assert hasattr(adapter, "parameter_placeholder")
        assert isinstance(adapter.parameter_placeholder, str)
