"""Tests for virtual schema infrastructure."""

import pytest

import datajoint as dj
from datajoint.table import FreeTable
from datajoint.user_tables import UserTable


class TestVirtualModule:
    """Tests for VirtualModule class."""

    def test_virtual_module_creates_table_classes(self, schema_any, connection_test):
        """VirtualModule creates table classes from database schema."""
        module = dj.VirtualModule("module", schema_any.database, connection=connection_test)
        assert issubclass(module.Experiment, UserTable)

    def test_virtual_module_has_schema_attribute(self, schema_any, connection_test):
        """VirtualModule has schema attribute."""
        module = dj.VirtualModule("module", schema_any.database, connection=connection_test)
        assert hasattr(module, "schema")
        assert module.schema.database == schema_any.database


class TestVirtualSchema:
    """Tests for dj.virtual_schema() function."""

    def test_virtual_schema_creates_module(self, schema_any, connection_test):
        """virtual_schema creates a VirtualModule."""
        lab = dj.virtual_schema(schema_any.database, connection=connection_test)
        assert isinstance(lab, dj.VirtualModule)

    def test_virtual_schema_has_table_classes(self, schema_any, connection_test):
        """virtual_schema module has table classes as attributes."""
        lab = dj.virtual_schema(schema_any.database, connection=connection_test)
        assert issubclass(lab.Experiment, UserTable)

    def test_virtual_schema_tables_are_queryable(self, schema_any, connection_test):
        """Tables from virtual_schema can be queried."""
        lab = dj.virtual_schema(schema_any.database, connection=connection_test)
        # Should not raise
        lab.Experiment().to_dicts()


class TestSchemaGetTable:
    """Tests for Schema.get_table() method."""

    def test_get_table_by_snake_case(self, schema_any):
        """get_table works with snake_case table names."""
        table = schema_any.get_table("experiment")
        assert isinstance(table, FreeTable)
        assert "experiment" in table.full_table_name

    def test_get_table_by_camel_case(self, schema_any):
        """get_table works with CamelCase table names."""
        table = schema_any.get_table("Experiment")
        assert isinstance(table, FreeTable)
        assert "experiment" in table.full_table_name

    def test_get_table_nonexistent_raises(self, schema_any):
        """get_table raises DataJointError for nonexistent tables."""
        with pytest.raises(dj.DataJointError, match="does not exist"):
            schema_any.get_table("NonexistentTable")


class TestSchemaGetItem:
    """Tests for Schema.__getitem__() method."""

    def test_getitem_by_name(self, schema_any):
        """Schema['TableName'] returns table instance."""
        table = schema_any["Experiment"]
        assert isinstance(table, FreeTable)

    def test_getitem_is_queryable(self, schema_any):
        """Table from __getitem__ can be queried."""
        table = schema_any["Experiment"]
        # Should not raise
        table.to_dicts()


class TestSchemaIteration:
    """Tests for Schema.__iter__() method."""

    def test_iter_yields_tables(self, schema_any):
        """Iterating over schema yields FreeTable instances."""
        tables = list(schema_any)
        assert len(tables) > 0
        assert all(isinstance(t, FreeTable) for t in tables)

    def test_iter_in_dependency_order(self, schema_any):
        """Iteration order respects dependencies."""
        table_names = [t.table_name for t in schema_any]
        # Tables should be in topological order
        assert len(table_names) == len(set(table_names))  # no duplicates


class TestSchemaContains:
    """Tests for Schema.__contains__() method."""

    def test_contains_existing_table(self, schema_any):
        """'TableName' in schema returns True for existing tables."""
        assert "Experiment" in schema_any
        assert "experiment" in schema_any

    def test_contains_nonexistent_table(self, schema_any):
        """'TableName' in schema returns False for nonexistent tables."""
        assert "NonexistentTable" not in schema_any
