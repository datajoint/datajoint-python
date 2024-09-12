import pytest
import re
import datajoint as dj
from . import schema as schema_any_module
from .schema_alter import Experiment, Parent, LOCALS_ALTER

COMBINED_CONTEXT = {
    **schema_any_module.LOCALS_ANY,
    **LOCALS_ALTER,
}


@pytest.fixture
def schema_alter(connection_test, schema_any):
    # Overwrite Experiment and Parent nodes
    schema_any(Experiment, context=LOCALS_ALTER)
    schema_any(Parent, context=LOCALS_ALTER)
    yield schema_any
    schema_any.drop()


class TestAlter:
    def verify_alter(self, schema_alter, table, attribute_sql):
        definition_original = schema_alter.connection.query(
            f"SHOW CREATE TABLE {table.full_table_name}"
        ).fetchone()[1]
        table.definition = table.definition_new
        table.alter(prompt=False)
        definition_new = schema_alter.connection.query(
            f"SHOW CREATE TABLE {table.full_table_name}"
        ).fetchone()[1]
        assert (
            re.sub(f"{attribute_sql},\n  ", "", definition_new) == definition_original
        )

    def test_alter(self, schema_alter):
        original = schema_alter.connection.query(
            "SHOW CREATE TABLE " + Experiment.full_table_name
        ).fetchone()[1]
        Experiment.definition = Experiment.definition1
        Experiment.alter(prompt=False, context=COMBINED_CONTEXT)
        altered = schema_alter.connection.query(
            "SHOW CREATE TABLE " + Experiment.full_table_name
        ).fetchone()[1]
        assert original != altered
        Experiment.definition = Experiment.original_definition
        Experiment().alter(prompt=False, context=COMBINED_CONTEXT)
        restored = schema_alter.connection.query(
            "SHOW CREATE TABLE " + Experiment.full_table_name
        ).fetchone()[1]
        assert altered != restored
        assert original == restored

    def test_alter_part(self, schema_alter):
        """
        https://github.com/datajoint/datajoint-python/issues/936
        """
        self.verify_alter(
            schema_alter, table=Parent.Child, attribute_sql="`child_id` .* DEFAULT NULL"
        )
        self.verify_alter(
            schema_alter,
            table=Parent.Grandchild,
            attribute_sql="`grandchild_id` .* DEFAULT NULL",
        )
