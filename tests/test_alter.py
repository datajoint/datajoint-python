import pytest
import re
import datajoint as dj
from . import schema_simple, schema_alter as schema_alter_module, PREFIX


@pytest.fixture
def _schema_alter(connection_test):
    context = {
        # **schema_alter_module.LOCALS_ALTER,
        # **schema_simple.LOCALS_SIMPLE,
    }
    schema = dj.Schema(
        PREFIX + "_alter", context=context, connection=connection_test
    )
    schema(schema_simple.IJ, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.JI, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.A, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.B, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.L, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.D, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.E, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.F, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.F, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.DataA, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.DataB, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.Website, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.Profile, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.Website, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.TTestUpdate, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.ArgmaxTest, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.ReservedWord, context=schema_simple.LOCALS_SIMPLE)
    schema(schema_simple.OutfitLaunch, context=schema_simple.LOCALS_SIMPLE)

    schema(schema_alter_module.Experiment, context=schema_alter_module.LOCALS_ALTER)
    schema(schema_alter_module.Parent, context=schema_alter_module.LOCALS_ALTER)

    yield schema
    schema.drop()


@pytest.fixture
def schema_alter(schema_simp):
    # context = {
    #     **schema_simple.LOCALS_SIMPLE,
    #     **schema_alter_module.LOCALS_ALTER,
    # }
    schema = schema_simp
    schema(schema_alter_module.Experiment)
    schema(schema_alter_module.Parent)
    yield schema
    schema.drop()



def test_alter(schema_alter):
    schema = schema_alter
    original = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    Experiment.definition = Experiment.definition1
    Experiment.alter(prompt=False)
    altered = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    assert original != altered
    Experiment.definition = Experiment.original_definition
    Experiment().alter(prompt=False)
    restored = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    assert altered != restored
    assert original == restored


def test_alter_part(schema_alter):
    # https://github.com/datajoint/datajoint-python/issues/936
    schema = schema_alter

    def verify_alter(table, attribute_sql):
        definition_original = schema.connection.query(
            f"SHOW CREATE TABLE {table.full_table_name}"
        ).fetchone()[1]
        table.definition = table.definition_new
        table.alter(prompt=False)
        definition_new = schema.connection.query(
            f"SHOW CREATE TABLE {table.full_table_name}"
        ).fetchone()[1]
        assert (
            re.sub(f"{attribute_sql},\n  ", "", definition_new) == definition_original
        )

    verify_alter(table=Parent.Child, attribute_sql="`child_id` .* DEFAULT NULL")
    verify_alter(
        table=Parent.Grandchild, attribute_sql="`grandchild_id` .* DEFAULT NULL"
    )
