import pytest
import re
import datajoint as dj
from . import schema as schema_any_module, schema_alter as schema_alter_module, PREFIX

COMBINED_CONTEXT = {
    **schema_any_module.LOCALS_ANY,
    **schema_alter_module.LOCALS_ALTER,
}


@pytest.fixture
def schema_alter(connection_test):
    context = {
        **schema_any_module.LOCALS_ANY,
        # **schema_alter_module.LOCALS_ALTER,
    }
    schema_any = dj.Schema(
        PREFIX + "_test1", context=context, connection=connection_test
    )
    schema_any(schema_any_module.TTest)
    schema_any(schema_any_module.TTest2)
    schema_any(schema_any_module.TTest3)
    schema_any(schema_any_module.NullableNumbers)
    schema_any(schema_any_module.TTestExtra)
    schema_any(schema_any_module.TTestNoExtra)
    schema_any(schema_any_module.Auto)
    schema_any(schema_any_module.User)
    schema_any(schema_any_module.Subject)
    schema_any(schema_any_module.Language)
    schema_any(schema_any_module.Experiment)
    schema_any(schema_any_module.Trial)
    schema_any(schema_any_module.Ephys)
    schema_any(schema_any_module.Image)
    schema_any(schema_any_module.UberTrash)
    schema_any(schema_any_module.UnterTrash)
    schema_any(schema_any_module.SimpleSource)
    schema_any(schema_any_module.SigIntTable)
    schema_any(schema_any_module.SigTermTable)
    schema_any(schema_any_module.DjExceptionName)
    schema_any(schema_any_module.ErrorClass)
    schema_any(schema_any_module.DecimalPrimaryKey)
    schema_any(schema_any_module.IndexRich)
    schema_any(schema_any_module.ThingA)
    schema_any(schema_any_module.ThingB)
    schema_any(schema_any_module.ThingC)
    schema_any(schema_any_module.Parent)
    schema_any(schema_any_module.Child)
    schema_any(schema_any_module.ComplexParent)
    schema_any(schema_any_module.ComplexChild)
    schema_any(schema_any_module.SubjectA)
    schema_any(schema_any_module.SessionA)
    schema_any(schema_any_module.SessionStatusA)
    schema_any(schema_any_module.SessionDateA)
    schema_any(schema_any_module.Stimulus)
    schema_any(schema_any_module.Longblob)

    schema_any(schema_alter_module.Experiment, context=schema_alter_module.LOCALS_ALTER)
    schema_any(schema_alter_module.Parent, context=schema_alter_module.LOCALS_ALTER)

    yield schema_any
    schema_any.drop()


# @pytest.fixture
def _schema_alter(schema_any):
    context = {
        **schema_any_module.LOCALS_ANY,
        **schema_alter_module.LOCALS_ALTER,
    }
    schema_any(schema_alter_module.Experiment, context=context)
    schema_any(schema_alter_module.Parent, context=context)
    yield schema_any
    schema_any.drop()



def test_alter(schema_alter):
    schema = schema_alter
    original = schema.connection.query(
        "SHOW CREATE TABLE " + schema_alter_module.Experiment.full_table_name
    ).fetchone()[1]
    schema_alter_module.Experiment.definition = schema_alter_module.Experiment.definition1
    schema_alter_module.Experiment.alter(prompt=False, context=COMBINED_CONTEXT)
    altered = schema.connection.query(
        "SHOW CREATE TABLE " + schema_alter_module.Experiment.full_table_name
    ).fetchone()[1]
    assert original != altered
    schema_alter_module.Experiment.definition = schema_alter_module.Experiment.original_definition
    schema_alter_module.Experiment().alter(prompt=False, context=COMBINED_CONTEXT)
    restored = schema.connection.query(
        "SHOW CREATE TABLE " + schema_alter_module.Experiment.full_table_name
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

    verify_alter(table=schema_alter_module.Parent.Child, attribute_sql="`child_id` .* DEFAULT NULL")
    verify_alter(
        table=schema_alter_module.Parent.Grandchild, attribute_sql="`grandchild_id` .* DEFAULT NULL"
    )
