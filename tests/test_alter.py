import pytest
import re
import datajoint as dj
from . import schema as schema_any_module, PREFIX


class Experiment(dj.Imported):
    original_definition = """  # information about experiments
    -> Subject
    experiment_id  :smallint  # experiment number for this subject
    ---
    experiment_date  :date   # date when experiment was started
    -> [nullable] User
    data_path=""     :varchar(255)  # file path to recorded data
    notes=""         :varchar(2048) # e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :timestamp   # automatic timestamp
    """

    definition1 = """  # Experiment
    -> Subject
    experiment_id  :smallint  # experiment number for this subject
    ---
    data_path     : int  # some number
    extra=null : longblob  # just testing
    -> [nullable] User
    subject_notes=null         :varchar(2048) # {notes} e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :timestamp   # automatic timestamp
    """


class Parent(dj.Manual):
    definition = """
    parent_id: int
    """

    class Child(dj.Part):
        definition = """
        -> Parent
        """
        definition_new = """
        -> master
        ---
        child_id=null: int
        """

    class Grandchild(dj.Part):
        definition = """
        -> master.Child
        """
        definition_new = """
        -> master.Child
        ---
        grandchild_id=null: int
        """


LOCALS_ALTER = {
    "Experiment": Experiment,
    "Parent": Parent
}
COMBINED_CONTEXT = {
    **schema_any_module.LOCALS_ANY,
    **LOCALS_ALTER,
}


@pytest.fixture
def schema_alter(connection_test):
    schema_any = dj.Schema(
        PREFIX + "_alter",
        context=schema_any_module.LOCALS_ANY,
        connection=connection_test,
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

    # Overwrite Experiment and Parent nodes
    schema_any(Experiment, context=LOCALS_ALTER)
    schema_any(Parent, context=LOCALS_ALTER)

    yield schema_any
    schema_any.drop()


class TestAlter:
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
