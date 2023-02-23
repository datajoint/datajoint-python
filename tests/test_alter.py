from nose.tools import assert_equal, assert_not_equal
import re
from .schema import *


@schema
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


@schema
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


def test_alter():
    original = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    Experiment.definition = Experiment.definition1
    Experiment.alter(prompt=False)
    altered = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    assert_not_equal(original, altered)
    Experiment.definition = Experiment.original_definition
    Experiment().alter(prompt=False)
    restored = schema.connection.query(
        "SHOW CREATE TABLE " + Experiment.full_table_name
    ).fetchone()[1]
    assert_not_equal(altered, restored)
    assert_equal(original, restored)


def test_alter_part():
    # https://github.com/datajoint/datajoint-python/issues/936

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
