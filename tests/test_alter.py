from nose.tools import assert_equal, assert_not_equal

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


def test_alter():

    original = schema.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
    Experiment.definition = Experiment.definition1
    Experiment.alter(prompt=False)
    altered = schema.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
    Experiment.definition = Experiment.original_definition
    Experiment().alter(prompt=False)
    restored = schema.connection.query("SHOW CREATE TABLE " + Experiment.full_table_name).fetchone()[1]
    assert_equal(original, restored)
    assert_not_equal(original, altered)

