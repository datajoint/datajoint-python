from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from . import schema
import datajoint as dj
from datajoint.declare import declare

auto = schema.Auto()
auto.fill()
user = schema.User()
subject = schema.Subject()
experiment = schema.Experiment()
trial = schema.Trial()
ephys = schema.Ephys()
channel = schema.Ephys.Channel()

context = schema.schema.context


class TestDeclare:

    @staticmethod
    def test_schema_decorator():
        assert_true(issubclass(schema.Subject, dj.Lookup))
        assert_true(not issubclass(schema.Subject, dj.Part))

    @staticmethod
    def test_describe():
        """real_definition should match original definition"""
        rel = schema.Experiment()
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert_equal(s1, s2)

    @staticmethod
    def test_attributes():
        # test autoincrement declaration
        assert_list_equal(auto.heading.names, ['id', 'name'])
        assert_true(auto.heading.attributes['id'].autoincrement)

        # test attribute declarations
        assert_list_equal(subject.heading.names,
                          ['subject_id', 'real_id', 'species', 'date_of_birth', 'subject_notes'])
        assert_list_equal(subject.primary_key,
                          ['subject_id'])
        assert_true(subject.heading.attributes['subject_id'].numeric)
        assert_false(subject.heading.attributes['real_id'].numeric)

        assert_list_equal(experiment.heading.names,
                          ['subject_id', 'experiment_id', 'experiment_date',
                           'username', 'data_path',
                           'notes', 'entry_time'])
        assert_list_equal(experiment.primary_key,
                          ['subject_id', 'experiment_id'])

        assert_list_equal(trial.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'start_time'])
        assert_list_equal(trial.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id'])

        assert_list_equal(ephys.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'sampling_frequency', 'duration'])
        assert_list_equal(ephys.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id'])

        assert_list_equal(channel.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'channel', 'voltage', 'current'])
        assert_list_equal(channel.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id', 'channel'])
        assert_true(channel.heading.attributes['voltage'].is_blob)

    def test_dependencies(self):
        assert_equal(set(user.children(primary=False)), set([experiment.full_table_name]))
        assert_equal(set(experiment.parents(primary=False)), set([user.full_table_name]))

        assert_equal(set(subject.children(primary=True)), set([experiment.full_table_name]))
        assert_equal(set(experiment.parents(primary=True)), set([subject.full_table_name]))

        assert_equal(set(experiment.children(primary=True)), set([trial.full_table_name]))
        assert_equal(set(trial.parents(primary=True)), set([experiment.full_table_name]))

        assert_equal(set(trial.children(primary=True)),
                     set((ephys.full_table_name, trial.Condition.full_table_name)))
        assert_equal(set(ephys.parents(primary=True)), set([trial.full_table_name]))

        assert_equal(set(ephys.children(primary=True)), set([channel.full_table_name]))
        assert_equal(set(channel.parents(primary=True)), set([ephys.full_table_name]))

    @raises(dj.DataJointError)
    def test_bad_attribute_name(self):

        @schema.schema
        class BadName(dj.Manual):
            definition = """
            Bad_name : int
            """
