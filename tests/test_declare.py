import warnings
from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from . import schema
import datajoint as dj

auto = schema.Auto()
user = schema.User()
subject = schema.Subject()
experiment = schema.Experiment()
trial = schema.Trial()
ephys = schema.Ephys()
channel = schema.Ephys.Channel()


class TestDeclare:
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
                          ['subject_id', 'experiment_id', 'trial_id', 'channel', 'voltage'])
        assert_list_equal(channel.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id', 'channel'])
        assert_true(channel.heading.attributes['voltage'].is_blob)

    def test_dependencies(self):
        assert_equal(user.references, [experiment.full_table_name])
        assert_equal(experiment.referenced, [user.full_table_name])

        assert_equal(subject.children, [experiment.full_table_name])
        assert_equal(experiment.parents, [subject.full_table_name])

        assert_equal(experiment.children, [trial.full_table_name])
        assert_equal(trial.parents, [experiment.full_table_name])

        assert_equal(trial.children, [ephys.full_table_name])
        assert_equal(ephys.parents, [trial.full_table_name])

        assert_equal(ephys.children, [channel.full_table_name])
        assert_equal(channel.parents, [ephys.full_table_name])


    @raises(dj.NoDefinitionError)
    def test_no_definition_error(self):
        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter('always')
            from . import PREFIX, CONN_INFO

            schema = dj.schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))

            @schema
            class FromMatlab(dj.Manual):
                definition = ...
        FromMatlab().declare()
