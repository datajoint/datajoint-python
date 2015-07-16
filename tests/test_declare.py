from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal
from . import schema


class TestDeclare:
    def __init__(self):
        self.user = schema.User()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.EphysChannel()

    def test_attributes(self):
        assert_list_equal(self.subject.heading.names,
                          ['subject_id', 'real_id', 'species', 'date_of_birth', 'subject_notes'])
        assert_list_equal(self.subject.primary_key,
                          ['subject_id'])
        assert_true(self.subject.heading.attributes['subject_id'].numeric)
        assert_false(self.subject.heading.attributes['real_id'].numeric)

        experiment = schema.Experiment()
        assert_list_equal(experiment.heading.names,
                          ['subject_id', 'experiment_id', 'experiment_date',
                           'username', 'data_path',
                           'notes', 'entry_time'])
        assert_list_equal(experiment.primary_key,
                          ['subject_id', 'experiment_id'])

        assert_list_equal(self.trial.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'start_time'])
        assert_list_equal(self.trial.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id'])

        assert_list_equal(self.ephys.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'sampling_frequency', 'duration'])
        assert_list_equal(self.ephys.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id'])

        assert_list_equal(self.channel.heading.names,
                          ['subject_id', 'experiment_id', 'trial_id', 'channel', 'voltage'])
        assert_list_equal(self.channel.primary_key,
                          ['subject_id', 'experiment_id', 'trial_id', 'channel'])
        assert_true(self.channel.heading.attributes['voltage'].is_blob)

    def test_dependencies(self):
        assert_equal(self.user.references, [self.experiment.full_table_name])
        assert_equal(self.experiment.referenced, [self.user.full_table_name])

        assert_equal(self.subject.children, [self.experiment.full_table_name])
        assert_equal(self.experiment.parents, [self.subject.full_table_name])

        assert_equal(self.experiment.children, [self.trial.full_table_name])
        assert_equal(self.trial.parents, [self.experiment.full_table_name])

        assert_equal(self.trial.children, [self.ephys.full_table_name])
        assert_equal(self.ephys.parents, [self.trial.full_table_name])

        assert_equal(self.ephys.children, [self.channel.full_table_name])
        assert_equal(self.channel.parents, [self.ephys.full_table_name])
