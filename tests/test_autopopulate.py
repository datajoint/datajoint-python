from nose.tools import assert_equal, assert_false, assert_true, raises
from . import schema
from datajoint import DataJointError


class TestPopulate:
    """
    Test base relations: insert, delete
    """

    def setUp(self):
        self.user = schema.User()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.Ephys.Channel()

    def tearDown(self):
        # delete automatic tables just in case
        self.channel.delete_quick()
        self.ephys.delete_quick()
        self.trial.Condition.delete_quick()
        self.trial.delete_quick()
        self.experiment.delete_quick()

    def test_populate(self):
        # test simple populate
        assert_true(self.subject, 'root tables are empty')
        assert_false(self.experiment, 'table already filled?')
        self.experiment.populate()
        assert_true(len(self.experiment) ==
                    len(self.subject)*self.experiment.fake_experiments_per_subject)

        # test restricted populate
        assert_false(self.trial, 'table already filled?')
        restriction = dict(subject_id=self.subject.proj().fetch()['subject_id'][0])
        d = self.trial.connection.dependencies
        d.load()
        self.trial.populate(restriction)
        assert_true(self.trial, 'table was not populated')
        key_source = self.trial.key_source
        assert_equal(len(key_source & self.trial), len(key_source & restriction))
        assert_equal(len(key_source - self.trial), len(key_source - restriction))

        # test subtable populate
        assert_false(self.ephys)
        assert_false(self.channel)
        self.ephys.populate()
        assert_true(self.ephys)
        assert_true(self.channel)

    def test_allow_direct_insert(self):
        assert_true(self.subject, 'root tables are empty')
        key = self.subject.fetch('KEY')[0]
        key['experiment_id'] = 1000
        key['experiment_date'] = '2018-10-30'
        self.experiment.insert1(key, allow_direct_insert=True)

    @raises(DataJointError)
    def test_allow_insert(self):
        assert_true(self.subject, 'root tables are empty')
        key = self.subject.fetch('KEY')[0]
        key['experiment_id'] = 1001
        key['experiment_date'] = '2018-10-30'
        self.experiment.insert1(key)
