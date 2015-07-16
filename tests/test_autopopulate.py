from nose.tools import assert_raises, assert_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises

from . import schema


class TestPopulate:
    """
    Test base relations: insert, delete
    """

    def __init__(self):
        self.user = schema.User()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.EphysChannel()

        # delete automatic tables just in case
        self.channel.delete_quick()
        self.ephys.delete_quick()
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
        assert_false(self.trial)
        restriction = dict(subject_id=self.subject.project().fetch()['subject_id'][0])
        self.trial.populate(restriction=restriction)
        assert_true(self.trial)
        assert_equal(len(self.experiment & self.trial), len(self.experiment & restriction))
        assert_equal(len(self.experiment - self.trial), len(self.experiment - restriction))

        # test subtable populate
        assert_false(self.ephys)
        assert_false(self.channel)
        self.ephys.populate()
        assert_true(self.ephys)
        assert_true(self.channel)
