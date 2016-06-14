from nose.tools import assert_equal, assert_true, raises, assert_list_equal
from . import schema
import datajoint as dj


class TestU:
    """
    Test base relations: insert, delete
    """

    def __init__(self):
        self.user = schema.User()
        self.language = schema.Language()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.Ephys.Channel()
        self.img = schema.Image()
        self.trash = schema.UberTrash()

    def test_restriction(self):
        language_set = {s[1] for s in self.language.contents}
        rel = dj.U('language') & self.language
        assert_list_equal(rel.heading.names, ['language'])
        assert_true(len(rel) == len(language_set))
        assert_true(set(rel.fetch['language']) == language_set)

    def test_ineffective_restriction(self):
        rel = self.language & dj.U('language')
        assert_true(rel.make_sql() == self.language.make_sql())

    def test_join(self):
        rel = self.experiment*dj.U('experiment_date')
        assert_equal(self.experiment.primary_key, ['subject_id', 'experiment_id'])
        assert_equal(rel.primary_key, self.experiment.primary_key + ['experiment_date'])

        rel = dj.U('experiment_date')*self.experiment
        assert_equal(self.experiment.primary_key, ['subject_id', 'experiment_id'])
        assert_equal(rel.primary_key, self.experiment.primary_key + ['experiment_date'])

    @raises(dj.DataJointError)
    def test_invalid_join(self):
        rel = dj.U('language') * dict(language="English")

    # def test_aggregations(self):
    #     rel = dj.U('language').aggregate(n='count(*)')