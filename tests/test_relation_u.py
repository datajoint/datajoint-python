import pytest
import datajoint as dj
from pytest import raises
from .schema import *
from .schema_simple import *


@pytest.fixture(scope="class")
def setup_class(request, schema_any):
    request.cls.user = User()
    request.cls.language = Language()
    request.cls.subject = Subject()
    request.cls.experiment = Experiment()
    request.cls.trial = Trial()
    request.cls.ephys = Ephys()
    request.cls.channel = Ephys.Channel()
    request.cls.img = Image()
    request.cls.trash = UberTrash()

@pytest.mark.skip(reason="temporary")
class TestU:
    """
    Test tables: insert, delete
    """

    def test_restriction(self, setup_class):
        language_set = {s[1] for s in self.language.contents}
        rel = dj.U("language") & self.language
        assert list(rel.heading.names) == ["language"]
        assert len(rel) == len(language_set)
        assert set(rel.fetch("language")) == language_set
        # Test for issue #342
        rel = self.trial * dj.U("start_time")
        assert list(rel.primary_key) == self.trial.primary_key + ["start_time"]
        assert list(rel.primary_key) == list((rel & "trial_id>3").primary_key)
        assert list((dj.U("start_time") & self.trial).primary_key) == ["start_time"]

    def test_invalid_restriction(self, setup_class):
        with raises(dj.DataJointError):
            result = dj.U("color") & dict(color="red")

    def test_ineffective_restriction(self, setup_class):
        rel = self.language & dj.U("language")
        assert rel.make_sql() == self.language.make_sql()

    def test_join(self, setup_class):
        rel = self.experiment * dj.U("experiment_date")
        assert self.experiment.primary_key == ["subject_id", "experiment_id"]
        assert rel.primary_key == self.experiment.primary_key + ["experiment_date"]

        rel = dj.U("experiment_date") * self.experiment
        assert self.experiment.primary_key == ["subject_id", "experiment_id"]
        assert rel.primary_key == self.experiment.primary_key + ["experiment_date"]

    def test_invalid_join(self, setup_class):
        with raises(dj.DataJointError):
            rel = dj.U("language") * dict(language="English")

    def test_repr_without_attrs(self, setup_class):
        """test dj.U() display"""
        query = dj.U().aggr(Language, n="count(*)")
        repr(query)

    def test_aggregations(self, setup_class):
        lang = Language()
        # test total aggregation on expression object
        n1 = dj.U().aggr(lang, n="count(*)").fetch1("n")
        assert n1 == len(lang.fetch())
        # test total aggregation on expression class
        n2 = dj.U().aggr(Language, n="count(*)").fetch1("n")
        assert n1 == n2
        rel = dj.U("language").aggr(Language, number_of_speakers="count(*)")
        assert len(rel) == len(set(l[1] for l in Language.contents))
        assert (rel & 'language="English"').fetch1("number_of_speakers") == 3

    def test_argmax(self, setup_class):
        rel = TTest()
        # get the tuples corresponding to the maximum value
        mx = (rel * dj.U().aggr(rel, mx="max(value)")) & "mx=value"
        assert mx.fetch("value")[0] == max(rel.fetch("value"))

    def test_aggr(self, setup_class, schema_simp):
        rel = ArgmaxTest()
        amax1 = (dj.U("val") * rel) & dj.U("secondary_key").aggr(rel, val="min(val)")
        amax2 = (dj.U("val") * rel) * dj.U("secondary_key").aggr(rel, val="min(val)")
        assert (
            len(amax1) == len(amax2) == rel.n
        ), "Aggregated argmax with join and restriction does not yield the same length."
