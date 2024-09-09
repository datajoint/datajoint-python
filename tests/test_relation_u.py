import pytest
import datajoint as dj
from pytest import raises
from .schema import *
from .schema_simple import *


def test_restriction(lang, languages, trial):
    language_set = {s[1] for s in languages}
    rel = dj.U("language") & lang
    assert list(rel.heading.names) == ["language"]
    assert len(rel) == len(language_set)
    assert set(rel.fetch("language")) == language_set
    # Test for issue #342
    rel = trial * dj.U("start_time")
    assert list(rel.primary_key) == trial.primary_key + ["start_time"]
    assert list(rel.primary_key) == list((rel & "trial_id>3").primary_key)
    assert list((dj.U("start_time") & trial).primary_key) == ["start_time"]


def test_invalid_restriction(schema_any):
    with raises(dj.DataJointError):
        result = dj.U("color") & dict(color="red")


def test_ineffective_restriction(lang):
    rel = lang & dj.U("language")
    assert rel.make_sql() == lang.make_sql()


def test_join(experiment):
    rel = experiment * dj.U("experiment_date")
    assert experiment.primary_key == ["subject_id", "experiment_id"]
    assert rel.primary_key == experiment.primary_key + ["experiment_date"]

    rel = dj.U("experiment_date") * experiment
    assert experiment.primary_key == ["subject_id", "experiment_id"]
    assert rel.primary_key == experiment.primary_key + ["experiment_date"]


def test_invalid_join(schema_any):
    with raises(dj.DataJointError):
        rel = dj.U("language") * dict(language="English")


def test_repr_without_attrs(schema_any):
    """test dj.U() display"""
    query = dj.U().aggr(Language, n="count(*)")
    repr(query)


def test_aggregations(schema_any):
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


def test_argmax(schema_any):
    rel = TTest()
    # get the tuples corresponding to the maximum value
    mx = (rel * dj.U().aggr(rel, mx="max(value)")) & "mx=value"
    assert mx.fetch("value")[0] == max(rel.fetch("value"))


def test_aggr(schema_any, schema_simp):
    rel = ArgmaxTest()
    amax1 = (dj.U("val") * rel) & dj.U("secondary_key").aggr(rel, val="min(val)")
    amax2 = (dj.U("val") * rel) * dj.U("secondary_key").aggr(rel, val="min(val)")
    assert (
        len(amax1) == len(amax2) == rel.n
    ), "Aggregated argmax with join and restriction does not yield the same length."
