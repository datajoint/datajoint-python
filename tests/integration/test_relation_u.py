from pytest import raises

import datajoint as dj

from tests.schema import Language, TTest
from tests.schema_simple import ArgmaxTest


def test_restriction(lang, languages, trial):
    """Test dj.U restriction semantics."""
    language_set = {s[1] for s in languages}
    rel = dj.U("language") & lang
    assert list(rel.heading.names) == ["language"]
    assert len(rel) == len(language_set)
    assert set(rel.to_arrays("language")) == language_set
    # dj.U & table promotes attributes to PK
    assert list((dj.U("start_time") & trial).primary_key) == ["start_time"]


def test_invalid_restriction(schema_any):
    with raises(dj.DataJointError):
        dj.U("color") & dict(color="red")


def test_ineffective_restriction(lang):
    rel = lang & dj.U("language")
    assert rel.make_sql() == lang.make_sql()


def test_join_with_u_removed(experiment):
    """Test that table * dj.U(...) raises an error (removed in 2.0)."""
    with raises(dj.DataJointError):
        experiment * dj.U("experiment_date")

    with raises(dj.DataJointError):
        dj.U("experiment_date") * experiment


def test_invalid_join(schema_any):
    """Test that dj.U * non-QueryExpression raises an error."""
    with raises(dj.DataJointError):
        dj.U("language") * dict(language="English")


def test_repr_without_attrs(schema_any):
    """test dj.U() display"""
    query = dj.U().aggr(Language, n="count(*)")
    repr(query)


def test_aggregations(schema_any):
    lang = Language()
    # test total aggregation on expression object
    n1 = dj.U().aggr(lang, n="count(*)").fetch1("n")
    assert n1 == len(lang.to_arrays())
    # test total aggregation on expression class
    n2 = dj.U().aggr(Language, n="count(*)").fetch1("n")
    assert n1 == n2
    rel = dj.U("language").aggr(Language, number_of_speakers="count(*)")
    assert len(rel) == len(set(lang[1] for lang in Language.contents))
    assert (rel & 'language="English"').fetch1("number_of_speakers") == 3


def test_argmax(schema_any):
    """Test argmax pattern using aggregation and restriction."""
    rel = TTest()
    # Get the maximum value using aggregation
    max_val = dj.U().aggr(rel, mx="max(value)").fetch1("mx")
    # Get tuples with that value
    mx = rel & f"value={max_val}"
    assert mx.to_arrays("value")[0] == max(rel.to_arrays("value"))


def test_aggr(schema_any, schema_simp):
    """Test aggregation with dj.U - the old * pattern is removed."""
    rel = ArgmaxTest()
    # The old pattern using dj.U("val") * rel is no longer supported
    # Use aggregation directly instead
    agg = dj.U("secondary_key").aggr(rel, min_val="min(val)")
    # Verify aggregation works
    assert len(agg) > 0
