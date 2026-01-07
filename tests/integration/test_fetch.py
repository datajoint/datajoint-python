"""Tests for the modern fetch API: to_dicts, to_pandas, to_arrays, keys, fetch1"""

import decimal
import itertools
import os
import shutil
from operator import itemgetter

import numpy as np
import pandas
import pytest

import datajoint as dj

from tests import schema


def test_getattribute(subject):
    """Testing fetch with attributes using new API"""
    list1 = sorted(subject.proj().to_dicts(), key=itemgetter("subject_id"))
    list2 = sorted(subject.keys(), key=itemgetter("subject_id"))
    for l1, l2 in zip(list1, list2):
        assert l1 == l2, "Primary key is not returned correctly"

    tmp = subject.to_arrays(order_by="subject_id")

    subject_notes, real_id = subject.to_arrays("subject_notes", "real_id")

    np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp["subject_notes"]))
    np.testing.assert_array_equal(sorted(real_id), sorted(tmp["real_id"]))


def test_getattribute_for_fetch1(subject):
    """Testing Fetch1.__call__ with attributes"""
    assert (subject & "subject_id=10").fetch1("subject_id") == 10
    assert (subject & "subject_id=10").fetch1("subject_id", "species") == (
        10,
        "monkey",
    )


def test_order_by(lang, languages):
    """Tests order_by sorting order"""
    for ord_name, ord_lang in itertools.product(*2 * [["ASC", "DESC"]]):
        cur = lang.to_arrays(order_by=("name " + ord_name, "language " + ord_lang))
        languages.sort(key=itemgetter(1), reverse=ord_lang == "DESC")
        languages.sort(key=itemgetter(0), reverse=ord_name == "DESC")
        for c, l in zip(cur, languages):  # noqa: E741
            assert np.all(cc == ll for cc, ll in zip(c, l)), "Sorting order is different"


def test_order_by_default(lang, languages):
    """Tests order_by sorting order with defaults"""
    cur = lang.to_arrays(order_by=("language", "name DESC"))
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    for c, l in zip(cur, languages):  # noqa: E741
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_limit(lang):
    """Test the limit kwarg"""
    limit = 4
    cur = lang.to_arrays(limit=limit)
    assert len(cur) == limit, "Length is not correct"


def test_order_by_limit(lang, languages):
    """Test the combination of order by and limit kwargs"""
    cur = lang.to_arrays(limit=4, order_by=["language", "name DESC"])
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages))[:4]:  # noqa: E741
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_head_tail(schema_any):
    """Test head() and tail() convenience methods"""
    query = schema.User * schema.Language
    n = 5
    # head and tail now return list of dicts
    head_result = query.head(n)
    assert isinstance(head_result, list)
    assert len(head_result) == n
    assert all(isinstance(row, dict) for row in head_result)

    n = 4
    tail_result = query.tail(n)
    assert isinstance(tail_result, list)
    assert len(tail_result) == n
    assert all(isinstance(row, dict) for row in tail_result)


def test_limit_offset(lang, languages):
    """Test the limit and offset kwargs together"""
    cur = lang.to_arrays(offset=2, limit=4, order_by=["language", "name DESC"])
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages[2:6])):  # noqa: E741
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_iter(lang, languages):
    """Test iterator - now lazy streaming"""
    languages_copy = languages.copy()
    languages_copy.sort(key=itemgetter(0), reverse=True)
    languages_copy.sort(key=itemgetter(1), reverse=False)

    # Iteration now yields dicts directly
    result = list(lang.to_dicts(order_by=["language", "name DESC"]))
    for row, (tname, tlang) in list(zip(result, languages_copy)):
        assert row["name"] == tname and row["language"] == tlang, "Values are not the same"


def test_keys(lang, languages):
    """test key fetch"""
    languages_copy = languages.copy()
    languages_copy.sort(key=itemgetter(0), reverse=True)
    languages_copy.sort(key=itemgetter(1), reverse=False)

    # Use to_arrays for attribute fetch
    cur = lang.to_arrays("name", "language", order_by=("language", "name DESC"))
    # Use keys() for primary key fetch
    cur2 = list(lang.keys(order_by=["language", "name DESC"]))

    for c, c2 in zip(zip(*cur), cur2):
        assert c == tuple(c2.values()), "Values are not the same"


def test_fetch1_step1(lang, languages):
    assert (
        lang.contents
        == languages
        == [
            ("Fabian", "English"),
            ("Edgar", "English"),
            ("Dimitri", "English"),
            ("Dimitri", "Ukrainian"),
            ("Fabian", "German"),
            ("Edgar", "Japanese"),
        ]
    ), "Unexpected contents in Language table"
    key = {"name": "Edgar", "language": "Japanese"}
    true = languages[-1]
    dat = (lang & key).fetch1()
    for k, (ke, c) in zip(true, dat.items()):
        assert k == c == (lang & key).fetch1(ke), "Values are not the same"


def test_misspelled_attribute(schema_any):
    """Test that misspelled attributes raise error"""
    with pytest.raises(dj.DataJointError):
        (schema.Language & 'lang = "ENGLISH"').to_dicts()


def test_to_dicts(lang):
    """Test to_dicts returns list of dictionaries"""
    d = lang.to_dicts()
    for dd in d:
        assert isinstance(dd, dict)


def test_offset(lang, languages):
    """Tests offset"""
    cur = lang.to_arrays(limit=4, offset=1, order_by=["language", "name DESC"])

    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages[1:]))[:4]:  # noqa: E741
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_len(lang):
    """Tests __len__"""
    assert len(lang.to_arrays()) == len(lang), "__len__ is not behaving properly"


def test_fetch1_step2(lang):
    """Tests whether fetch1 raises error for multiple rows"""
    with pytest.raises(dj.DataJointError):
        lang.fetch1()


def test_fetch1_step3(lang):
    """Tests whether fetch1 raises error for multiple rows with attribute"""
    with pytest.raises(dj.DataJointError):
        lang.fetch1("name")


def test_decimal(schema_any):
    """Tests that decimal fields are correctly fetched and used in restrictions, see issue #334"""
    rel = schema.DecimalPrimaryKey()
    assert len(rel.to_arrays()), "Table DecimalPrimaryKey contents are empty"
    rel.insert1([decimal.Decimal("3.1415926")])
    keys = rel.to_arrays()
    assert len(keys) > 0
    assert len(rel & keys[0]) == 1
    keys = rel.keys()
    assert len(keys) >= 2
    assert len(rel & keys[1]) == 1


def test_nullable_numbers(schema_any):
    """test mixture of values and nulls in numeric attributes"""
    table = schema.NullableNumbers()
    table.insert(
        (
            (
                k,
                np.random.randn(),
                np.random.randint(-1000, 1000),
                np.random.randn(),
            )
            for k in range(10)
        )
    )
    table.insert1((100, None, None, None))
    f, d, i = table.to_arrays("fvalue", "dvalue", "ivalue")
    # Check for None in integer column
    assert None in i
    # Check for None or nan in float columns (None may be returned for nullable fields)
    assert any(v is None or (isinstance(v, float) and np.isnan(v)) for v in d)
    assert any(v is None or (isinstance(v, float) and np.isnan(v)) for v in f)


def test_to_pandas(subject):
    """Test to_pandas returns DataFrame with primary key as index"""
    df = subject.to_pandas(order_by="subject_id")
    assert isinstance(df, pandas.DataFrame)
    assert df.index.names == subject.primary_key


def test_to_polars(subject):
    """Test to_polars returns polars DataFrame"""
    polars = pytest.importorskip("polars")
    df = subject.to_polars()
    assert isinstance(df, polars.DataFrame)


def test_to_arrow(subject):
    """Test to_arrow returns PyArrow Table"""
    pyarrow = pytest.importorskip("pyarrow")
    table = subject.to_arrow()
    assert isinstance(table, pyarrow.Table)


def test_same_secondary_attribute(schema_any):
    children = (schema.Child * schema.Parent().proj()).to_arrays()["name"]
    assert len(children) == 1
    assert children[0] == "Dan"


def test_query_caching(schema_any):
    """Test query caching with to_arrays"""
    # initialize cache directory
    os.makedirs(os.path.expanduser("~/dj_query_cache"), exist_ok=True)

    with dj.config.override(query_cache=os.path.expanduser("~/dj_query_cache")):
        conn = schema.TTest3.connection
        # insert sample data and load cache
        schema.TTest3.insert([dict(key=100 + i, value=200 + i) for i in range(2)])
        conn.set_query_cache(query_cache="main")
        cached_res = schema.TTest3().to_arrays()
        # attempt to insert while caching enabled
        try:
            schema.TTest3.insert([dict(key=200 + i, value=400 + i) for i in range(2)])
            assert False, "Insert allowed while query caching enabled"
        except dj.DataJointError:
            conn.set_query_cache()
        # insert new data
        schema.TTest3.insert([dict(key=600 + i, value=800 + i) for i in range(2)])
        # re-enable cache to access old results
        conn.set_query_cache(query_cache="main")
        previous_cache = schema.TTest3().to_arrays()
        # verify properly cached and how to refresh results
        assert all([c == p for c, p in zip(cached_res, previous_cache)])
        conn.set_query_cache()
        uncached_res = schema.TTest3().to_arrays()
        assert len(uncached_res) > len(cached_res)
        # purge query cache
        conn.purge_query_cache()

    # reset cache directory state
    shutil.rmtree(os.path.expanduser("~/dj_query_cache"), ignore_errors=True)


def test_fetch_group_by(schema_any):
    """
    https://github.com/datajoint/datajoint-python/issues/914
    """
    assert schema.Parent().keys(order_by="name") == [{"parent_id": 1}]


def test_dj_u_distinct(schema_any):
    """
    Test developed to see if removing DISTINCT from the select statement
    generation breaks the dj.U universal set implementation
    """

    # Contents to be inserted
    contents = [(1, 2, 3), (2, 2, 3), (3, 3, 2), (4, 5, 5)]
    schema.Stimulus.insert(contents)

    # Query the whole table
    test_query = schema.Stimulus()

    # Use dj.U to create a list of unique contrast and brightness combinations
    result = dj.U("contrast", "brightness") & test_query
    expected_result = [
        {"contrast": 2, "brightness": 3},
        {"contrast": 3, "brightness": 2},
        {"contrast": 5, "brightness": 5},
    ]

    fetched_result = result.to_dicts(order_by=("contrast", "brightness"))
    schema.Stimulus.delete_quick()
    assert fetched_result == expected_result


def test_backslash(schema_any):
    """
    https://github.com/datajoint/datajoint-python/issues/999
    """
    expected = "She\\Hulk"
    schema.Parent.insert([(2, expected)])
    q = schema.Parent & dict(name=expected)
    assert q.fetch1("name") == expected
    q.delete()


def test_lazy_iteration(lang, languages):
    """Test that iteration is lazy (uses generator)"""
    # The new iteration is a generator
    iter_obj = iter(lang)
    # Should be a generator
    import types

    assert isinstance(iter_obj, types.GeneratorType)

    # Each item should be a dict
    first = next(iter_obj)
    assert isinstance(first, dict)
    assert "name" in first and "language" in first


def test_to_arrays_include_key(lang, languages):
    """Test to_arrays with include_key=True returns keys as list of dicts"""
    # Fetch with include_key=True
    keys, names, langs = lang.to_arrays("name", "language", include_key=True, order_by="KEY")

    # keys should be a list of dicts with primary key columns
    assert isinstance(keys, list)
    assert all(isinstance(k, dict) for k in keys)
    assert all(set(k.keys()) == {"name", "language"} for k in keys)

    # names and langs should be numpy arrays
    assert isinstance(names, np.ndarray)
    assert isinstance(langs, np.ndarray)

    # Length should match
    assert len(keys) == len(names) == len(langs) == len(languages)

    # Keys should match the data
    for key, name, language in zip(keys, names, langs):
        assert key["name"] == name
        assert key["language"] == language

    # Keys should be usable for restrictions
    first_key = keys[0]
    restricted = lang & first_key
    assert len(restricted) == 1
    assert restricted.fetch1("name") == first_key["name"]


def test_to_arrays_include_key_single_attr(subject):
    """Test to_arrays include_key with single attribute"""
    keys, species = subject.to_arrays("species", include_key=True)

    assert isinstance(keys, list)
    assert isinstance(species, np.ndarray)
    assert len(keys) == len(species)

    # Verify keys have only primary key columns
    assert all("subject_id" in k for k in keys)


def test_to_arrays_without_include_key(lang):
    """Test that to_arrays without include_key doesn't return keys"""
    result = lang.to_arrays("name", "language")

    # Should return tuple of arrays, not (keys, ...)
    assert isinstance(result, tuple)
    assert len(result) == 2
    names, langs = result
    assert isinstance(names, np.ndarray)
    assert isinstance(langs, np.ndarray)
