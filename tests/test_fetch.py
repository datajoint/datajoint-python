import pytest
from typing import List
from operator import itemgetter
import itertools
import numpy as np
import decimal
import pandas
import warnings
from . import schema
import datajoint as dj
import os
import logging
import io


def test_getattribute(subject):
    """Testing Fetch.__call__ with attributes"""
    list1 = sorted(subject.proj().fetch(as_dict=True), key=itemgetter("subject_id"))
    list2 = sorted(subject.fetch(dj.key), key=itemgetter("subject_id"))
    for l1, l2 in zip(list1, list2):
        assert l1 == l2, "Primary key is not returned correctly"

    tmp = subject.fetch(order_by="subject_id")

    subject_notes, key, real_id = subject.fetch("subject_notes", dj.key, "real_id")

    np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp["subject_notes"]))
    np.testing.assert_array_equal(sorted(real_id), sorted(tmp["real_id"]))
    list1 = sorted(key, key=itemgetter("subject_id"))
    for l1, l2 in zip(list1, list2):
        assert l1 == l2, "Primary key is not returned correctly"


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
        cur = lang.fetch(order_by=("name " + ord_name, "language " + ord_lang))
        languages.sort(key=itemgetter(1), reverse=ord_lang == "DESC")
        languages.sort(key=itemgetter(0), reverse=ord_name == "DESC")
        for c, l in zip(cur, languages):
            assert np.all(
                cc == ll for cc, ll in zip(c, l)
            ), "Sorting order is different"


def test_order_by_default(lang, languages):
    """Tests order_by sorting order with defaults"""
    cur = lang.fetch(order_by=("language", "name DESC"))
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    for c, l in zip(cur, languages):
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_limit(lang):
    """Test the limit kwarg"""
    limit = 4
    cur = lang.fetch(limit=limit)
    assert len(cur) == limit, "Length is not correct"


def test_order_by_limit(lang, languages):
    """Test the combination of order by and limit kwargs"""
    cur = lang.fetch(limit=4, order_by=["language", "name DESC"])
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages))[:4]:
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_head_tail(schema_any):
    query = schema.User * schema.Language
    n = 5
    frame = query.head(n, format="frame")
    assert isinstance(frame, pandas.DataFrame)
    array = query.head(n, format="array")
    assert array.size == n
    assert len(frame) == n
    assert query.primary_key == frame.index.names

    n = 4
    frame = query.tail(n, format="frame")
    array = query.tail(n, format="array")
    assert array.size == n
    assert len(frame) == n
    assert query.primary_key == frame.index.names


def test_limit_offset(lang, languages):
    """Test the limit and offset kwargs together"""
    cur = lang.fetch(offset=2, limit=4, order_by=["language", "name DESC"])
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages[2:6])):
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_iter(lang, languages):
    """Test iterator"""
    cur = lang.fetch(order_by=["language", "name DESC"])
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    for (name, lang_val), (tname, tlang) in list(zip(cur, languages)):
        assert name == tname and lang_val == tlang, "Values are not the same"
    # now as dict
    cur = lang.fetch(as_dict=True, order_by=("language", "name DESC"))
    for row, (tname, tlang) in list(zip(cur, languages)):
        assert (
            row["name"] == tname and row["language"] == tlang
        ), "Values are not the same"


def test_keys(lang, languages):
    """test key fetch"""
    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)

    lang = schema.Language()
    cur = lang.fetch("name", "language", order_by=("language", "name DESC"))
    cur2 = list(lang.fetch("KEY", order_by=["language", "name DESC"]))

    for c, c2 in zip(zip(*cur), cur2):
        assert c == tuple(c2.values()), "Values are not the same"


def test_attributes_as_dict(subject):
    """
    Issue #595
    """
    attrs = ("species", "date_of_birth")
    result = subject.fetch(*attrs, as_dict=True)
    assert bool(result) and len(result) == len(subject)
    assert set(result[0]) == set(attrs)


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
    with pytest.raises(dj.DataJointError):
        f = (schema.Language & 'lang = "ENGLISH"').fetch()


def test_repr(subject):
    """Test string representation of fetch, returning table preview"""
    repr = subject.fetch.__repr__()
    n = len(repr.strip().split("\n"))
    limit = dj.config["display.limit"]
    # 3 lines are used for headers (2) and summary statement (1)
    assert n - 3 <= limit


def test_fetch_none(lang):
    """Test preparing attributes for getitem"""
    with pytest.raises(dj.DataJointError):
        lang.fetch(None)


def test_asdict(lang):
    """Test returns as dictionaries"""
    d = lang.fetch(as_dict=True)
    for dd in d:
        assert isinstance(dd, dict)


def test_offset(lang, languages):
    """Tests offset"""
    cur = lang.fetch(limit=4, offset=1, order_by=["language", "name DESC"])

    languages.sort(key=itemgetter(0), reverse=True)
    languages.sort(key=itemgetter(1), reverse=False)
    assert len(cur) == 4, "Length is not correct"
    for c, l in list(zip(cur, languages[1:]))[:4]:
        assert np.all([cc == ll for cc, ll in zip(c, l)]), "Sorting order is different"


def test_limit_warning(lang):
    """Tests whether warning is raised if offset is used without limit."""
    logger = logging.getLogger("datajoint")
    log_capture = io.StringIO()
    stream_handler = logging.StreamHandler(log_capture)
    log_format = logging.Formatter(
        "[%(asctime)s][%(funcName)s][%(levelname)s]: %(message)s"
    )
    stream_handler.setFormatter(log_format)
    stream_handler.set_name("test_limit_warning")
    logger.addHandler(stream_handler)
    lang.fetch(offset=1)

    log_contents = log_capture.getvalue()
    log_capture.close()

    for handler in logger.handlers:  # Clean up handler
        if handler.name == "test_limit_warning":
            logger.removeHandler(handler)
    assert "[WARNING]: Offset set, but no limit." in log_contents


def test_len(lang):
    """Tests __len__"""
    assert len(lang.fetch()) == len(lang), "__len__ is not behaving properly"


def test_fetch1_step2(lang):
    """Tests whether fetch1 raises error"""
    with pytest.raises(dj.DataJointError):
        lang.fetch1()


def test_fetch1_step3(lang):
    """Tests whether fetch1 raises error"""
    with pytest.raises(dj.DataJointError):
        lang.fetch1("name")


def test_decimal(schema_any):
    """Tests that decimal fields are correctly fetched and used in restrictions, see issue #334"""
    rel = schema.DecimalPrimaryKey()
    assert len(rel.fetch()), "Table DecimalPrimaryKey contents are empty"
    rel.insert1([decimal.Decimal("3.1415926")])
    keys = rel.fetch()
    assert len(keys) > 0
    assert len(rel & keys[0]) == 1
    keys = rel.fetch(dj.key)
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
    f, d, i = table.fetch("fvalue", "dvalue", "ivalue")
    assert None in i
    assert any(np.isnan(d))
    assert any(np.isnan(f))


def test_fetch_format(subject):
    """test fetch_format='frame'"""
    with dj.config(fetch_format="frame"):
        # test if lists are both dicts
        list1 = sorted(subject.proj().fetch(as_dict=True), key=itemgetter("subject_id"))
        list2 = sorted(subject.fetch(dj.key), key=itemgetter("subject_id"))
        for l1, l2 in zip(list1, list2):
            assert l1 == l2, "Primary key is not returned correctly"

        # tests if pandas dataframe
        tmp = subject.fetch(order_by="subject_id")
        assert isinstance(tmp, pandas.DataFrame)
        tmp = tmp.to_records()

        subject_notes, key, real_id = subject.fetch("subject_notes", dj.key, "real_id")

        np.testing.assert_array_equal(
            sorted(subject_notes), sorted(tmp["subject_notes"])
        )
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp["real_id"]))
        list1 = sorted(key, key=itemgetter("subject_id"))
        for l1, l2 in zip(list1, list2):
            assert l1 == l2, "Primary key is not returned correctly"


def test_key_fetch1(subject):
    """test KEY fetch1 - issue #976"""
    with dj.config(fetch_format="array"):
        k1 = (subject & "subject_id=10").fetch1("KEY")
    with dj.config(fetch_format="frame"):
        k2 = (subject & "subject_id=10").fetch1("KEY")
    assert k1 == k2


def test_same_secondary_attribute(schema_any):
    children = (schema.Child * schema.Parent().proj()).fetch()["name"]
    assert len(children) == 1
    assert children[0] == "Dan"


def test_query_caching(schema_any):
    # initialize cache directory
    os.mkdir(os.path.expanduser("~/dj_query_cache"))

    with dj.config(query_cache=os.path.expanduser("~/dj_query_cache")):
        conn = schema.TTest3.connection
        # insert sample data and load cache
        schema.TTest3.insert([dict(key=100 + i, value=200 + i) for i in range(2)])
        conn.set_query_cache(query_cache="main")
        cached_res = schema.TTest3().fetch()
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
        previous_cache = schema.TTest3().fetch()
        # verify properly cached and how to refresh results
        assert all([c == p for c, p in zip(cached_res, previous_cache)])
        conn.set_query_cache()
        uncached_res = schema.TTest3().fetch()
        assert len(uncached_res) > len(cached_res)
        # purge query cache
        conn.purge_query_cache()

    # reset cache directory state (will fail if purge was unsuccessful)
    os.rmdir(os.path.expanduser("~/dj_query_cache"))


def test_fetch_group_by(schema_any):
    """
    https://github.com/datajoint/datajoint-python/issues/914
    """

    assert schema.Parent().fetch("KEY", order_by="name") == [{"parent_id": 1}]


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

    fetched_result = result.fetch(as_dict=True, order_by=("contrast", "brightness"))
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
