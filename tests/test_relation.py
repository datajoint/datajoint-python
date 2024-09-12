import pytest
from inspect import getmembers
import re
import pandas
import numpy as np
import datajoint as dj
from datajoint.table import Table
from unittest.mock import patch
from . import schema


def test_contents(user, subject):
    """
    test the ability of tables to self-populate using the contents property
    """
    # test contents
    assert user
    assert len(user) == len(user.contents)
    u = user.fetch(order_by=["username"])
    assert list(u["username"]) == sorted([s[0] for s in user.contents])

    # test prepare
    assert subject
    assert len(subject) == len(subject.contents)
    u = subject.fetch(order_by=["subject_id"])
    assert list(u["subject_id"]) == sorted([s[0] for s in subject.contents])


def test_misnamed_attribute1(user):
    with pytest.raises(dj.DataJointError):
        user.insert([dict(username="Bob"), dict(user="Alice")])


def test_misnamed_attribute2(user):
    with pytest.raises(KeyError):
        user.insert1(dict(user="Bob"))


def test_extra_attribute1(user):
    with pytest.raises(KeyError):
        user.insert1(dict(username="Robert", spouse="Alice"))


def test_extra_attribute2(user):
    user.insert1(dict(username="Robert", spouse="Alice"), ignore_extra_fields=True)


def test_missing_definition(schema_any):
    class MissingDefinition(dj.Manual):
        definitions = """  # misspelled definition
        id : int
        ---
        comment : varchar(16)  # otherwise everything's normal
        """

    with pytest.raises(NotImplementedError):
        schema_any(MissingDefinition, context=dict(MissingDefinition=MissingDefinition))


def test_empty_insert1(user):
    with pytest.raises(dj.DataJointError):
        user.insert1(())


def test_empty_insert(user):
    with pytest.raises(dj.DataJointError):
        user.insert([()])


def test_wrong_arguments_insert(user):
    with pytest.raises(dj.DataJointError):
        user.insert1(("First", "Second"))


def test_wrong_insert_type(user):
    with pytest.raises(dj.DataJointError):
        user.insert1(3)


def test_insert_select(subject, test, test2):
    test2.delete()
    test2.insert(test)
    assert len(test2) == len(test)

    original_length = len(subject)
    elements = subject.proj(..., s="subject_id")
    elements = elements.proj(
        "real_id",
        "date_of_birth",
        "subject_notes",
        subject_id="s+1000",
        species='"human"',
    )
    subject.insert(elements, ignore_extra_fields=True)
    assert len(subject) == 2 * original_length


def test_insert_pandas_roundtrip(test, test2):
    """ensure fetched frames can be inserted"""
    test2.delete()
    n = len(test)
    assert n > 0
    df = test.fetch(format="frame")
    assert isinstance(df, pandas.DataFrame)
    assert len(df) == n
    test2.insert(df)
    assert len(test2) == n


def test_insert_pandas_userframe(test, test2):
    """
    ensure simple user-created frames (1 field, non-custom index)
    can be inserted without extra index adjustment
    """
    test2.delete()
    n = len(test)
    assert n > 0
    df = pandas.DataFrame(test.fetch())
    assert isinstance(df, pandas.DataFrame)
    assert len(df) == n
    test2.insert(df)
    assert len(test2) == n


def test_insert_select_ignore_extra_fields0(test, test_extra):
    """need ignore extra fields for insert select"""
    test_extra.insert1((test.fetch("key").max() + 1, 0, 0))
    with pytest.raises(dj.DataJointError):
        test.insert(test_extra)


def test_insert_select_ignore_extra_fields1(test, test_extra):
    """make sure extra fields works in insert select"""
    test_extra.delete()
    keyno = test.fetch("key").max() + 1
    test_extra.insert1((keyno, 0, 0))
    test.insert(test_extra, ignore_extra_fields=True)
    assert keyno in test.fetch("key")


def test_insert_select_ignore_extra_fields2(test_no_extra, test):
    """make sure insert select still works when ignoring extra fields when there are none"""
    test_no_extra.delete()
    test_no_extra.insert(test, ignore_extra_fields=True)


def test_insert_select_ignore_extra_fields3(test, test_no_extra, test_extra):
    """make sure insert select works for from query result"""
    # Recreate table state from previous tests
    keyno = test.fetch("key").max() + 1
    test_extra.insert1((keyno, 0, 0))
    test.insert(test_extra, ignore_extra_fields=True)

    assert len(test_extra.fetch("key")), "test_extra is empty"
    test_no_extra.delete()
    assert len(test_extra.fetch("key")), "test_extra is empty"
    keystr = str(test_extra.fetch("key").max())
    test_no_extra.insert((test_extra & "`key`=" + keystr), ignore_extra_fields=True)


def test_skip_duplicates(test_no_extra, test):
    """test that skip_duplicates works when inserting from another table"""
    test_no_extra.delete()
    test_no_extra.insert(test, ignore_extra_fields=True, skip_duplicates=True)
    test_no_extra.insert(test, ignore_extra_fields=True, skip_duplicates=True)


def test_replace(subject):
    """
    Test replacing or ignoring duplicate entries
    """
    key = dict(subject_id=7)
    date = "2015-01-01"
    subject.insert1(dict(key, real_id=7, date_of_birth=date, subject_notes=""))
    assert date == str((subject & key).fetch1("date_of_birth")), "incorrect insert"
    date = "2015-01-02"
    subject.insert1(
        dict(key, real_id=7, date_of_birth=date, subject_notes=""),
        skip_duplicates=True,
    )
    assert date != str((subject & key).fetch1("date_of_birth")), "inappropriate replace"
    subject.insert1(
        dict(key, real_id=7, date_of_birth=date, subject_notes=""), replace=True
    )
    assert date == str((subject & key).fetch1("date_of_birth")), "replace failed"


def test_delete_quick(subject):
    """Tests quick deletion"""
    tmp = np.array(
        [
            (2, "Klara", "monkey", "2010-01-01", ""),
            (1, "Peter", "mouse", "2015-01-01", ""),
        ],
        dtype=subject.heading.as_dtype,
    )
    subject.insert(tmp)
    s = subject & ("subject_id in (%s)" % ",".join(str(r) for r in tmp["subject_id"]))
    assert len(s) == 2, "insert did not work."
    s.delete_quick()
    assert len(s) == 0, "delete did not work."


def test_skip_duplicate(subject):
    """Tests if duplicates are properly skipped."""
    tmp = np.array(
        [
            (2, "Klara", "monkey", "2010-01-01", ""),
            (1, "Peter", "mouse", "2015-01-01", ""),
        ],
        dtype=subject.heading.as_dtype,
    )
    subject.insert(tmp)
    tmp = np.array(
        [
            (2, "Klara", "monkey", "2010-01-01", ""),
            (1, "Peter", "mouse", "2015-01-01", ""),
        ],
        dtype=subject.heading.as_dtype,
    )
    subject.insert(tmp, skip_duplicates=True)


def test_not_skip_duplicate(subject):
    """Tests if duplicates are not skipped."""
    tmp = np.array(
        [
            (2, "Klara", "monkey", "2010-01-01", ""),
            (2, "Klara", "monkey", "2010-01-01", ""),
            (1, "Peter", "mouse", "2015-01-01", ""),
        ],
        dtype=subject.heading.as_dtype,
    )
    with pytest.raises(dj.errors.DuplicateError):
        subject.insert(tmp, skip_duplicates=False)


def test_no_error_suppression(test):
    """skip_duplicates=True should not suppress other errors"""
    with pytest.raises(dj.errors.MissingAttributeError):
        test.insert([dict(key=100)], skip_duplicates=True)


def test_blob_insert(img):
    """Tests inserting and retrieving blobs."""
    X = np.random.randn(20, 10)
    img.insert1((1, X))
    Y = img.fetch()[0]["img"]
    assert np.all(X == Y), "Inserted and retrieved image are not identical"


def test_drop(trash):
    """Tests dropping tables"""
    dj.config["safemode"] = True
    with patch.object(dj.utils, "input", create=True, return_value="yes"):
        trash.drop()
    try:
        trash.fetch()
        raise Exception("Fetched after table dropped.")
    except dj.DataJointError:
        pass
    finally:
        dj.config["safemode"] = False


def test_table_regexp(schema_any):
    """Test whether table names are matched by regular expressions"""

    def relation_selector(attr):
        try:
            return issubclass(attr, Table)
        except TypeError:
            return False

    tiers = [dj.Imported, dj.Manual, dj.Lookup, dj.Computed]
    for name, rel in getmembers(schema, relation_selector):
        assert re.match(
            rel.tier_regexp, rel.table_name
        ), "Regular expression does not match for {name}".format(name=name)
        for tier in tiers:
            assert issubclass(rel, tier) or not re.match(
                tier.tier_regexp, rel.table_name
            ), "Regular expression matches for {name} but should not".format(name=name)


def test_table_size(experiment):
    """test getting the size of the table and its indices in bytes"""
    number_of_bytes = experiment.size_on_disk
    assert isinstance(number_of_bytes, int) and number_of_bytes > 100


def test_repr_html(ephys):
    assert ephys._repr_html_().strip().startswith("<style")
