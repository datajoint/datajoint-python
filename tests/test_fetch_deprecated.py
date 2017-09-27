from operator import itemgetter
import itertools
from nose.tools import assert_true, raises, assert_equal, assert_dict_equal, assert_in
from datajoint.fetch import Fetch, Fetch1
import numpy as np
import warnings
from . import schema
import datajoint as dj


def check_warning_content(warnings, phrase):
    for w in warnings:
        if phrase.lower() in w.message.args[0].lower():
            return True
    return False


def assert_warning_about(warnings, phrase, message=None):
    if message is None:
        message = "Warning message did not contain phrase {}".format(phrase)
    assert_true(check_warning_content(warnings, phrase), message)


class TestFetchDeprecated:
    """
    Tests deprecated features of fetch for backward compatibility
    """

    def __init__(self):
        self.subject = schema.Subject()
        self.lang = schema.Language()

    def test_getitem_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch[dj.key]
        assert_warning_about(w, "deprecated")

    def test_order_by_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.order_by('subject_id')
        assert_warning_about(w, "deprecated")

    def test_as_dict_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.as_dict()
        assert_warning_about(w, "deprecated")

    def test_fetch_squeeze_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.squeeze()
        assert_warning_about(w, "deprecated")

    def test_fetch1_squeeze_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            (self.subject & 'subject_id = 10').fetch1.squeeze()
        assert_warning_about(w, "deprecated")

    def test_fetch_copy_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.copy()
        assert_warning_about(w, "deprecated")

    def test_fetch1_copy_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            (self.subject & 'subject_id = 10').fetch1.copy()
        assert_warning_about(w, "deprecated")

    def test_limit_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.limit(10)
        assert_warning_about(w, "deprecated")

    def test_offset_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch.offset(10)
        assert_warning_about(w, "deprecated")

    def test_fetch_getitem_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            self.subject.fetch['subject_id']
        assert_warning_about(w, "deprecated")

    def test_fetch1_getitem_deprecation(self):
        with warnings.catch_warnings(record=True) as w:
            (self.subject & 'subject_id = 10').fetch1['subject_id']
        assert_warning_about(w, "deprecated")

    def test_behavior_inheritance(self):
        """Testing behavior property of Fetch objects"""
        mock = {}

        f1 = Fetch(mock)
        assert_in('squeeze', f1.ext_behavior)

        f2 = Fetch1(mock)
        assert_in('squeeze', f2.ext_behavior)

    def test_copy_constructor(self):
        """Test copy constructor for Fetch and Fetch1"""
        mock = {}

        f1 = Fetch(mock).squeeze
        f1.limit(1)
        f2 = Fetch(f1)
        assert_true(isinstance(f2, Fetch), 'Copy constructor is not returning correct object type')
        assert_dict_equal(f1.sql_behavior, f2.sql_behavior, 'SQL behavior dictionary content is not copied correctly')
        assert_dict_equal(f1.ext_behavior, f2.ext_behavior, 'Extra behavior dictionary content is not copied correctly')
        assert_true(f1._relation is f2._relation, 'Relation reference is not copied correctly')

        f3 = Fetch1(mock).squeeze
        f4 = Fetch1(f3)
        assert_true(isinstance(f4, Fetch1), 'Copy constructor is not returning correct object type')
        assert_dict_equal(f3.sql_behavior, f4.sql_behavior, 'Behavior dictionary content is not copied correctly')
        assert_dict_equal(f3.ext_behavior, f4.ext_behavior, 'Extra behavior dictionary content is not copied correctly')

        assert_true(f3._relation is f4._relation, 'Relation reference is not copied correctly')

    def test_getitem(self):
        """Testing Fetch.__getitem__"""
        list1 = sorted(self.subject.proj().fetch.as_dict(), key=itemgetter('subject_id'))
        list2 = sorted(self.subject.fetch[dj.key], key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2,  'Primary key is not returned correctly')

        tmp = self.subject.fetch(order_by=['subject_id'])

        subject_notes, key, real_id = self.subject.fetch['subject_notes', dj.key, 'real_id']

        np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
        list1 = sorted(key, key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2,  'Primary key is not returned correctly')

    def test_getitem_for_fetch1(self):
        """Testing Fetch1.__getitem__"""
        assert_true((self.subject & "subject_id=10").fetch1['subject_id'] == 10)
        assert_equal((self.subject & "subject_id=10").fetch1['subject_id', 'species'],
                     (10, 'monkey'))

    def test_order_by(self):
        """Tests order_by sorting order"""
        languages = schema.Language.contents

        for ord_name, ord_lang in itertools.product(*2 * [['ASC', 'DESC']]):
            cur = self.lang.fetch.order_by('name ' + ord_name, 'language ' + ord_lang)()
            languages.sort(key=itemgetter(1), reverse=ord_lang == 'DESC')
            languages.sort(key=itemgetter(0), reverse=ord_name == 'DESC')
            for c, l in zip(cur, languages):
                assert_true(np.all(cc == ll for cc, ll in zip(c, l)), 'Sorting order is different')

    def test_squeeze(self):
        pass

    def test_order_by_default(self):
        """Tests order_by sorting order with defaults"""
        languages = schema.Language.contents
        cur = self.lang.fetch.order_by('language', 'name DESC')()
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)

        for c, l in zip(cur, languages):
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_limit(self):
        """Test the limit function """
        languages = schema.Language.contents

        cur = self.lang.fetch.limit(4)(order_by=['language', 'name DESC'])
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages))[:4]:
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_limit_offset(self):
        """Test the limit and offset functions together"""
        languages = schema.Language.contents

        cur = self.lang.fetch.offset(2).limit(4)(order_by=['language', 'name DESC'])
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages[2:6])):
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    @raises(dj.DataJointError)
    def test_prepare_attributes(self):
        """Test preparing attributes for getitem"""
        self.lang.fetch[None]

    def test_asdict(self):
        """Test returns as dictionaries"""
        d = self.lang.fetch.as_dict()
        for dd in d:
            assert_true(isinstance(dd, dict))

    def test_offset(self):
        """Tests offset"""
        with warnings.catch_warnings(record=True) as w:
            cur = self.lang.fetch.limit(4).offset(1)(order_by=['language', 'name DESC'])

        languages = self.lang.contents
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages[1:]))[:4]:
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_limit_warning(self):
        """Tests whether warning is raised if offset is used without limit."""
        with warnings.catch_warnings(record=True) as w:
            self.lang.fetch.offset(1)()
        assert_warning_about(w, 'limit')

    @raises(dj.DataJointError)
    def test_fetch1_step2(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1()

    @raises(dj.DataJointError)
    def test_fetch1_step3(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1['name']
