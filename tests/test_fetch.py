from operator import itemgetter
import itertools
from nose.tools import assert_true, raises, assert_equal, assert_dict_equal, assert_in
from datajoint.fetch import Fetch, Fetch1
import numpy as np
import warnings
from . import schema
import datajoint as dj


class TestFetch:

    def __init__(self):
        self.subject = schema.Subject()
        self.lang = schema.Language()

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
        cur = self.lang.fetch.

    def test_order_by_default(self):
        """Tests order_by sorting order with defaults"""
        languages = schema.Language.contents
        cur = self.lang.fetch.order_by('language', 'name DESC')()
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)

        for c, l in zip(cur, languages):
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_order_by_direct(self):
        """Tests order_by sorting order passing it to __call__"""
        languages = schema.Language.contents
        cur = self.lang.fetch(order_by=['language', 'name DESC'])
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

        cur = self.lang.fetch(offset=2, limit=4, order_by=['language', 'name DESC'])
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages[2:6])):
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_iter(self):
        """Test iterator"""
        languages = schema.Language.contents
        cur = self.lang.fetch.order_by('language', 'name DESC')
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        for (name, lang), (tname, tlang) in list(zip(cur, languages)):
            assert_true(name == tname and lang == tlang, 'Values are not the same')
        # now as dict
        cur = self.lang.fetch.as_dict.order_by('language', 'name DESC')
        for row, (tname, tlang) in list(zip(cur, languages)):
            assert_true(row['name'] == tname and row['language'] == tlang, 'Values are not the same')

    def test_keys(self):
        """test key iterator"""
        languages = schema.Language.contents
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)

        cur = self.lang.fetch.order_by('language', 'name DESC')['name', 'language']
        cur2 = list(self.lang.fetch.order_by('language', 'name DESC').keys())

        for c, c2 in zip(zip(*cur), cur2):
            assert_true(c == tuple(c2.values()), 'Values are not the same')

    def test_fetch1_step1(self):
        key = {'name': 'Edgar', 'language': 'Japanese'}
        true = schema.Language.contents[-1]
        dat = (self.lang & key).fetch1()
        for k, (ke, c) in zip(true, dat.items()):
            assert_true(k == c == (self.lang & key).fetch1[ke],
                        'Values are not the same')

    def test_repr(self):
        """Test string representation of fetch, returning table preview"""
        repr = self.subject.fetch.__repr__()
        n = len(repr.strip().split('\n'))
        limit = dj.config['display.limit']
        # 3 lines are used for headers (2) and summary statement (1)
        assert_true(n - 3 <= limit)

    @raises(dj.DataJointError)
    def test_prepare_attributes(self):
        """Test preparing attributes for getitem"""
        self.lang.fetch[None]

    def test_asdict(self):
        """Test returns as dictionaries"""
        d = self.lang.fetch.as_dict()
        for dd in d:
            assert_true(isinstance(dd, dict))

    def test_asdict_with_call(self):
        """Test returns as dictionaries with call."""
        d = self.lang.fetch.as_dict()
        for dd in d:
            assert_true(isinstance(dd, dict))

    def test_offset(self):
        """Tests offset"""
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
            assert_true(len(w) > 0, "Warning war not raised")

    def test_len(self):
        """Tests __len__"""
        assert_true(len(self.lang.fetch) == len(self.lang), '__len__ is not behaving properly')

    @raises(dj.DataJointError)
    def test_fetch1_step2(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1()

    @raises(dj.DataJointError)
    def test_fetch1_step3(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1['name']
