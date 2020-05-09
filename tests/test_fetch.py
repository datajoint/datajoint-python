from nose.tools import assert_true, raises, assert_equal, assert_dict_equal, assert_list_equal, assert_set_equal
from operator import itemgetter
import itertools
import numpy as np
import decimal
import pandas
import warnings
from . import schema
import datajoint as dj
dj.config['enable_python_native_blobs'] = True


class TestFetch:

    @classmethod
    def setup_class(cls):
        cls.subject = schema.Subject()
        cls.lang = schema.Language()

    def test_getattribute(self):
        """Testing Fetch.__call__ with attributes"""
        list1 = sorted(self.subject.proj().fetch(as_dict=True), key=itemgetter('subject_id'))
        list2 = sorted(self.subject.fetch(dj.key), key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2, 'Primary key is not returned correctly')

        tmp = self.subject.fetch(order_by='subject_id')

        subject_notes, key, real_id = self.subject.fetch('subject_notes', dj.key, 'real_id')

        np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
        list1 = sorted(key, key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2, 'Primary key is not returned correctly')

    def test_getattribute_for_fetch1(self):
        """Testing Fetch1.__call__ with attributes"""
        assert_true((self.subject & "subject_id=10").fetch1('subject_id') == 10)
        assert_equal((self.subject & "subject_id=10").fetch1('subject_id', 'species'),
                     (10, 'monkey'))

    def test_order_by(self):
        """Tests order_by sorting order"""
        languages = schema.Language.contents

        for ord_name, ord_lang in itertools.product(*2 * [['ASC', 'DESC']]):
            cur = self.lang.fetch(order_by=('name ' + ord_name, 'language ' + ord_lang))
            languages.sort(key=itemgetter(1), reverse=ord_lang == 'DESC')
            languages.sort(key=itemgetter(0), reverse=ord_name == 'DESC')
            for c, l in zip(cur, languages):
                assert_true(np.all(cc == ll for cc, ll in zip(c, l)), 'Sorting order is different')

    def test_order_by_default(self):
        """Tests order_by sorting order with defaults"""
        languages = schema.Language.contents
        cur = self.lang.fetch(order_by=('language', 'name DESC'))
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        for c, l in zip(cur, languages):
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_limit(self):
        """Test the limit kwarg"""
        limit = 4
        cur = self.lang.fetch(limit=limit)
        assert_equal(len(cur), limit, 'Length is not correct')

    def test_order_by_limit(self):
        """Test the combination of order by and limit kwargs"""
        languages = schema.Language.contents

        cur = self.lang.fetch(limit=4, order_by=['language', 'name DESC'])
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages))[:4]:
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    @staticmethod
    def test_head_tail():
        query = schema.User * schema.Language
        n = 5
        frame = query.head(n, format='frame')
        assert_true(isinstance(frame, pandas.DataFrame))
        array = query.head(n, format='array')
        assert_equal(array.size, n)
        assert_equal(len(frame), n)
        assert_list_equal(query.primary_key, frame.index.names)

        n = 4
        frame = query.tail(n, format='frame')
        array = query.tail(n, format='array')
        assert_equal(array.size, n)
        assert_equal(len(frame), n)
        assert_list_equal(query.primary_key, frame.index.names)

    def test_limit_offset(self):
        """Test the limit and offset kwargs together"""
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
        cur = self.lang.fetch(order_by=['language', 'name DESC'])
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        for (name, lang), (tname, tlang) in list(zip(cur, languages)):
            assert_true(name == tname and lang == tlang, 'Values are not the same')
        # now as dict
        cur = self.lang.fetch(as_dict=True, order_by=('language', 'name DESC'))
        for row, (tname, tlang) in list(zip(cur, languages)):
            assert_true(row['name'] == tname and row['language'] == tlang, 'Values are not the same')

    def test_keys(self):
        """test key fetch"""
        languages = schema.Language.contents
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)

        cur = self.lang.fetch('name', 'language', order_by=('language', 'name DESC'))
        cur2 = list(self.lang.fetch("KEY", order_by=['language', 'name DESC']))

        for c, c2 in zip(zip(*cur), cur2):
            assert_true(c == tuple(c2.values()), 'Values are not the same')

    def test_attributes_as_dict(self):   # issue #595
        attrs = ('species', 'date_of_birth')
        result = self.subject.fetch(*attrs, as_dict=True)
        assert_true(bool(result) and len(result) == len(self.subject))
        assert_set_equal(set(result[0]), set(attrs))

    def test_fetch1_step1(self):
        key = {'name': 'Edgar', 'language': 'Japanese'}
        true = schema.Language.contents[-1]
        dat = (self.lang & key).fetch1()
        for k, (ke, c) in zip(true, dat.items()):
            assert_true(k == c == (self.lang & key).fetch1(ke),
                        'Values are not the same')

    @raises(dj.DataJointError)
    def test_misspelled_attribute(self):
        f = (schema.Language & 'lang = "ENGLISH"').fetch()

    def test_repr(self):
        """Test string representation of fetch, returning table preview"""
        repr = self.subject.fetch.__repr__()
        n = len(repr.strip().split('\n'))
        limit = dj.config['display.limit']
        # 3 lines are used for headers (2) and summary statement (1)
        assert_true(n - 3 <= limit)

    @raises(dj.DataJointError)
    def test_fetch_none(self):
        """Test preparing attributes for getitem"""
        self.lang.fetch(None)

    def test_asdict(self):
        """Test returns as dictionaries"""
        d = self.lang.fetch(as_dict=True)
        for dd in d:
            assert_true(isinstance(dd, dict))

    def test_offset(self):
        """Tests offset"""
        with warnings.catch_warnings(record=True) as w:
            cur = self.lang.fetch(limit=4, offset=1, order_by=['language', 'name DESC'])

        languages = self.lang.contents
        languages.sort(key=itemgetter(0), reverse=True)
        languages.sort(key=itemgetter(1), reverse=False)
        assert_equal(len(cur), 4, 'Length is not correct')
        for c, l in list(zip(cur, languages[1:]))[:4]:
            assert_true(np.all([cc == ll for cc, ll in zip(c, l)]), 'Sorting order is different')

    def test_limit_warning(self):
        """Tests whether warning is raised if offset is used without limit."""
        with warnings.catch_warnings(record=True) as w:
            self.lang.fetch(offset=1)
            assert_true(len(w) > 0, "Warning was not raised")

    def test_len(self):
        """Tests __len__"""
        assert_true(len(self.lang.fetch()) == len(self.lang), '__len__ is not behaving properly')

    @raises(dj.DataJointError)
    def test_fetch1_step2(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1()

    @raises(dj.DataJointError)
    def test_fetch1_step3(self):
        """Tests whether fetch1 raises error"""
        self.lang.fetch1('name')

    def test_decimal(self):
        """Tests that decimal fields are correctly fetched and used in restrictions, see issue #334"""
        rel = schema.DecimalPrimaryKey()
        rel.insert1([decimal.Decimal('3.1415926')])
        keys = rel.fetch()
        assert_true(len(rel & keys[0]) == 1)
        keys = rel.fetch(dj.key)
        assert_true(len(rel & keys[1]) == 1)

    def test_nullable_numbers(self):
        """ test mixture of values and nulls in numeric attributes """
        table = schema.NullableNumbers()
        table.insert((
            (k, np.random.randn(), np.random.randint(-1000, 1000), np.random.randn())
            for k in range(10)))
        table.insert1((100, None, None, None))
        f, d, i = table.fetch('fvalue', 'dvalue', 'ivalue')
        assert_true(None in i)
        assert_true(any(np.isnan(d)))
        assert_true(any(np.isnan(f)))

    def test_fetch_format(self):
        """test fetch_format='frame'"""
        dj.config['fetch_format'] = 'frame'
        # test if lists are both dicts
        list1 = sorted(self.subject.proj().fetch(as_dict=True), key=itemgetter('subject_id'))
        list2 = sorted(self.subject.fetch(dj.key), key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2, 'Primary key is not returned correctly')

        # tests if pandas dataframe
        tmp = self.subject.fetch(order_by='subject_id')
        assert_true(isinstance(tmp, pandas.DataFrame))
        tmp = tmp.to_records()

        subject_notes, key, real_id = self.subject.fetch('subject_notes', dj.key, 'real_id')

        np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
        list1 = sorted(key, key=itemgetter('subject_id'))
        for l1, l2 in zip(list1, list2):
            assert_dict_equal(l1, l2, 'Primary key is not returned correctly')
        # revert configuration of fetch format
        dj.config['fetch_format'] = 'array'
