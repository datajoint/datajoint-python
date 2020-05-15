from inspect import getmembers
import re
import pandas
import numpy as np
from nose.tools import assert_equal, assert_not_equal, assert_true, assert_list_equal, raises
import datajoint as dj
from datajoint.table import Table
from unittest.mock import patch

from . import schema


def relation_selector(attr):
    try:
        return issubclass(attr, Table)
    except TypeError:
        return False


class TestRelation:
    """
    Test base relations: insert, delete
    """

    @classmethod
    def setup_class(cls):
        cls.test = schema.TTest()
        cls.test_extra = schema.TTestExtra()
        cls.test_no_extra = schema.TTestNoExtra()
        cls.user = schema.User()
        cls.subject = schema.Subject()
        cls.experiment = schema.Experiment()
        cls.trial = schema.Trial()
        cls.ephys = schema.Ephys()
        cls.channel = schema.Ephys.Channel()
        cls.img = schema.Image()
        cls.trash = schema.UberTrash()

    def test_contents(self):
        """
        test the ability of tables to self-populate using the contents property
        """
        # test contents
        assert_true(self.user)
        assert_true(len(self.user) == len(self.user.contents))
        u = self.user.fetch(order_by=['username'])
        assert_list_equal(list(u['username']), sorted([s[0] for s in self.user.contents]))

        # test prepare
        assert_true(self.subject)
        assert_true(len(self.subject) == len(self.subject.contents))
        u = self.subject.fetch(order_by=['subject_id'])
        assert_list_equal(list(u['subject_id']), sorted([s[0] for s in self.subject.contents]))

    @raises(dj.DataJointError)
    def test_misnamed_attribute1(self):
        self.user.insert([dict(username="Bob"), dict(user="Alice")])

    @raises(KeyError)
    def test_misnamed_attribute2(self):
        self.user.insert1(dict(user="Bob"))

    @raises(KeyError)
    def test_extra_attribute1(self):
        self.user.insert1(dict(username="Robert", spouse="Alice"))

    def test_extra_attribute2(self):
        self.user.insert1(dict(username="Robert", spouse="Alice"), ignore_extra_fields=True)

    @raises(NotImplementedError)
    def test_missing_definition(self):
        @schema.schema
        class MissingDefinition(dj.Manual):
            definitions = """  # misspelled definition
            id : int
            ---
            comment : varchar(16)  # otherwise everything's normal
            """

    @raises(dj.DataJointError)
    def test_empty_insert1(self):
        self.user.insert1(())

    @raises(dj.DataJointError)
    def test_empty_insert(self):
        self.user.insert([()])

    @raises(dj.DataJointError)
    def test_wrong_arguments_insert(self):
        self.user.insert1(('First', 'Second'))

    @raises(dj.DataJointError)
    def test_wrong_insert_type(self):
        self.user.insert1(3)

    def test_insert_select(self):
        schema.TTest2.delete()
        schema.TTest2.insert(schema.TTest)
        assert_equal(len(schema.TTest2()), len(schema.TTest()))

        original_length = len(self.subject)
        self.subject.insert(self.subject.proj(
            'real_id', 'date_of_birth', 'subject_notes', subject_id='subject_id+1000', species='"human"'))
        assert_equal(len(self.subject), 2*original_length)

    def test_insert_pandas_roundtrip(self):
        ''' ensure fetched frames can be inserted '''
        schema.TTest2.delete()
        n = len(schema.TTest())
        assert_true(n > 0)
        df = schema.TTest.fetch(format='frame')
        assert_true(isinstance(df, pandas.DataFrame))
        assert_equal(len(df), n)
        schema.TTest2.insert(df)
        assert_equal(len(schema.TTest2()), n)

    def test_insert_pandas_userframe(self):
        '''
        ensure simple user-created frames (1 field, non-custom index)
        can be inserted without extra index adjustment
        '''
        schema.TTest2.delete()
        n = len(schema.TTest())
        assert_true(n > 0)
        df = pandas.DataFrame(schema.TTest.fetch())
        assert_true(isinstance(df, pandas.DataFrame))
        assert_equal(len(df), n)
        schema.TTest2.insert(df)
        assert_equal(len(schema.TTest2()), n)

    @raises(dj.DataJointError)
    def test_insert_select_ignore_extra_fields0(self):
        """ need ignore extra fields for insert select """
        self.test_extra.insert1((self.test.fetch('key').max() + 1, 0, 0))
        self.test.insert(self.test_extra)

    def test_insert_select_ignore_extra_fields1(self):
        """ make sure extra fields works in insert select """
        self.test_extra.delete()
        keyno = self.test.fetch('key').max() + 1
        self.test_extra.insert1((keyno, 0, 0))
        self.test.insert(self.test_extra, ignore_extra_fields=True)
        assert(keyno in self.test.fetch('key'))

    def test_insert_select_ignore_extra_fields2(self):
        """ make sure insert select still works when ignoring extra fields when there are none """
        self.test_no_extra.delete()
        self.test_no_extra.insert(self.test, ignore_extra_fields=True)

    def test_insert_select_ignore_extra_fields3(self):
        """ make sure insert select works for from query result """
        self.test_no_extra.delete()
        keystr = str(self.test_extra.fetch('key').max())
        self.test_no_extra.insert((self.test_extra & '`key`=' + keystr),
                                  ignore_extra_fields=True)

    def test_skip_duplicates(self):
        """ test that skip_dublicates works when inserting from another relation  """
        self.test_no_extra.delete()
        self.test_no_extra.insert(self.test, ignore_extra_fields=True, skip_duplicates=True)
        self.test_no_extra.insert(self.test, ignore_extra_fields=True, skip_duplicates=True)

    def test_replace(self):
        """
        Test replacing or ignoring duplicate entries
        """
        key = dict(subject_id=7)
        date = "2015-01-01"
        self.subject.insert1(
            dict(key, real_id=7, date_of_birth=date, subject_notes=""))
        assert_equal(date, str((self.subject & key).fetch1('date_of_birth')), 'incorrect insert')
        date = "2015-01-02"
        self.subject.insert1(
            dict(key, real_id=7, date_of_birth=date, subject_notes=""), skip_duplicates=True)
        assert_not_equal(date, str((self.subject & key).fetch1('date_of_birth')),
                         'inappropriate replace')
        self.subject.insert1(
            dict(key, real_id=7, date_of_birth=date, subject_notes=""), replace=True)
        assert_equal(date, str((self.subject & key).fetch1('date_of_birth')), "replace failed")

    def test_delete_quick(self):
        """Tests quick deletion"""
        tmp = np.array([
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (1, 'Peter', 'mouse', '2015-01-01', '')],
            dtype=self.subject.heading.as_dtype)
        self.subject.insert(tmp)
        s = self.subject & ('subject_id in (%s)' % ','.join(str(r) for r in tmp['subject_id']))
        assert_true(len(s) == 2, 'insert did not work.')
        s.delete_quick()
        assert_true(len(s) == 0, 'delete did not work.')

    def test_skip_duplicate(self):
        """Tests if duplicates are properly skipped."""
        tmp = np.array([
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (1, 'Peter', 'mouse', '2015-01-01', '')],
            dtype=self.subject.heading.as_dtype)
        self.subject.insert(tmp)
        tmp = np.array([
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (1, 'Peter', 'mouse', '2015-01-01', '')],
            dtype=self.subject.heading.as_dtype)
        self.subject.insert(tmp, skip_duplicates=True)

    @raises(dj.errors.DuplicateError)
    def test_not_skip_duplicate(self):
        """Tests if duplicates are not skipped."""
        tmp = np.array([
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (1, 'Peter', 'mouse', '2015-01-01', '')],
            dtype=self.subject.heading.as_dtype)
        self.subject.insert(tmp, skip_duplicates=False)

    @raises(dj.errors.MissingAttributeError)
    def test_no_error_suppression(self):
        """skip_duplicates=True should not suppress other errors"""
        self.test.insert([dict(key=100)], skip_duplicates=True)

    def test_blob_insert(self):
        """Tests inserting and retrieving blobs."""
        X = np.random.randn(20, 10)
        self.img.insert1((1, X))
        Y = self.img.fetch()[0]['img']
        assert_true(np.all(X == Y), 'Inserted and retrieved image are not identical')

    @raises(dj.DataJointError)
    def test_drop(self):
        """Tests dropping tables"""
        dj.config['safemode'] = True
        try:
            with patch.object(dj.utils, "input", create=True, return_value='yes'):
                self.trash.drop()
        except:
            pass
        finally:
            dj.config['safemode'] = False
        self.trash.fetch()

    def test_table_regexp(self):
        """Test whether table names are matched by regular expressions"""
        tiers = [dj.Imported, dj.Manual, dj.Lookup, dj.Computed]
        for name, rel in getmembers(schema, relation_selector):
            assert_true(re.match(rel.tier_regexp, rel.table_name),
                        'Regular expression does not match for {name}'.format(name=name))
            for tier in tiers:
                assert_true(issubclass(rel, tier) or not re.match(tier.tier_regexp, rel.table_name),
                            'Regular expression matches for {name} but should not'.format(name=name))

    def test_table_size(self):
        """test getting the size of the table and its indices in bytes"""
        number_of_bytes = self.experiment.size_on_disk
        assert_true(isinstance(number_of_bytes, int) and number_of_bytes > 100)

    def test_repr_html(self):
        assert_true(self.ephys._repr_html_().strip().startswith("<style"))
