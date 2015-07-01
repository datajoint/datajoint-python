import random
import string
from numpy.testing import assert_array_equal
import numpy as np
from nose.tools import assert_raises, assert_equal, assert_regexp_matches, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises

from datajoint import DataJointError
from .schemas import Subjects, Animals, Matrix, Trials, SquaredScore, SquaredSubtable, \
    ErrorGenerator, schema

import datajoint as dj


class TestRelation:
    """
    Test creation of relations, their dependencies
    """

    def __init__(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        self.subjects = Subjects()
        self.animals = Animals()
        self.matrix = Matrix()
        self.trials = Trials()
        self.score = SquaredScore()
        self.subtable = SquaredSubtable()

    def setup(self):
        pass

    def teardown(self):
        self.subjects.delete()

    def test_table_name_manual(self):
        assert_true(not self.subjects.table_name.startswith('#') and
                    not self.subjects.table_name.startswith('_') and not self.subjects.table_name.startswith('__'))

    def test_table_name_computed(self):
        assert_true(self.score.table_name.startswith('__'))
        assert_true(self.subtable.table_name.startswith('__'))

    def test_population_relation_subordinate(self):
        assert_true(self.subtable.populated_from is None)

    @raises(NotImplementedError)
    def test_make_tubles_not_implemented_subordinate(self):
        self.subtable._make_tuples(None)

    def test_instantiate_relation(self):
        s = Subjects()

    def test_compound_restriction(self):
        s = self.subjects
        t = self.trials

        s.insert(dict(subject_id=1, real_id='M'))
        s.insert(dict(subject_id=2, real_id='F'))

        # insert trials
        n_trials = 20
        for subject_id in [1, 2]:
            for trial_id in range(n_trials):
                t.insert(
                    trial_id=trial_id,
                    subject_id=subject_id,
                    outcome=int(np.random.randint(10)),
                    notes='no comment')

        tM = t & (s & "real_id = 'M'")
        t1 = t & "subject_id = 1"

        assert_equal(len(tM), len(t1), "Results of compound request does not have same length")

        for t1_item, tM_item in zip(sorted(t1, key=lambda item: item['trial_id']),
                                    sorted(tM, key=lambda item: item['trial_id'])):
            assert_dict_equal(t1_item, tM_item,
                              'Dictionary elements do not agree in compound statement')

    def test_record_insert(self):
        "Test whether record insert works"
        tmp = np.array([(2, 'Klara', 'monkey')],
                       dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])

        self.subjects.insert(tmp[0])
        testt2 = (self.subjects & 'subject_id = 2').fetch()[0]
        assert_equal(tuple(tmp[0]), tuple(testt2), "Inserted and fetched record do not match!")

    def test_delete(self):
        "Test whether delete works"
        tmp = np.array([(2, 'Klara', 'monkey'), (1, 'Peter', 'mouse')],
                       dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])

        self.subjects.batch_insert(tmp)
        assert_true(len(self.subjects) == 2, 'Length does not match 2.')
        self.subjects.delete()
        assert_true(len(self.subjects) == 0, 'Length does not match 0.')

    #
    #     # def test_cascading_delete(self):
    #     #     "Test whether delete works"
    #     #     tmp = np.array([(2, 'Klara', 'monkey'), (1,'Peter', 'mouse')],
    #     #                    dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])
    #     #
    #     #     self.subjects.batch_insert(tmp)
    #     #
    #     #     self.trials.insert(dict(subject_id=1, trial_id=1, outcome=0))
    #     #     self.trials.insert(dict(subject_id=1, trial_id=2, outcome=1))
    #     #     self.trials.insert(dict(subject_id=2, trial_id=3, outcome=2))
    #     #     assert_true(len(self.subjects) == 2, 'Length does not match 2.')
    #     #     assert_true(len(self.trials) == 3, 'Length does not match 3.')
    #     #     (self.subjects & 'subject_id=1').delete()
    #     #     assert_true(len(self.subjects) == 1, 'Length does not match 1.')
    #     #     assert_true(len(self.trials) == 1, 'Length does not match 1.')
    #
    #     def test_short_hand_foreign_reference(self):
    #         self.animals.heading
    #
    #
    #
    def test_record_insert_different_order(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.insert(tmp[0])
        testt2 = (self.subjects & 'subject_id = 2').fetch()[0]
        assert_equal((2, 'Klara', 'monkey'), tuple(testt2),
                     "Inserted and fetched record do not match!")

    @raises(KeyError)
    def test_wrong_key_insert_records(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey')],
                       dtype=[('real_deal', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.insert(tmp[0])

        def test_dict_insert(self):
            "Test whether record insert works"
            tmp = {'real_id': 'Brunhilda',
                   'subject_id': 3,
                   'species': 'human'}

            self.subjects.insert(tmp)
            testt2 = (self.subjects & 'subject_id = 3').fetch()[0]
            assert_equal((3, 'Brunhilda', 'human'), tuple(testt2), "Inserted and fetched record do not match!")

    @raises(KeyError)
    def test_wrong_key_insert(self):
        "Test whether a correct error is generated when inserting wrong attribute name"
        tmp = {'real_deal': 'Brunhilda',
               'subject_database': 3,
               'species': 'human'}

        self.subjects.insert(tmp)

    #
    def test_batch_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.batch_insert(tmp)

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.subjects.fetch()

        for e, d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d), 'Inserted and fetched records do not match')
        #

    def test_iter_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.iter_insert(tmp.__iter__())

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.subjects.fetch()

        for e, d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d), 'Inserted and fetched records do not match')
        #

    def test_blob_insert(self):
        x = np.random.randn(10)
        t = {'matrix_id': 0, 'data': x, 'comment': 'this is a random image'}
        self.matrix.insert(t)
        x2 = self.matrix.fetch()[0][1]
        assert_array_equal(x, x2, 'inserted blob does not match')


def id_generator(size=6, chars=string.ascii_uppercase + string.digits):
    return ''.join(random.choice(chars) for _ in range(size))


class TestIterator(object):
    def __init__(self):
        self.relvar = None
        self.setup()

    """
    Test cases for Iterators in Relations objects
    """

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        self.matrix = Matrix()

    def teardown(self):
        pass

    #
    #
    def test_blob_iteration(self):
        """Tests the basic call of the iterator"""
        dicts = []
        for i in range(10):
            c = id_generator()
            t = {'matrix_id': i,
                 'data': np.random.randn(4, 4, 4),
                 'comment': c}
            self.matrix.insert(t)
            dicts.append(t)

        for t, t2 in zip(dicts, self.matrix):
            assert_true(isinstance(t2, dict), 'iterator does not return dict')

            assert_equal(t['matrix_id'], t2['matrix_id'],
                         'inserted and retrieved tuples do not match')
            assert_equal(t['comment'], t2['comment'],
                         'inserted and retrieved tuples do not match')
            assert_true(np.all(t['data'] == t2['data']),
                        'inserted and retrieved tuples do not match')

    def test_fetch(self):
        dicts = []
        for i in range(10):
            c = id_generator()

            t = {'matrix_id': i,
                 'data': np.random.randn(4, 4, 4),
                 'comment': c}
            self.matrix.insert(t)
            dicts.append(t)

        tuples2 = self.matrix.fetch()
        assert_true(isinstance(tuples2, np.ndarray), "Return value of fetch does not have proper type.")
        assert_true(isinstance(tuples2[0], np.void), "Return value of fetch does not have proper type.")
        for t, t2 in zip(dicts, tuples2):
            assert_equal(t['matrix_id'], t2['matrix_id'], 'inserted and retrieved tuples do not match')
            assert_equal(t['comment'], t2['comment'], 'inserted and retrieved tuples do not match')
            assert_true(np.all(t['data'] == t2['data']), 'inserted and retrieved tuples do not match')

    def test_fetch_dicts(self):
        dicts = []
        for i in range(10):
            c = id_generator()

            t = {'matrix_id': i,
                 'data': np.random.randn(4, 4, 4),
                 'comment': c}
            self.matrix.insert(t)
            dicts.append(t)

        tuples2 = self.matrix.fetch(as_dict=True)
        assert_true(isinstance(tuples2, list),
                    "Return value of fetch with as_dict=True does not have proper type.")
        assert_true(isinstance(tuples2[0], dict),
                    "Return value of fetch with as_dict=True does not have proper type.")
        for t, t2 in zip(dicts, tuples2):
            assert_equal(t['matrix_id'], t2['matrix_id'],
                         'inserted and retrieved dicts do not match')
            assert_equal(t['comment'], t2['comment'],
                         'inserted and retrieved dicts do not match')
            assert_true(np.all(t['data'] == t2['data']),
                        'inserted and retrieved dicts do not match')


#
class TestAutopopulate:
    def __init__(self):
        self.relvar = None
        self.setup()

    """
    Test cases for Iterators in Relations objects
    """

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        self.subjects = Subjects()
        self.trials = Trials()
        self.squared = SquaredScore()
        self.dummy = SquaredSubtable()
        self.error_generator = ErrorGenerator()
        self.fill_relation()

    def teardown(self):
        self.error_generator.delete_quick()


    def fill_relation(self):
        tmp = np.array([('Klara', 2, 'monkey'), ('Peter', 3, 'mouse')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])
        self.subjects.batch_insert(tmp)

        for trial_id in range(1, 11):
            self.trials.insert(dict(subject_id=2, trial_id=trial_id, outcome=np.random.randint(0, 10)))

    def teardown(self):
        pass

    def test_autopopulate(self):
        self.squared.populate()
        assert_equal(len(self.squared), 10)

        for trial in self.trials * self.squared:
            assert_equal(trial['outcome'] ** 2, trial['squared'])

    def test_autopopulate_restriction(self):
        self.squared.populate(restriction='trial_id <= 5')
        assert_equal(len(self.squared), 5)

        for trial in self.trials * self.squared:
            assert_equal(trial['outcome'] ** 2, trial['squared'])

    @raises(DataJointError)
    def test_autopopulate_relation_check(self):
        @schema
        class Dummy(dj.Computed):
            def populated_from(self):
                return None

            def _make_tuples(self, key):
                pass

        du = Dummy()
        du.populate()

    @raises(DataJointError)
    def test_autopopulate_relation_check(self):
        self.dummy1.populate()

    @raises(Exception)
    def test_autopopulate_relation_check(self):
        self.error_generator.populate()

    @raises(Exception)
    def test_autopopulate_relation_check2(self):
        tmp = self.dummy2.populate(suppress_errors=True)
        assert_equal(len(tmp), 1, 'Error list should have length 1.')
