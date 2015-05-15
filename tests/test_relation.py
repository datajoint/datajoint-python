import random
import string

__author__ = 'fabee'

from .schemata.schema1 import test1, test4

from . import BASE_CONN, CONN_INFO, PREFIX, cleanup
from datajoint.connection import Connection
from nose.tools import assert_raises, assert_equal, assert_regexp_matches, assert_false, assert_true, assert_list_equal,\
    assert_tuple_equal, assert_dict_equal, raises
from datajoint import DataJointError
import numpy as np
from numpy.testing import assert_array_equal
from datajoint.free_relation import FreeRelation
import numpy as np


def trial_faker(n=10):
    def iter():
        for s in [1, 2]:
            for i in range(n):
                yield dict(trial_id=i, subject_id=s, outcome=int(np.random.randint(10)), notes= 'no comment')
    return iter()


def setup():
    """
    Setup connections and bindings
    """
    pass


class TestTableObject(object):
    def __init__(self):
        self.subjects = None
        self.setup()

    """
    Test cases for FreeRelation objects
    """

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        cleanup()  # drop all databases with PREFIX
        test1.__dict__.pop('conn', None)
        test4.__dict__.pop('conn', None)  # make sure conn is not defined at schema level

        self.conn = Connection(**CONN_INFO)
        test1.conn = self.conn
        test4.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX + '_test1')
        self.conn.bind(test4.__name__, PREFIX + '_test4')
        self.subjects = test1.Subjects()
        self.relvar_blob = test4.Matrix()
        self.trials = test1.Trials()

    def teardown(self):
        cleanup()

    def test_compound_restriction(self):
        s = self.subjects
        t = self.trials

        s.insert(dict(subject_id=1, real_id='M' ))
        s.insert(dict(subject_id=2, real_id='F' ))
        t.iter_insert(trial_faker(20))

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

    def test_batch_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.batch_insert(tmp)

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.subjects.fetch()

        for e,d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d),'Inserted and fetched records do not match')

    def test_iter_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.subjects.iter_insert(tmp.__iter__())

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.subjects.fetch()

        for e,d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d),'Inserted and fetched records do not match')

    def test_blob_insert(self):
        x = np.random.randn(10)
        t = {'matrix_id':0, 'data':x, 'comment':'this is a random image'}
        self.relvar_blob.insert(t)
        x2 = self.relvar_blob.fetch()[0][1]
        assert_array_equal(x,x2, 'inserted blob does not match')

class TestUnboundTables(object):
    """
    Test usages of FreeRelation objects not connected to a module.
    """
    def setup(self):
        cleanup()
        self.conn = Connection(**CONN_INFO)

    def test_creation_from_definition(self):
        definition = """
        `dj_free`.Animals (manual)  # my animal table
        animal_id   : int           # unique id for the animal
        ---
        animal_name : varchar(128)  # name of the animal
        """
        table = FreeRelation(self.conn, 'dj_free', 'Animals', definition)
        table.declare()
        assert_true('animal_id' in table.primary_key)

    def test_reference_to_non_existant_table_should_fail(self):
        definition = """
        `dj_free`.Recordings (manual)  # recordings
        -> `dj_free`.Animals
        rec_session_id : int     # recording session identifier
        """
        table = FreeRelation(self.conn, 'dj_free', 'Recordings', definition)
        assert_raises(DataJointError, table.declare)

    def test_reference_to_existing_table(self):
        definition1 = """
        `dj_free`.Animals (manual)  # my animal table
        animal_id   : int           # unique id for the animal
        ---
        animal_name : varchar(128)  # name of the animal
        """
        table1 = FreeRelation(self.conn, 'dj_free', 'Animals', definition1)
        table1.declare()

        definition2 = """
        `dj_free`.Recordings (manual)  # recordings
        -> `dj_free`.Animals
        rec_session_id : int     # recording session identifier
        """
        table2 = FreeRelation(self.conn, 'dj_free', 'Recordings', definition2)
        table2.declare()
        assert_true('animal_id' in table2.primary_key)


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
        cleanup()  # drop all databases with PREFIX
        test4.__dict__.pop('conn', None) # make sure conn is not defined at schema level

        self.conn = Connection(**CONN_INFO)
        test4.conn = self.conn
        self.conn.bind(test4.__name__, PREFIX + '_test4')
        self.relvar_blob = test4.Matrix()

    def teardown(self):
        cleanup()


    def test_blob_iteration(self):
        "Tests the basic call of the iterator"

        dicts = []
        for i in range(10):

            c = id_generator()

            t = {'matrix_id':i,
                 'data': np.random.randn(4,4,4),
                 'comment': c}
            self.relvar_blob.insert(t)
            dicts.append(t)

        for t, t2 in zip(dicts, self.relvar_blob):
            assert_true(isinstance(t2, dict), 'iterator does not return dict')

            assert_equal(t['matrix_id'], t2['matrix_id'], 'inserted and retrieved tuples do not match')
            assert_equal(t['comment'], t2['comment'], 'inserted and retrieved tuples do not match')
            assert_true(np.all(t['data'] == t2['data']), 'inserted and retrieved tuples do not match')

    def test_fetch(self):
        dicts = []
        for i in range(10):

            c = id_generator()

            t = {'matrix_id':i,
                 'data': np.random.randn(4,4,4),
                 'comment': c}
            self.relvar_blob.insert(t)
            dicts.append(t)

        tuples2 = self.relvar_blob.fetch()
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

            t = {'matrix_id':i,
                 'data': np.random.randn(4,4,4),
                 'comment': c}
            self.relvar_blob.insert(t)
            dicts.append(t)

        tuples2 = self.relvar_blob.fetch(as_dict=True)
        assert_true(isinstance(tuples2, list), "Return value of fetch with as_dict=True does not have proper type.")
        assert_true(isinstance(tuples2[0], dict), "Return value of fetch with as_dict=True does not have proper type.")
        for t, t2 in zip(dicts, tuples2):
            assert_equal(t['matrix_id'], t2['matrix_id'], 'inserted and retrieved dicts do not match')
            assert_equal(t['comment'], t2['comment'], 'inserted and retrieved dicts do not match')
            assert_true(np.all(t['data'] == t2['data']), 'inserted and retrieved dicts do not match')

