__author__ = 'fabee'

from .schemata.schema1 import test1

from . import BASE_CONN, CONN_INFO, PREFIX, cleanup
from datajoint.connection import Connection
from nose.tools import assert_raises, assert_equal, assert_regexp_matches, assert_false, assert_true, assert_list_equal
from datajoint import DataJointError
import numpy as np


def setup():
    """
    Setup connections and bindings
    """
    pass


class TestTableObject(object):
    def __init__(self):
        self.relvar = None
        self.setup()

    """
    Test cases for Table objects
    """

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        cleanup()  # drop all databases with PREFIX
        self.conn = Connection(**CONN_INFO)
        test1.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX + '_test1')
        self.relvar = test1.Subjects()

    def teardown(self):
        cleanup()

    def test_tuple_insert(self):
        "Test whether tuple insert works"
        testt = (1, 'Peter', 'mouse')
        self.relvar.insert(testt)
        testt2 = tuple((self.relvar & 'subject_id = 1').fetch()[0])
        assert_equal(testt2, testt, "Inserted and fetched tuple do not match!")

    def test_record_insert(self):
        "Test whether record insert works"
        tmp = np.array([(2, 'Klara', 'monkey')],
                       dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])

        self.relvar.insert(tmp[0])
        testt2 = (self.relvar & 'subject_id = 2').fetch()[0]
        assert_equal(tuple(tmp[0]), tuple(testt2), "Inserted and fetched record do not match!")

    def test_record_insert_different_order(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.relvar.insert(tmp[0])
        testt2 = (self.relvar & 'subject_id = 2').fetch()[0]
        assert_equal((2, 'Klara', 'monkey'), tuple(testt2), "Inserted and fetched record do not match!")

    def test_dict_insert(self):
        "Test whether record insert works"
        tmp = {'real_id': 'Brunhilda',
               'subject_id': 3,
               'species': 'human'}

        self.relvar.insert(tmp)
        testt2 = (self.relvar & 'subject_id = 3').fetch()[0]
        assert_equal((3, 'Brunhilda', 'human'), tuple(testt2), "Inserted and fetched record do not match!")


    def test_batch_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.relvar.batch_insert(tmp)

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.relvar.fetch()

        for e,d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d),'Inserted and fetched records do not match')

    def test_iter_insert(self):
        "Test whether record insert works"
        tmp = np.array([('Klara', 2, 'monkey'), ('Brunhilda', 3, 'mouse'), ('Mickey', 1, 'human')],
                       dtype=[('real_id', 'O'), ('subject_id', '>i4'), ('species', 'O')])

        self.relvar.iter_insert(tmp.__iter__())

        expected = np.array([(1, 'Mickey', 'human'), (2, 'Klara', 'monkey'),
                             (3, 'Brunhilda', 'mouse')],
                            dtype=[('subject_id', '<i4'), ('real_id', 'O'), ('species', 'O')])
        delivered = self.relvar.fetch()

        for e,d in zip(expected, delivered):
            assert_equal(tuple(e), tuple(d),'Inserted and fetched records do not match')