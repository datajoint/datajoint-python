"""
Collection of test cases for base module. Tests functionalities such as
creating tables using docstring table declarations
"""
__author__ = 'eywalker'

from . import BASE_CONN, CONN_INFO, PREFIX, cleanup
import datajoint as dj #TODO: probably make this a relative import"
from ..base import Base
from ..connection import Connection
from .schemata import test1, test2, test3
from nose.tools import raises, assert_raises, assert_equal, assert_regexp_matches, assert_false, assert_true
from ..core import DataJointError


def setup():
    """
    Setup connections and bindings
    """
    pass


class TestBaseObject(object):
    """
    Test cases for Base objects
    """
    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        test2 - has conn but not bound
        test3 - no conn and not bound
        """
        cleanup() # drop all databases with PREFIX

        self.conn = Connection(**CONN_INFO)
        test1.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX+'_test1')

        test2.conn = self.conn

        test3.__dict__.pop('conn', None) # make sure conn is not defined


    def teardown(self):
        cleanup()

    def test_instantiation_from_unbound_module_should_fail(self):
        """
        Attempting to instantiate a Base derivative from a module with
        connection defined but not bound to a database should raise error
        """
        with assert_raises(DataJointError) as e:
            test2.Subjects()
        assert_regexp_matches(e.exception.args[0], r".*not bound.*")

    def test_instantiation_from_module_without_conn_should_fail(self):
        """
        Attempting to instantiate a Base derivative from a module that lacks
        `conn` object should raise error
        """
        with assert_raises(DataJointError) as e:
            test3.Subjects()
        assert_regexp_matches(e.exception.args[0], r".*define.*conn.*")

    def test_instantiation_of_base_derivatives(self):
        """
        Test instantiation and initialization of objects derived from
        Base class
        """
        s = test1.Subjects()
        assert_equal(s.dbname, PREFIX + '_test1')
        assert_equal(s.conn, self.conn)
        assert_equal(s.declaration, test1.Subjects.__doc__)

    def test_declaration_status(self):
        b = test1.Subjects()
        assert_false(b.is_declared)
        b.declare()
        assert_true(b.is_declared)

    def test_full_table_name(self):
        """
        Full table name should return appropriate table name
        """

    def test_declaration_from_doc_string(self):
        cur = BASE_CONN.cursor()
        assert_equal(cur.execute("SHOW TABLES IN `{}` LIKE 'subjects'".format(PREFIX + '_test1')), 0)
        test1.Subjects().declare()
        assert_equal(cur.execute("SHOW TABLES IN `{}` LIKE 'subjects'".format(PREFIX + '_test1')), 1)







