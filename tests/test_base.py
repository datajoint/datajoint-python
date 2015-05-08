"""
Collection of test cases for base module. Tests functionalities such as
creating tables using docstring table declarations
"""
from .schemata import schema1, schema2
from .schemata.schema1 import test1, test2, test3


__author__ = 'eywalker'

from . import BASE_CONN, CONN_INFO, PREFIX, cleanup, setup_sample_db
from datajoint.connection import Connection
from nose.tools import assert_raises, assert_equal, assert_regexp_matches, assert_false, assert_true, raises
from datajoint import DataJointError


def setup():
    """
    Setup connections and bindings
    """
    pass


class TestBaseInstantiations(object):
    """
    Test cases for instantiating Relation objects
    """
    def __init__(self):
        self.conn = None

    def setup(self):
        """
        Create a connection object and prepare test modules
        as follows:
        test1 - has conn and bounded
        """
        self.conn = Connection(**CONN_INFO)
        cleanup()  # drop all databases with PREFIX
        #test1.conn = self.conn
        #self.conn.bind(test1.__name__, PREFIX+'_test1')

        #test2.conn = self.conn

        #test3.__dict__.pop('conn', None)  # make sure conn is not defined in test3
        test1.__dict__.pop('conn', None)
        schema1.__dict__.pop('conn', None) # make sure conn is not defined at schema level


    def teardown(self):
        cleanup()


    def test_instantiation_from_unbound_module_should_fail(self):
        """
        Attempting to instantiate a Relation derivative from a module with
        connection defined but not bound to a database should raise error
        """
        test1.conn = self.conn
        with assert_raises(DataJointError) as e:
            test1.Subjects()
        assert_regexp_matches(e.exception.args[0], r".*not bound.*")

    def test_instantiation_from_module_without_conn_should_fail(self):
        """
        Attempting to instantiate a Relation derivative from a module that lacks
        `conn` object should raise error
        """
        with assert_raises(DataJointError) as e:
            test1.Subjects()
        assert_regexp_matches(e.exception.args[0], r".*define.*conn.*")

    def test_instantiation_of_base_derivatives(self):
        """
        Test instantiation and initialization of objects derived from
        Relation class
        """
        test1.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX + '_test1')
        s = test1.Subjects()
        assert_equal(s.dbname, PREFIX + '_test1')
        assert_equal(s.conn, self.conn)
        assert_equal(s.definition, test1.Subjects.definition)



    def test_packagelevel_binding(self):
        schema2.conn = self.conn
        self.conn.bind(schema2.__name__, PREFIX + '_test1')
        s = schema2.test1.Subjects()


class TestBaseDeclaration(object):
    """
    Test declaration (creation of table) from
    definition in Relation under various circumstances
    """

    def setup(self):
        cleanup()

        self.conn = Connection(**CONN_INFO)
        test1.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX + '_test1')
        test2.conn = self.conn
        self.conn.bind(test2.__name__, PREFIX + '_test2')

    def test_is_declared(self):
        """
        The table should not be created immediately after instantiation,
        but should be created when declare method is called
        :return:
        """
        s = test1.Subjects()
        assert_false(s.is_declared)
        s.declare()
        assert_true(s.is_declared)

    def test_calling_heading_should_trigger_declaration(self):
        s = test1.Subjects()
        assert_false(s.is_declared)
        a = s.heading
        assert_true(s.is_declared)

    def test_foreign_key_ref_in_same_schema(self):
        s = test1.Experiments()
        assert_true('subject_id' in s.heading.primary_key)

    def test_foreign_key_ref_in_another_schema(self):
        s = test2.Experiments()
        assert_true('subject_id' in s.heading.primary_key)

    def test_aliased_module_name_should_resolve(self):
        """
        Module names that were aliased in the definition should
        be properly resolved.
        """
        s = test2.Conditions()
        assert_true('subject_id' in s.heading.primary_key)

    def test_reference_to_unknown_module_in_definition_should_fail(self):
        """
        Module names in table definition that is not aliased via import
        results in error
        """
        s = test2.FoodPreference()
        with assert_raises(DataJointError) as e:
            s.declare()


class TestBaseWithExistingTables(object):
    """
    Test base derivatives behaviors when some of the tables
    already exists in the database
    """
    def setup(self):
        cleanup()
        self.conn = Connection(**CONN_INFO)
        setup_sample_db()
        test1.conn = self.conn
        self.conn.bind(test1.__name__, PREFIX + '_test1')
        test2.conn = self.conn
        self.conn.bind(test2.__name__, PREFIX + '_test2')
        self.conn.load_headings(force=True)

        schema2.conn = self.conn
        self.conn.bind(schema2.__name__, PREFIX + '_package')

    def teardown(selfself):
        schema1.__dict__.pop('conn', None)
        cleanup()

    def test_detection_of_existing_table(self):
        """
        The Relation instance should be able to detect if the
        corresponding table already exists in the database
        """
        s = test1.Subjects()
        assert_true(s.is_declared)

    def test_definition_referring_to_existing_table_without_class(self):
        s1 = test1.Sessions()
        assert_true('experimenter_id' in s1.primary_key)

        s2 = test2.Session()
        assert_true('experimenter_id' in s2.primary_key)

    def test_reference_to_package_level_table(self):
        s = test1.Match()
        s.declare()
        assert_true('pop_id' in s.primary_key)

    def test_direct_reference_to_existing_table_should_fail(self):
        """
        When deriving from Relation, definition should not contain direct reference
        to a database name
        """
        s = test1.TrainingSession()
        with assert_raises(DataJointError):
            s.declare()

@raises(TypeError)
def test_instantiation_of_base_derivative_without_definition_should_fail():
    test1.Empty()


















