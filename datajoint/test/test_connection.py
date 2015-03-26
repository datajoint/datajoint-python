"""
Collection of test cases to test connection module.
"""
from test.schemata.schema1 import test1

__author__ = 'eywalker'
from . import (CONN_INFO, PREFIX, BASE_CONN, cleanup)
from nose.tools import assert_true, assert_raises, assert_equal
import datajoint as dj
from datajoint.core import DataJointError


def setup():
    cleanup()


def test_dj_conn():
    """
    Should be able to establish a connection
    """
    c = dj.conn(**CONN_INFO)
    assert c.is_connected


def test_persistent_dj_conn():
    """
    conn() method should provide persistent connection
    across calls.
    """
    c1 = dj.conn(**CONN_INFO)
    c2 = dj.conn(host='test', user='test', passwd='test')
    assert_true(c1 is c2)


def test_dj_conn_reset():
    """
    Passing in rest=True should allow for new persistent
    connection to be created.
    """
    c1 = dj.conn(**CONN_INFO)
    c2 = dj.conn(reset=True, **CONN_INFO)
    assert_true(c1 is not c2)


def setup_sample_db():
    """
    Helper method to setup databases with tables to be used
    during the test
    """
    cur = BASE_CONN.cursor()
    cur.execute("CREATE DATABASE `{}_test1`".format(PREFIX))
    cur.execute("CREATE DATABASE `{}_test2`".format(PREFIX))
    query1 = """
    CREATE TABLE `{prefix}_test1`.`subjects`
    (
      subject_id      SMALLINT        COMMENT 'Unique subject ID',
      subject_name    VARCHAR(255)    COMMENT 'Subject name',
      subject_email   VARCHAR(255)    COMMENT 'Subject email address',
      PRIMARY KEY (subject_id)
    )
    """.format(prefix=PREFIX)
    cur.execute(query1)
    # query2 = """
    # CREATE TABLE `{prefix}_test2`.`experiments`
    # (
    #   experiment_id       SMALLINT        COMMENT 'Unique experiment ID',
    #   experiment_name     VARCHAR(255)    COMMENT 'Experiment name',
    #   subject_id          SMALLINT,
    #   CONSTRAINT FOREIGN KEY (`subject_id`) REFERENCES `dj_test1`.`subjects` (`subject_id`) ON UPDATE CASCADE ON DELETE RESTRICT,
    #   PRIMARY KEY (subject_id, experiment_id)
    # )""".format(prefix=PREFIX)
    # cur.execute(query2)


class TestConnectionWithoutBindings(object):
    """
    Test methods from Connection that does not
    depend on presence of module to database bindings.
    """
    def setup(self):
        self.conn = dj.Connection(**CONN_INFO)
        setup_sample_db()

    def teardown(self):
        cleanup()

    def check_binding(self, db_name, module):
        """
        Helper method to check if the specified database-module pairing exists
        """
        assert_equal(self.conn.db_to_mod[db_name], module)
        assert_equal(self.conn.mod_to_db[module], db_name)

    def test_bind_to_existing_database(self):
        """
        Should be able to bind a module to an existing database
        """
        db_name = PREFIX + '_test1'
        module = test1.__name__
        self.conn.bind(module, db_name)
        self.check_binding(db_name, module)

    def test_bind_to_non_existing_database(self):
        """
        Should be able to bind a module to a non-existing database by creating target
        """
        db_name = PREFIX + '_test3'
        module = test1.__name__
        cur = BASE_CONN.cursor()

        # Ensure target database doesn't exist
        cur.execute("DROP DATABASE IF EXISTS `{}`".format(db_name))
        # Bind module to non-existing database
        self.conn.bind(module, db_name)
        # Check that target database was created
        assert_equal(cur.execute("SHOW DATABASES LIKE '{}'".format(db_name)), 1)
        self.check_binding(db_name, module)
        # Remove the target database
        cur.execute("DROP DATABASE IF EXISTS `{}`".format(db_name))

    def test_cannot_bind_to_multiple_databases(self):
        """
        Bind will fail when db_name is a pattern that
        matches multiple databases
        """
        db_name = PREFIX + "_test%%"
        module = test1.__name__
        with assert_raises(DataJointError):
            self.conn.bind(module, db_name)

    def test_basic_sql_query(self):
        """
        Test execution of basic SQL query using connection
        object.
        """
        cur = self.conn.query('SHOW DATABASES')
        results1 = cur.fetchall()
        cur2 = BASE_CONN.cursor()
        cur2.execute('SHOW DATABASES')
        results2 = cur2.fetchall()
        assert_equal(results1, results2)

    def test_transaction_commit(self):
        """
        Test transaction commit
        """
        table_name = PREFIX + '_test1.subjects'
        self.conn.start_transaction()
        self.conn.query("INSERT INTO {table} VALUES (0, 'dj_user', 'dj_user@example.com')".format(table=table_name))
        cur = BASE_CONN.cursor()
        assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)
        self.conn.commit_transaction()
        assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 1)

    def test_transaction_rollback(self):
        """
        Test transaction rollback
        """
        table_name = PREFIX + '_test1.subjects'
        self.conn.start_transaction()
        self.conn.query("INSERT INTO {table} VALUES (0, 'dj_user', 'dj_user@example.com')".format(table=table_name))
        cur = BASE_CONN.cursor()
        assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)
        self.conn.cancel_transaction()
        assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)



# class TestConnectionWithBindings(object):
#     """
#     Tests heading and dependency loadings
#     """
#     def setup(self):
#         self.conn = dj.Connection(**CONN_INFO)
#         cur.execute(query)










