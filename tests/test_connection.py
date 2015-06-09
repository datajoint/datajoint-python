# """
# Collection of test cases to test connection module.
# """
# from .schemata import schema1
# from .schemata.schema1 import test1
# import numpy as np
#
# __author__ = 'eywalker, fabee'
# from . import (CONN_INFO, PREFIX, BASE_CONN, cleanup)
# from nose.tools import assert_true, assert_raises, assert_equal, raises
# import datajoint as dj
# from datajoint.utils import DataJointError
#
#
# def setup():
#     cleanup()
#
#
# def test_dj_conn():
#     """
#     Should be able to establish a connection
#     """
#     c = dj.conn(**CONN_INFO)
#     assert c.is_connected
#
#
# def test_persistent_dj_conn():
#     """
#     conn() method should provide persistent connection
#     across calls.
#     """
#     c1 = dj.conn(**CONN_INFO)
#     c2 = dj.conn()
#     assert_true(c1 is c2)
#
#
# def test_dj_conn_reset():
#     """
#     Passing in reset=True should allow for new persistent
#     connection to be created.
#     """
#     c1 = dj.conn(**CONN_INFO)
#     c2 = dj.conn(reset=True, **CONN_INFO)
#     assert_true(c1 is not c2)
#
#
#
# def setup_sample_db():
#     """
#     Helper method to setup databases with tables to be used
#     during the test
#     """
#     cur = BASE_CONN.cursor()
#     cur.execute("CREATE DATABASE `{}_test1`".format(PREFIX))
#     cur.execute("CREATE DATABASE `{}_test2`".format(PREFIX))
#     query1 = """
#     CREATE TABLE `{prefix}_test1`.`subjects`
#     (
#       subject_id      SMALLINT        COMMENT 'Unique subject ID',
#       subject_name    VARCHAR(255)    COMMENT 'Subject name',
#       subject_email   VARCHAR(255)    COMMENT 'Subject email address',
#       PRIMARY KEY (subject_id)
#     )
#     """.format(prefix=PREFIX)
#     cur.execute(query1)
#     # query2 = """
#     # CREATE TABLE `{prefix}_test2`.`experiments`
#     # (
#     #   experiment_id       SMALLINT        COMMENT 'Unique experiment ID',
#     #   experiment_name     VARCHAR(255)    COMMENT 'Experiment name',
#     #   subject_id          SMALLINT,
#     #   CONSTRAINT FOREIGN KEY (`subject_id`) REFERENCES `dj_test1`.`subjects` (`subject_id`) ON UPDATE CASCADE ON DELETE RESTRICT,
#     #   PRIMARY KEY (subject_id, experiment_id)
#     # )""".format(prefix=PREFIX)
#     # cur.execute(query2)
#
#
# class TestConnectionWithoutBindings(object):
#     """
#     Test methods from Connection that does not
#     depend on presence of module to database bindings.
#     This includes tests for `bind` method itself.
#     """
#     def setup(self):
#         self.conn = dj.Connection(**CONN_INFO)
#         test1.__dict__.pop('conn', None)
#         schema1.__dict__.pop('conn', None)
#         setup_sample_db()
#
#     def teardown(self):
#         cleanup()
#
#     def check_binding(self, db_name, module):
#         """
#         Helper method to check if the specified database-module pairing exists
#         """
#         assert_equal(self.conn.db_to_mod[db_name], module)
#         assert_equal(self.conn.mod_to_db[module], db_name)
#
#     def test_bind_to_existing_database(self):
#         """
#         Should be able to bind a module to an existing database
#         """
#         db_name = PREFIX + '_test1'
#         module = test1.__name__
#         self.conn.bind(module, db_name)
#         self.check_binding(db_name, module)
#
#     def test_bind_at_package_level(self):
#         db_name = PREFIX + '_test1'
#         package = schema1.__name__
#         self.conn.bind(package, db_name)
#         self.check_binding(db_name, package)
#
#     def test_bind_to_non_existing_database(self):
#         """
#         Should be able to bind a module to a non-existing database by creating target
#         """
#         db_name = PREFIX + '_test3'
#         module = test1.__name__
#         cur = BASE_CONN.cursor()
#
#         # Ensure target database doesn't exist
#         if cur.execute("SHOW DATABASES LIKE '{}'".format(db_name)):
#             cur.execute("DROP DATABASE IF EXISTS `{}`".format(db_name))
#         # Bind module to non-existing database
#         self.conn.bind(module, db_name)
#         # Check that target database was created
#         assert_equal(cur.execute("SHOW DATABASES LIKE '{}'".format(db_name)), 1)
#         self.check_binding(db_name, module)
#         # Remove the target database
#         cur.execute("DROP DATABASE IF EXISTS `{}`".format(db_name))
#
#     def test_cannot_bind_to_multiple_databases(self):
#         """
#         Bind will fail when db_name is a pattern that
#         matches multiple databases
#         """
#         db_name = PREFIX + "_test%%"
#         module = test1.__name__
#         with assert_raises(DataJointError):
#             self.conn.bind(module, db_name)
#
#     def test_basic_sql_query(self):
#         """
#         Test execution of basic SQL query using connection
#         object.
#         """
#         cur = self.conn.query('SHOW DATABASES')
#         results1 = cur.fetchall()
#         cur2 = BASE_CONN.cursor()
#         cur2.execute('SHOW DATABASES')
#         results2 = cur2.fetchall()
#         assert_equal(results1, results2)
#
#     def test_transaction_commit(self):
#         """
#         Test transaction commit
#         """
#         table_name = PREFIX + '_test1.subjects'
#         self.conn.start_transaction()
#         self.conn.query("INSERT INTO {table} VALUES (0, 'dj_user', 'dj_user@example.com')".format(table=table_name))
#         cur = BASE_CONN.cursor()
#         assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)
#         self.conn.commit_transaction()
#         assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 1)
#
#     def test_transaction_rollback(self):
#         """
#         Test transaction rollback
#         """
#         table_name = PREFIX + '_test1.subjects'
#         self.conn.start_transaction()
#         self.conn.query("INSERT INTO {table} VALUES (0, 'dj_user', 'dj_user@example.com')".format(table=table_name))
#         cur = BASE_CONN.cursor()
#         assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)
#         self.conn.cancel_transaction()
#         assert_equal(cur.execute("SELECT * FROM {}".format(table_name)), 0)
#
# # class TestContextManager(object):
# #     def __init__(self):
# #         self.relvar = None
# #         self.setup()
# #
# #     """
# #     Test cases for FreeRelation objects
# #     """
# #
# #     def setup(self):
# #         """
# #         Create a connection object and prepare test modules
# #         as follows:
# #         test1 - has conn and bounded
# #         """
# #         cleanup()  # drop all databases with PREFIX
# #         test1.__dict__.pop('conn', None)
# #
# #         self.conn = dj.Connection(**CONN_INFO)
# #         test1.conn = self.conn
# #         self.conn.bind(test1.__name__, PREFIX + '_test1')
# #         self.relvar = test1.Subjects()
# #
# #     def teardown(self):
# #         cleanup()
# #
# #     # def test_active(self):
# #     #     with self.conn.transaction() as tr:
# #     #         assert_true(tr.is_active, "Transaction is not active")
# #
# #     # def test_rollback(self):
# #     #
# #     #     tmp = np.array([(1,'Peter','mouse'),(2, 'Klara', 'monkey')],
# #     #                    dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])
# #     #
# #     #     self.relvar.insert(tmp[0])
# #     #     try:
# #     #         with self.conn.transaction():
# #     #             self.relvar.insert(tmp[1])
# #     #             raise DataJointError("Just to test")
# #     #     except DataJointError as e:
# #     #         pass
# #     #
# #     #     testt2 = (self.relvar & 'subject_id = 2').fetch()
# #     #     assert_equal(len(testt2), 0, "Length is not 0. Expected because rollback should have happened.")
# #
# #     # def test_cancel(self):
# #     #     """Tests cancelling a transaction"""
# #     #     tmp = np.array([(1,'Peter','mouse'),(2, 'Klara', 'monkey')],
# #     #                    dtype=[('subject_id', '>i4'), ('real_id', 'O'), ('species', 'O')])
# #     #
# #     #     self.relvar.insert(tmp[0])
# #     #     with self.conn.transaction() as transaction:
# #     #         self.relvar.insert(tmp[1])
# #     #         transaction.cancel()
# #     #
# #     #     testt2 = (self.relvar & 'subject_id = 2').fetch()
# #     #     assert_equal(len(testt2), 0, "Length is not 0. Expected because rollback should have happened.")
#
#
#
# # class TestConnectionWithBindings(object):
# #     """
# #     Tests heading and dependency loadings
# #     """
# #     def setup(self):
# #         self.conn = dj.Connection(**CONN_INFO)
# #         cur.execute(query)
#
#
#
#
#
#
#
#
#
#
