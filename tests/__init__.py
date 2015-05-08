"""
Package for testing datajoint. Setup fixture will be run
to ensure that proper database connection and access privilege
exists. The content of the test database will be destroyed
after the test.
"""

import pymysql
import logging
from os import environ

logging.basicConfig(level=logging.DEBUG)

# Connection information for testing
CONN_INFO = {
    'host': environ.get('DJ_TEST_HOST', 'localhost'),
    'user': environ.get('DJ_TEST_USER', 'datajoint'),
    'passwd': environ.get('DJ_TEST_PASSWORD', 'datajoint')
}
# Prefix for all databases used during testing
PREFIX = environ.get('DJ_TEST_DB_PREFIX', 'dj')
# Bare connection used for verification of query results
BASE_CONN = pymysql.connect(**CONN_INFO)
BASE_CONN.autocommit(True)


def setup():
    cleanup()


def teardown():
    cleanup()


def cleanup():
    """
    Removes all databases with name starting with the prefix.
    To deal with possible foreign key constraints, it will unset
    and then later reset FOREIGN_KEY_CHECKS flag
    """
    cur = BASE_CONN.cursor()
    # cancel any unfinished transactions
    cur.execute("ROLLBACK")
    # start a transaction now
    cur.execute("START TRANSACTION WITH CONSISTENT SNAPSHOT")
    cur.execute("SHOW DATABASES LIKE '{}\_%'".format(PREFIX))
    dbs = [x[0] for x in cur.fetchall()]
    cur.execute('SET FOREIGN_KEY_CHECKS=0') # unset foreign key check while deleting
    for db in dbs:
        cur.execute('DROP DATABASE `{}`'.format(db))
    cur.execute('SET FOREIGN_KEY_CHECKS=1') # set foreign key check back on
    cur.execute("COMMIT")

def setup_sample_db():
    """
    Helper method to setup databases with tables to be used
    during the test
    """
    cur = BASE_CONN.cursor()
    cur.execute("CREATE DATABASE IF NOT EXISTS `{}_test1`".format(PREFIX))
    cur.execute("CREATE DATABASE IF NOT EXISTS `{}_test2`".format(PREFIX))
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
    query2 = """
    CREATE TABLE `{prefix}_test2`.`experimenter`
    (
      experimenter_id       SMALLINT        COMMENT 'Unique experimenter ID',
      experimenter_name     VARCHAR(255)    COMMENT 'Experimenter name',
      PRIMARY KEY (experimenter_id)
    )""".format(prefix=PREFIX)
    cur.execute(query2)






