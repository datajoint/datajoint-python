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
    'host': environ.get('DJ_TEST_HOST', '127.0.0.1'),
    'user': environ.get('DJ_TEST_USER', 'root'),
    'passwd': environ.get('DJ_TEST_PASSWORD', None)
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
    cur.execute("SHOW DATABASES LIKE '{}\_%'".format(PREFIX))
    dbs = [x[0] for x in cur.fetchall()]
    cur.execute('SET FOREIGN_KEY_CHECKS=0') # unset foreign key check while deleting
    for db in dbs:
        cur.execute('DROP DATABASE `{}`'.format(db))
    cur.execute('SET FOREIGN_KEY_CHECKS=1') # set foreign key check back on






