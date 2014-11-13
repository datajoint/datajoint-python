"""
Package for testing datajoint. Setup fixture will be run
to ensure that proper database connection and access privilege
exists. The content of the test database will be destroyed
after the test.
"""

import pymysql
from os import environ

# Connection information for testing
CONN_INFO = {
    'host': environ.get('DJ_TEST_HOST', 'localhost'),
    'user': environ.get('DJ_TEST_USER', 'dj_test'),
    'passwd': environ.get('DJ_TEST_PASSWORD', 'dj_test')
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
    Removes all databases beginning with the prefix
    """
    cur = BASE_CONN.cursor()
    cur.execute("SHOW DATABASES LIKE '{}\_%'".format(PREFIX))
    dbs = [x[0] for x in cur.fetchall()]
    for db in dbs:
        cur.execute('DROP DATABASE `{}`'.format(db))






