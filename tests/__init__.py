"""
Package for testing datajoint. Setup fixture will be run
to ensure that proper database connection and access privilege
exists. The content of the test database will be destroyed
after the test.
"""

import logging
from os import environ
import datajoint as dj

__author__ = 'Edgar Walker, Fabian Sinz, Dimitri Yatsenko'

# turn on verbose logging
logging.basicConfig(level=logging.DEBUG)

__all__ = ['__author__', 'PREFIX', 'CONN_INFO']

# Connection for testing
CONN_INFO = dict(
    host=environ.get('DJ_TEST_HOST', 'localhost'),
    user=environ.get('DJ_TEST_USER', 'datajoint'),
    password=environ.get('DJ_TEST_PASSWORD', 'datajoint'))

# Prefix for all databases used during testing
PREFIX = environ.get('DJ_TEST_DB_PREFIX', 'djtest')


def setup_package():
    """
    Package-level unit test setup
    Turns off safemode
    """
    dj.config['safemode'] = False


def teardown_package():
    """
    Package-level unit test teardown.
    Removes all databases with name starting with PREFIX.
    To deal with possible foreign key constraints, it will unset
    and then later reset FOREIGN_KEY_CHECKS flag
    """
    conn = dj.conn(**CONN_INFO)
    conn.query('SET FOREIGN_KEY_CHECKS=0')
    cur = conn.query('SHOW DATABASES LIKE "{}\_%%"'.format(PREFIX))
    for db in cur.fetchall():
        conn.query('DROP DATABASE `{}`'.format(db[0]))
    conn.query('SET FOREIGN_KEY_CHECKS=1')
