"""
Package for testing datajoint. Setup fixture will be run
to ensure that proper database connection and access privilege
exists. The content of the test database will be destroyed
after the test.
"""

import logging
from os import environ, remove
import datajoint as dj
from distutils.version import LooseVersion

__author__ = 'Edgar Walker, Fabian Sinz, Dimitri Yatsenko'

# turn on verbose logging
logging.basicConfig(level=logging.DEBUG)

__all__ = ['__author__', 'PREFIX', 'CONN_INFO']

# Connection for testing
CONN_INFO = dict(
    host=environ.get('DJ_TEST_HOST', 'localhost'),
    user=environ.get('DJ_TEST_USER', 'datajoint'),
    password=environ.get('DJ_TEST_PASSWORD', 'datajoint'))

CONN_INFO_ROOT = dict(
    host=environ.get('DJ_HOST', 'localhost'),
    user=environ.get('DJ_USER', 'root'),
    password=environ.get('DJ_PASS', 'simple'))

S3_CONN_INFO = dict(
    endpoint=environ.get('S3_ENDPOINT', 'localhost:9000'),
    access_key=environ.get('S3_ACCESS_KEY', 'datajoint'),
    secret_key=environ.get('S3_SECRET_KEY', 'datajoint'),
    bucket=environ.get('S3_BUCKET', 'datajoint-test'))

# Prefix for all databases used during testing
PREFIX = environ.get('DJ_TEST_DB_PREFIX', 'djtest')
conn_root = dj.conn(**CONN_INFO_ROOT)

if LooseVersion(conn_root.query("select @@version;").fetchone()[0]) >= LooseVersion('8.0.0'):
    # create user if necessary on mysql8
    conn_root.query("CREATE USER IF NOT EXISTS 'datajoint'@'%%'  IDENTIFIED BY 'datajoint';")
    conn_root.query("CREATE USER IF NOT EXISTS 'djview'@'%%' IDENTIFIED BY 'djview';")
    conn_root.query("CREATE USER IF NOT EXISTS 'djssl'@'%%' IDENTIFIED BY 'djssl';")
    conn_root.query(
        "GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%' REQUIRE SSL;")
else:
    # grant permissions. For mysql5.6/5.7 this also automatically creates user if not exists
    conn_root.query(
        "GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%' IDENTIFIED BY 'datajoint';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%' IDENTIFIED BY 'djview';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%' IDENTIFIED BY 'djssl' REQUIRE SSL;")


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
    remove("dj_local_conf.json")
