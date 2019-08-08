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
import os
from pathlib import Path
from minio import Minio
import urllib3
import certifi
import shutil

__author__ = 'Edgar Walker, Fabian Sinz, Dimitri Yatsenko, Raphael Guzman'

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

if LooseVersion(conn_root.query(
        "select @@version;").fetchone()[0]) >= LooseVersion('8.0.0'):
    # create user if necessary on mysql8
    conn_root.query("""
            CREATE USER IF NOT EXISTS 'datajoint'@'%%'
            IDENTIFIED BY 'datajoint';
            """)
    conn_root.query("""
            CREATE USER IF NOT EXISTS 'djview'@'%%'
            IDENTIFIED BY 'djview';
            """)
    conn_root.query("""
            CREATE USER IF NOT EXISTS 'djssl'@'%%'
            IDENTIFIED BY 'djssl'
            REQUIRE SSL;
            """)
    conn_root.query(
        "GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%';")
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%';")
else:
    # grant permissions. For mysql5.6/5.7 this also automatically creates user
    # if not exists
    conn_root.query("""
        GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%'
        IDENTIFIED BY 'datajoint';
        """)
    conn_root.query(
        "GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%' IDENTIFIED BY 'djview';"
        )
    conn_root.query("""
        GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%'
        IDENTIFIED BY 'djssl'
        REQUIRE SSL;
        """)

# Initialize httpClient with relevant timeout.
httpClient = urllib3.PoolManager(
            timeout=30,
            cert_reqs='CERT_REQUIRED',
            ca_certs=certifi.where(),
            retries=urllib3.Retry(
                total=3,
                backoff_factor=0.2,
                status_forcelist=[500, 502, 503, 504]
            )
        )

# Initialize minioClient with an endpoint and access/secret keys.
minioClient = Minio(S3_CONN_INFO['endpoint'],
                    access_key=S3_CONN_INFO['access_key'],
                    secret_key=S3_CONN_INFO['secret_key'],
                    secure=False,
                    http_client=httpClient)


def setup_package():
    """
    Package-level unit test setup
    Turns off safemode
    """
    dj.config['safemode'] = False

    # Add old MySQL
    source = os.path.dirname(os.path.realpath(__file__)) + \
        "/external-legacy-data"
    db_name = "djtest_blob_migrate"
    db_file = "v0_11.sql"
    conn_root.query("""
        CREATE DATABASE {};
        """.format(db_name))

    def parse_sql(filename):
        stmts = []
        DELIMITER = ';'
        stmt = ''
        for line in open(filename, 'r').readlines():
            if not line.strip():
                continue

            if line.startswith('--'):
                continue

            if 'DELIMITER' in line:
                DELIMITER = line.split()[1]
                continue

            if (DELIMITER not in line):
                stmt += line.replace(DELIMITER, ';')
                continue

            if stmt:
                stmt += line
                stmts.append(stmt.strip())
                stmt = ''
            else:
                stmts.append(line.strip())
        return stmts

    stmts = parse_sql('{}/{}'.format(source, db_file))
    for stmt in stmts:
        conn_root.query(stmt)

    # Add old S3
    source = os.path.dirname(os.path.realpath(__file__)) + \
        "/external-legacy-data/s3"
    bucket = "migrate-test"
    region = "us-east-1"
    minioClient.make_bucket(bucket, location=region)

    pathlist = Path(source).glob('**/*')
    for path in pathlist:
        if os.path.isfile(str(path)) and ".sql" not in str(path):
            minioClient.fput_object(
                    bucket, os.path.relpath(
                        str(path),
                        '{}/{}'.format(source, bucket)
                        ), str(path))

    # Add S3
    minioClient.make_bucket("datajoint-test", location=region)

    # Add old File Content
    shutil.copytree(
            os.path.dirname(os.path.realpath(__file__)) +
            "/external-legacy-data/file/temp",
            os.path.expanduser('~/temp'))


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

    # Remove old S3
    bucket = "migrate-test"
    objs = list(minioClient.list_objects_v2(
            bucket, recursive=True))
    objs = [minioClient.remove_object(bucket,
            o.object_name.encode('utf-8')) for o in objs]
    minioClient.remove_bucket(bucket)

    # Remove S3
    bucket = "datajoint-test"
    objs = list(minioClient.list_objects_v2(bucket, recursive=True))
    objs = [minioClient.remove_object(bucket,
            o.object_name.encode('utf-8')) for o in objs]
    minioClient.remove_bucket(bucket)

    # Remove old File Content
    shutil.rmtree(os.path.expanduser('~/temp'))
