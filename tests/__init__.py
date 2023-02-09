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
import minio
import urllib3
import certifi
import shutil
from datajoint.utils import parse_sql

__author__ = "Edgar Walker, Fabian Sinz, Dimitri Yatsenko, Raphael Guzman"

# turn on verbose logging
logging.basicConfig(level=logging.DEBUG)

__all__ = ["__author__", "PREFIX", "CONN_INFO"]

# Connection for testing
CONN_INFO = dict(
    host=environ.get("DJ_TEST_HOST", "fakeservices.datajoint.io"),
    user=environ.get("DJ_TEST_USER", "datajoint"),
    password=environ.get("DJ_TEST_PASSWORD", "datajoint"),
)

CONN_INFO_ROOT = dict(
    host=environ.get("DJ_HOST", "fakeservices.datajoint.io"),
    user=environ.get("DJ_USER", "root"),
    password=environ.get("DJ_PASS", "simple"),
)

S3_CONN_INFO = dict(
    endpoint=environ.get("S3_ENDPOINT", "fakeservices.datajoint.io"),
    access_key=environ.get("S3_ACCESS_KEY", "datajoint"),
    secret_key=environ.get("S3_SECRET_KEY", "datajoint"),
    bucket=environ.get("S3_BUCKET", "datajoint.test"),
)

# Prefix for all databases used during testing
PREFIX = environ.get("DJ_TEST_DB_PREFIX", "djtest")
conn_root = dj.conn(**CONN_INFO_ROOT)

# Initialize httpClient with relevant timeout.
httpClient = urllib3.PoolManager(
    timeout=30,
    cert_reqs="CERT_REQUIRED",
    ca_certs=certifi.where(),
    retries=urllib3.Retry(
        total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
    ),
)

# Initialize minioClient with an endpoint and access/secret keys.
minioClient = minio.Minio(
    S3_CONN_INFO["endpoint"],
    access_key=S3_CONN_INFO["access_key"],
    secret_key=S3_CONN_INFO["secret_key"],
    secure=True,
    http_client=httpClient,
)


def setup_package():
    """
    Package-level unit test setup
    Turns off safemode
    """
    dj.config["safemode"] = False

    # Create MySQL users
    if LooseVersion(conn_root.query("select @@version;").fetchone()[0]) >= LooseVersion(
        "8.0.0"
    ):
        # create user if necessary on mysql8
        conn_root.query(
            """
                CREATE USER IF NOT EXISTS 'datajoint'@'%%'
                IDENTIFIED BY 'datajoint';
                """
        )
        conn_root.query(
            """
                CREATE USER IF NOT EXISTS 'djview'@'%%'
                IDENTIFIED BY 'djview';
                """
        )
        conn_root.query(
            """
                CREATE USER IF NOT EXISTS 'djssl'@'%%'
                IDENTIFIED BY 'djssl'
                REQUIRE SSL;
                """
        )
        conn_root.query("GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%';")
        conn_root.query("GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%';")
        conn_root.query("GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%';")
    else:
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
        conn_root.query(
            """
            GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%'
            IDENTIFIED BY 'datajoint';
            """
        )
        conn_root.query(
            "GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%' IDENTIFIED BY 'djview';"
        )
        conn_root.query(
            """
            GRANT SELECT ON `djtest%%`.* TO 'djssl'@'%%'
            IDENTIFIED BY 'djssl'
            REQUIRE SSL;
            """
        )

    region = "us-east-1"
    # Add S3
    try:
        minioClient.make_bucket(S3_CONN_INFO["bucket"], location=region)
    except minio.error.S3Error as e:
        if e.code != "BucketAlreadyOwnedByYou":
            raise e


def teardown_package():
    """
    Package-level unit test teardown.
    Removes all databases with name starting with PREFIX.
    To deal with possible foreign key constraints, it will unset
    and then later reset FOREIGN_KEY_CHECKS flag
    """
    conn_root.query("SET FOREIGN_KEY_CHECKS=0")
    cur = conn_root.query('SHOW DATABASES LIKE "{}\_%%"'.format(PREFIX))
    for db in cur.fetchall():
        conn_root.query("DROP DATABASE `{}`".format(db[0]))
    conn_root.query("SET FOREIGN_KEY_CHECKS=1")
    if os.path.exists("dj_local_conf.json"):
        remove("dj_local_conf.json")

    # Remove created users
    conn_root.query("DROP USER `datajoint`")
    conn_root.query("DROP USER `djview`")
    conn_root.query("DROP USER `djssl`")

    # Remove S3
    objs = list(minioClient.list_objects(S3_CONN_INFO["bucket"], recursive=True))
    objs = [
        minioClient.remove_object(S3_CONN_INFO["bucket"], o.object_name.encode("utf-8"))
        for o in objs
    ]
    minioClient.remove_bucket(S3_CONN_INFO["bucket"])
