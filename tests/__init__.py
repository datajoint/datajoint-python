import datajoint as dj
from packaging import version
import pytest
import os

PREFIX = os.environ.get("DJ_TEST_DB_PREFIX", "djtest")

# Connection for testing
CONN_INFO = dict(
    host=os.environ.get("DJ_TEST_HOST", "fakeservices.datajoint.io"),
    user=os.environ.get("DJ_TEST_USER", "datajoint"),
    password=os.environ.get("DJ_TEST_PASSWORD", "datajoint"),
)

CONN_INFO_ROOT = dict(
    host=os.environ.get("DJ_HOST", "fakeservices.datajoint.io"),
    user=os.environ.get("DJ_USER", "root"),
    password=os.environ.get("DJ_PASS", "simple"),
)

S3_CONN_INFO = dict(
    endpoint=os.environ.get("S3_ENDPOINT", "fakeservices.datajoint.io"),
    access_key=os.environ.get("S3_ACCESS_KEY", "datajoint"),
    secret_key=os.environ.get("S3_SECRET_KEY", "datajoint"),
    bucket=os.environ.get("S3_BUCKET", "datajoint.test"),
)
