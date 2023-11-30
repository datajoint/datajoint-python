import datajoint as dj
from packaging import version
import pytest
import os

PREFIX = "djtest"

# Connection for testing
CONN_INFO = dict(
    host=os.getenv("DJ_HOST"),
    user=os.getenv("DJ_USER"),
    password=os.getenv("DJ_PASS"),
)

CONN_INFO_ROOT = dict(
    host=os.getenv("DJ_HOST"),
    user=os.getenv("DJ_USER"),
    password=os.getenv("DJ_PASS"),
)
