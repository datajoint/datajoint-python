"""
Collection of test cases to test connection module.
"""

import datajoint as dj
from datajoint import DataJointError
import numpy as np
from . import CONN_INFO_ROOT, connection_root, connection_test

from . import PREFIX
import pymysql
import pytest


def test_set_password_prompt_match(monkeypatch):
    """
    Should be able to change the password using user prompt
    """
    c = dj.conn(**CONN_INFO_ROOT)
    c.query("CREATE USER 'alice'@'%' IDENTIFIED BY 'pass';")
    # prompt responses: new password / confirm password / update local setting?
    responses = ["newpass", "newpass", "yes"]
    monkeypatch.setattr('getpass.getpass', lambda _: next(responses)) 
    monkeypatch.setattr('input', lambda _: next(responses)) 

    dj.set_password()

    with pytest.raises(pymysql.err.OperationalError):
        # should not be able to log in with old credentials
        dj.conn(host=CONN_INFO_ROOT["host"], user="alice", password="pass")

    # should be able to log in with new credentials
    dj.conn(host=CONN_INFO_ROOT["host"], user="alice", password="newpass")

    assert dj.config["database.password"] == "newpass"

def test_set_password_prompt_mismatch(monkeypatch):
    """
    Should not be able to change the password when passwords do not match
    """
    pass

def test_set_password_arg(monkeypatch):
    """
    Should be able to change the password with an argument
    """
    pass

def test_set_password_no_update_config(monkeypatch):
    """
    Should be able to change the password without updating local config
    """
    pass