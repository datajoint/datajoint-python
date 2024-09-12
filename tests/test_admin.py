"""
Collection of test cases to test admin module.
"""

import datajoint as dj
import os
import pymysql
import pytest


@pytest.fixture()
def user_alice(db_creds_root) -> dict:
    # set up - reset config, log in as root, and create a new user alice
    # reset dj.config manually because its state may be changed by these tests
    if os.path.exists(dj.settings.LOCALCONFIG):
        os.remove(dj.settings.LOCALCONFIG)
    dj.config["database.password"] = os.getenv("DJ_PASS")
    root_conn = dj.conn(**db_creds_root, reset=True)
    new_credentials = dict(
        host=db_creds_root["host"],
        user="alice",
        password="oldpass",
    )
    root_conn.query(f"DROP USER IF EXISTS '{new_credentials['user']}'@'%%';")
    root_conn.query(
        f"CREATE USER '{new_credentials['user']}'@'%%' "
        f"IDENTIFIED BY '{new_credentials['password']}';"
    )

    # test the connection
    dj.Connection(**new_credentials)

    # return alice's credentials
    yield new_credentials

    # tear down - delete the user and the local config file
    root_conn.query(f"DROP USER '{new_credentials['user']}'@'%%';")
    if os.path.exists(dj.settings.LOCALCONFIG):
        os.remove(dj.settings.LOCALCONFIG)


def test_set_password_prompt_match(monkeypatch, user_alice: dict):
    """
    Should be able to change the password using user prompt
    """
    # reset the connection to use alice's credentials
    dj.conn(**user_alice, reset=True)

    # prompts: new password / confirm password
    password_resp = iter(["newpass", "newpass"])
    # NOTE: because getpass.getpass is imported in datajoint.admin and used as
    # getpass in that module, we need to patch datajoint.admin.getpass
    # instead of getpass.getpass
    monkeypatch.setattr("datajoint.admin.getpass", lambda _: next(password_resp))

    # respond no to prompt to update local config
    monkeypatch.setattr("builtins.input", lambda _: "no")

    # reset password of user of current connection (alice)
    dj.set_password()

    # should not be able to connect with old credentials
    with pytest.raises(pymysql.err.OperationalError):
        dj.Connection(**user_alice)

    # should be able to connect with new credentials
    dj.Connection(host=user_alice["host"], user=user_alice["user"], password="newpass")

    # check that local config is not updated
    assert dj.config["database.password"] == os.getenv("DJ_PASS")
    assert not os.path.exists(dj.settings.LOCALCONFIG)


def test_set_password_prompt_mismatch(monkeypatch, user_alice: dict):
    """
    Should not be able to change the password when passwords do not match
    """
    # reset the connection to use alice's credentials
    dj.conn(**user_alice, reset=True)

    # prompts: new password / confirm password
    password_resp = iter(["newpass", "wrong"])
    # NOTE: because getpass.getpass is imported in datajoint.admin and used as
    # getpass in that module, we need to patch datajoint.admin.getpass
    # instead of getpass.getpass
    monkeypatch.setattr("datajoint.admin.getpass", lambda _: next(password_resp))

    # reset password of user of current connection (alice)
    # should be nop
    dj.set_password()

    # should be able to connect with old credentials
    dj.Connection(**user_alice)


def test_set_password_args(user_alice: dict):
    """
    Should be able to change the password with an argument
    """
    # reset the connection to use alice's credentials
    dj.conn(**user_alice, reset=True)

    # reset password of user of current connection (alice)
    dj.set_password(new_password="newpass", update_config=False)

    # should be able to connect with new credentials
    dj.Connection(host=user_alice["host"], user=user_alice["user"], password="newpass")


def test_set_password_update_config(monkeypatch, user_alice: dict):
    """
    Should be able to change the password and update local config
    """
    # reset the connection to use alice's credentials
    dj.conn(**user_alice, reset=True)

    # respond yes to prompt to update local config
    monkeypatch.setattr("builtins.input", lambda _: "yes")

    # reset password of user of current connection (alice)
    dj.set_password(new_password="newpass")

    # should be able to connect with new credentials
    dj.Connection(host=user_alice["host"], user=user_alice["user"], password="newpass")

    # check that local config is updated
    # NOTE: the global config state is changed unless dj modules are reloaded
    # NOTE: this test is a bit unrealistic because the config user does not match
    # the user whose password is being updated, so the config credentials
    # will be invalid after update...
    assert dj.config["database.password"] == "newpass"
    assert os.path.exists(dj.settings.LOCALCONFIG)


def test_set_password_conn(user_alice: dict):
    """
    Should be able to change the password using a given connection
    """
    # create a connection with alice's credentials
    conn_alice = dj.Connection(**user_alice)

    # reset password of user of alice's connection (alice) and do not update config
    dj.set_password(new_password="newpass", connection=conn_alice, update_config=False)

    # should be able to connect with new credentials
    dj.Connection(host=user_alice["host"], user=user_alice["user"], password="newpass")

    # check that local config is not updated
    assert dj.config["database.password"] == os.getenv("DJ_PASS")
    assert not os.path.exists(dj.settings.LOCALCONFIG)
