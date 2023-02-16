import datajoint as dj
from packaging import version
import pytest
import os

PREFIX = "djtest"

CONN_INFO_ROOT = dict(
    host=os.getenv("DJ_HOST"),
    user=os.getenv("DJ_USER"),
    password=os.getenv("DJ_PASS"),
)


@pytest.fixture
def connection_root():
    """Root user database connection."""
    dj.config["safemode"] = False
    connection = dj.Connection(
        host=os.getenv("DJ_HOST"),
        user=os.getenv("DJ_USER"),
        password=os.getenv("DJ_PASS"),
    )
    yield connection
    dj.config["safemode"] = True
    connection.close()


@pytest.fixture
def connection_test(connection_root):
    """Test user database connection."""
    database = f"{PREFIX}%%"
    credentials = dict(
        host=os.getenv("DJ_HOST"), user="datajoint", password="datajoint"
    )
    permission = "ALL PRIVILEGES"

    # Create MySQL users
    if version.parse(
        connection_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
        # create user if necessary on mysql8
        connection_root.query(
            f"""
            CREATE USER IF NOT EXISTS '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON `{database}`.*
            TO '{credentials["user"]}'@'%%';
            """
        )
    else:
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
        connection_root.query(
            f"""
            GRANT {permission} ON `{database}`.*
            TO '{credentials["user"]}'@'%%'
            IDENTIFIED BY '{credentials["password"]}';
            """
        )

    connection = dj.Connection(**credentials)
    yield connection
    connection_root.query(f"""DROP USER `{credentials["user"]}`""")
    connection.close()
