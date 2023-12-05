import datajoint as dj
from packaging import version
import os
from os import environ, remove
import minio
import urllib3
import certifi
from distutils.version import LooseVersion
import shutil
import pytest
import networkx as nx
import json
from pathlib import Path
import tempfile
from datajoint import errors
from datajoint.errors import (
    ADAPTED_TYPE_SWITCH, FILEPATH_FEATURE_SWITCH
)
from . import (
    PREFIX, CONN_INFO, S3_CONN_INFO,
    schema, schema_simple, schema_advanced, schema_adapted
)


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="module")
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="session")
def connection_root_bare():
    connection = dj.Connection(
        host=os.getenv("DJ_HOST"),
        user=os.getenv("DJ_USER"),
        password=os.getenv("DJ_PASS"),
    )
    yield connection


@pytest.fixture(scope="session")
def connection_root(connection_root_bare):
    """Root user database connection."""
    dj.config["safemode"] = False
    conn_root = connection_root_bare
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

    yield conn_root

    # Teardown
    conn_root.query("SET FOREIGN_KEY_CHECKS=0")
    cur = conn_root.query('SHOW DATABASES LIKE "{}\_%%"'.format(PREFIX))
    for db in cur.fetchall():
        conn_root.query("DROP DATABASE `{}`".format(db[0]))
    conn_root.query("SET FOREIGN_KEY_CHECKS=1")
    if os.path.exists("dj_local_conf.json"):
        remove("dj_local_conf.json")

    # Remove created users
    conn_root.query("DROP USER IF EXISTS `datajoint`")
    conn_root.query("DROP USER IF EXISTS `djview`")
    conn_root.query("DROP USER IF EXISTS `djssl`")
    conn_root.close()


@pytest.fixture(scope="session")
def connection_test(connection_root):
    """Test user database connection."""
    database = f"{PREFIX}%%"
    credentials = dict(host=os.getenv("DJ_HOST"), user="datajoint", password="datajoint")
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


@pytest.fixture
def schema_any(connection_test):
    schema_any = dj.Schema(
        PREFIX + "_test1", schema.LOCALS_ANY, connection=connection_test
    )
    assert schema.LOCALS_ANY, "LOCALS_ANY is empty"
    schema_any(schema.TTest)
    schema_any(schema.TTest2)
    schema_any(schema.TTest3)
    schema_any(schema.NullableNumbers)
    schema_any(schema.TTestExtra)
    schema_any(schema.TTestNoExtra)
    schema_any(schema.Auto)
    schema_any(schema.User)
    schema_any(schema.Subject)
    schema_any(schema.Language)
    schema_any(schema.Experiment)
    schema_any(schema.Trial)
    schema_any(schema.Ephys)
    schema_any(schema.Image)
    schema_any(schema.UberTrash)
    schema_any(schema.UnterTrash)
    schema_any(schema.SimpleSource)
    schema_any(schema.SigIntTable)
    schema_any(schema.SigTermTable)
    schema_any(schema.DjExceptionName)
    schema_any(schema.ErrorClass)
    schema_any(schema.DecimalPrimaryKey)
    schema_any(schema.IndexRich)
    schema_any(schema.ThingA)
    schema_any(schema.ThingB)
    schema_any(schema.ThingC)
    schema_any(schema.Parent)
    schema_any(schema.Child)
    schema_any(schema.ComplexParent)
    schema_any(schema.ComplexChild)
    schema_any(schema.SubjectA)
    schema_any(schema.SessionA)
    schema_any(schema.SessionStatusA)
    schema_any(schema.SessionDateA)
    schema_any(schema.Stimulus)
    schema_any(schema.Longblob)
    yield schema_any
    schema_any.drop()


@pytest.fixture
def schema_simp(connection_test):
    schema = dj.Schema(
        PREFIX + "_relational", schema_simple.LOCALS_SIMPLE, connection=connection_test
    )
    schema(schema_simple.IJ)
    schema(schema_simple.JI)
    schema(schema_simple.A)
    schema(schema_simple.B)
    schema(schema_simple.L)
    schema(schema_simple.D)
    schema(schema_simple.E)
    schema(schema_simple.F)
    schema(schema_simple.F)
    schema(schema_simple.DataA)
    schema(schema_simple.DataB)
    schema(schema_simple.Website)
    schema(schema_simple.Profile)
    schema(schema_simple.Website)
    schema(schema_simple.TTestUpdate)
    schema(schema_simple.ArgmaxTest)
    schema(schema_simple.ReservedWord)
    schema(schema_simple.OutfitLaunch)
    yield schema
    schema.drop()


@pytest.fixture
def schema_adv(connection_test):
    schema = dj.Schema(
        PREFIX + "_advanced", schema_advanced.LOCALS_ADVANCED, connection=connection_test
    )
    schema(schema_advanced.Person)
    schema(schema_advanced.Parent)
    schema(schema_advanced.Subject)
    schema(schema_advanced.Prep)
    schema(schema_advanced.Slice)
    schema(schema_advanced.Cell)
    schema(schema_advanced.InputCell)
    schema(schema_advanced.LocalSynapse)
    schema(schema_advanced.GlobalSynapse)
    yield schema
    schema.drop()


@pytest.fixture(scope='session')
def http_client():
    # Initialize httpClient with relevant timeout.
    client = urllib3.PoolManager(
        timeout=30,
        cert_reqs="CERT_REQUIRED",
        ca_certs=certifi.where(),
        retries=urllib3.Retry(
            total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
        ),
    )
    yield client


@pytest.fixture(scope='session')
def minio_client_bare(http_client):
    client = minio.Minio(
        S3_CONN_INFO["endpoint"],
        access_key=S3_CONN_INFO["access_key"],
        secret_key=S3_CONN_INFO["secret_key"],
        secure=True,
        http_client=http_client,
    )
    return client


@pytest.fixture(scope='session')
def minio_client(minio_client_bare):
    """Initialize MinIO with an endpoint and access/secret keys."""
    # Bootstrap MinIO bucket
    aws_region = "us-east-1"
    try:
        minio_client_bare.make_bucket(S3_CONN_INFO["bucket"], location=aws_region)
    except minio.error.S3Error as e:
        if e.code != "BucketAlreadyOwnedByYou":
            raise e

    yield minio_client_bare

    # Teardown S3
    objs = list(minio_client_bare.list_objects(S3_CONN_INFO["bucket"], recursive=True))
    objs = [
        minio_client_bare.remove_object(S3_CONN_INFO["bucket"], o.object_name.encode("utf-8"))
        for o in objs
    ]
    minio_client_bare.remove_bucket(S3_CONN_INFO["bucket"])
