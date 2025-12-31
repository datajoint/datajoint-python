"""
Pytest configuration for DataJoint tests.

Expects MySQL and MinIO services to be running via docker-compose:
    docker-compose up -d db minio

Environment variables (with defaults from docker-compose.yaml):
    DJ_HOST=db          MySQL host
    DJ_USER=root        MySQL root user
    DJ_PASS=password    MySQL root password
    S3_ENDPOINT=minio:9000   MinIO endpoint
    S3_ACCESS_KEY=datajoint  MinIO access key
    S3_SECRET_KEY=datajoint  MinIO secret key
"""

import logging
import os
from os import remove
from typing import Dict, List

import certifi
import minio
import pytest
import urllib3
from packaging import version

import datajoint as dj
from datajoint.errors import (
    FILEPATH_FEATURE_SWITCH,
    DataJointError,
)

from . import schema, schema_advanced, schema_external, schema_object, schema_simple
from . import schema_uuid as schema_uuid_module
from . import schema_type_aliases as schema_type_aliases_module

logger = logging.getLogger(__name__)


# --- Database connection fixtures ---


@pytest.fixture(scope="session")
def prefix():
    return os.environ.get("DJ_TEST_DB_PREFIX", "djtest")


@pytest.fixture(scope="session")
def db_creds_root() -> Dict:
    """Root database credentials from environment."""
    host = os.environ.get("DJ_HOST", "db")
    port = os.environ.get("DJ_PORT", "3306")
    return dict(
        host=f"{host}:{port}" if port else host,
        user=os.environ.get("DJ_USER", "root"),
        password=os.environ.get("DJ_PASS", "password"),
    )


@pytest.fixture(scope="session")
def db_creds_test() -> Dict:
    """Test user database credentials from environment."""
    host = os.environ.get("DJ_HOST", "db")
    port = os.environ.get("DJ_PORT", "3306")
    return dict(
        host=f"{host}:{port}" if port else host,
        user=os.environ.get("DJ_TEST_USER", "datajoint"),
        password=os.environ.get("DJ_TEST_PASSWORD", "datajoint"),
    )


@pytest.fixture(scope="session")
def s3_creds() -> Dict:
    """S3/MinIO credentials from environment."""
    return dict(
        endpoint=os.environ.get("S3_ENDPOINT", "minio:9000"),
        access_key=os.environ.get("S3_ACCESS_KEY", "datajoint"),
        secret_key=os.environ.get("S3_SECRET_KEY", "datajoint"),
        bucket=os.environ.get("S3_BUCKET", "datajoint.test"),
    )


@pytest.fixture(scope="session", autouse=True)
def configure_datajoint(db_creds_root):
    """Configure DataJoint to use docker-compose services."""
    host = os.environ.get("DJ_HOST", "db")
    port = os.environ.get("DJ_PORT", "3306")

    dj.config["database.host"] = host
    dj.config["database.port"] = int(port)
    dj.config["safemode"] = False

    logger.info(f"Configured DataJoint to use MySQL at {host}:{port}")


@pytest.fixture(scope="session")
def connection_root_bare(db_creds_root):
    """Bare root connection without user setup."""
    connection = dj.Connection(**db_creds_root)
    yield connection


@pytest.fixture(scope="session")
def connection_root(connection_root_bare, prefix):
    """Root database connection with test users created."""
    conn_root = connection_root_bare

    # Create MySQL users
    if version.parse(conn_root.query("select @@version;").fetchone()[0]) >= version.parse("8.0.0"):
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
        conn_root.query(
            """
            GRANT ALL PRIVILEGES ON `djtest%%`.* TO 'datajoint'@'%%'
            IDENTIFIED BY 'datajoint';
            """
        )
        conn_root.query("GRANT SELECT ON `djtest%%`.* TO 'djview'@'%%' IDENTIFIED BY 'djview';")
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
    cur = conn_root.query('SHOW DATABASES LIKE "{}\\_%%"'.format(prefix))
    for db in cur.fetchall():
        conn_root.query("DROP DATABASE `{}`".format(db[0]))
    conn_root.query("SET FOREIGN_KEY_CHECKS=1")
    if os.path.exists("dj_local_conf.json"):
        remove("dj_local_conf.json")

    conn_root.query("DROP USER IF EXISTS `datajoint`")
    conn_root.query("DROP USER IF EXISTS `djview`")
    conn_root.query("DROP USER IF EXISTS `djssl`")
    conn_root.close()


@pytest.fixture(scope="session")
def connection_test(connection_root, prefix, db_creds_test):
    """Test user database connection."""
    database = f"{prefix}%%"
    permission = "ALL PRIVILEGES"

    if version.parse(connection_root.query("select @@version;").fetchone()[0]) >= version.parse("8.0.0"):
        connection_root.query(
            f"""
            CREATE USER IF NOT EXISTS '{db_creds_test["user"]}'@'%%'
            IDENTIFIED BY '{db_creds_test["password"]}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON `{database}`.*
            TO '{db_creds_test["user"]}'@'%%';
            """
        )
    else:
        connection_root.query(
            f"""
            GRANT {permission} ON `{database}`.*
            TO '{db_creds_test["user"]}'@'%%'
            IDENTIFIED BY '{db_creds_test["password"]}';
            """
        )

    connection = dj.Connection(**db_creds_test)
    yield connection
    connection_root.query(f"""DROP USER `{db_creds_test["user"]}`""")
    connection.close()


# --- S3/MinIO fixtures ---


@pytest.fixture(scope="session")
def stores_config(s3_creds, tmpdir_factory):
    return {
        "raw": dict(protocol="file", location=tmpdir_factory.mktemp("raw")),
        "repo": dict(
            stage=tmpdir_factory.mktemp("repo"),
            protocol="file",
            location=tmpdir_factory.mktemp("repo"),
        ),
        "repo-s3": dict(
            s3_creds,
            protocol="s3",
            location="dj/repo",
            stage=tmpdir_factory.mktemp("repo-s3"),
        ),
        "local": dict(protocol="file", location=tmpdir_factory.mktemp("local"), subfolding=(1, 1)),
        "share": dict(s3_creds, protocol="s3", location="dj/store/repo", subfolding=(2, 4)),
    }


@pytest.fixture
def mock_stores(stores_config):
    og_stores_config = dj.config.get("stores")
    dj.config["stores"] = stores_config
    yield
    if og_stores_config is None:
        del dj.config["stores"]
    else:
        dj.config["stores"] = og_stores_config


@pytest.fixture
def mock_cache(tmpdir_factory):
    og_cache = dj.config.get("cache")
    dj.config["cache"] = tmpdir_factory.mktemp("cache")
    yield
    if og_cache is None:
        del dj.config["cache"]
    else:
        dj.config["cache"] = og_cache


@pytest.fixture(scope="session")
def http_client():
    client = urllib3.PoolManager(
        timeout=30,
        cert_reqs="CERT_REQUIRED",
        ca_certs=certifi.where(),
        retries=urllib3.Retry(total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]),
    )
    yield client


@pytest.fixture(scope="session")
def minio_client_bare(s3_creds):
    """Initialize MinIO client."""
    return minio.Minio(
        endpoint=s3_creds["endpoint"],
        access_key=s3_creds["access_key"],
        secret_key=s3_creds["secret_key"],
        secure=False,
    )


@pytest.fixture(scope="session")
def minio_client(s3_creds, minio_client_bare, teardown=False):
    """MinIO client with test bucket created."""
    aws_region = "us-east-1"
    try:
        minio_client_bare.make_bucket(s3_creds["bucket"], location=aws_region)
    except minio.error.S3Error as e:
        if e.code != "BucketAlreadyOwnedByYou":
            raise e

    yield minio_client_bare

    if not teardown:
        return
    objs = list(minio_client_bare.list_objects(s3_creds["bucket"], recursive=True))
    objs = [minio_client_bare.remove_object(s3_creds["bucket"], o.object_name.encode("utf-8")) for o in objs]
    minio_client_bare.remove_bucket(s3_creds["bucket"])


# --- Utility fixtures ---


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="module")
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture
def enable_adapted_types():
    """Deprecated - custom attribute types no longer require a feature flag."""
    yield


@pytest.fixture
def enable_filepath_feature(monkeypatch):
    monkeypatch.setenv(FILEPATH_FEATURE_SWITCH, "TRUE")
    yield
    monkeypatch.delenv(FILEPATH_FEATURE_SWITCH, raising=True)


# --- Cleanup fixtures ---


@pytest.fixture
def clean_autopopulate(experiment, trial, ephys):
    """Cleanup fixture for autopopulate tests."""
    yield
    ephys.delete()
    trial.delete()
    experiment.delete()


@pytest.fixture
def clean_jobs(schema_any):
    """Cleanup fixture for jobs tests."""
    try:
        schema_any.jobs.delete()
    except DataJointError:
        pass
    yield


@pytest.fixture
def clean_test_tables(test, test_extra, test_no_extra):
    """Cleanup fixture for relation tests."""
    if not test:
        test.insert(test.contents, skip_duplicates=True)
    yield
    test.delete()
    test.insert(test.contents, skip_duplicates=True)
    test_extra.delete()
    test_no_extra.delete()


# --- Schema fixtures ---


@pytest.fixture(scope="module")
def schema_any(connection_test, prefix):
    schema_any = dj.Schema(prefix + "_test1", schema.LOCALS_ANY, connection=connection_test)
    assert schema.LOCALS_ANY, "LOCALS_ANY is empty"
    try:
        schema_any.jobs.delete()
    except DataJointError:
        pass
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
    schema_any(schema.ThingD)
    schema_any(schema.ThingE)
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
    try:
        schema_any.jobs.delete()
    except DataJointError:
        pass
    schema_any.drop()


@pytest.fixture
def schema_any_fresh(connection_test, prefix):
    """Function-scoped schema_any for tests that need fresh schema state."""
    schema_any = dj.Schema(prefix + "_test1_fresh", schema.LOCALS_ANY, connection=connection_test)
    assert schema.LOCALS_ANY, "LOCALS_ANY is empty"
    try:
        schema_any.jobs.delete()
    except DataJointError:
        pass
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
    schema_any(schema.ThingD)
    schema_any(schema.ThingE)
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
    try:
        schema_any.jobs.delete()
    except DataJointError:
        pass
    schema_any.drop()


@pytest.fixture
def thing_tables(schema_any):
    a = schema.ThingA()
    b = schema.ThingB()
    c = schema.ThingC()
    d = schema.ThingD()
    e = schema.ThingE()

    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))
    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    yield a, b, c, d, e


@pytest.fixture(scope="module")
def schema_simp(connection_test, prefix):
    schema = dj.Schema(prefix + "_relational", schema_simple.LOCALS_SIMPLE, connection=connection_test)
    schema(schema_simple.SelectPK)
    schema(schema_simple.KeyPK)
    schema(schema_simple.IJ)
    schema(schema_simple.JI)
    schema(schema_simple.A)
    schema(schema_simple.B)
    schema(schema_simple.L)
    schema(schema_simple.D)
    schema(schema_simple.E)
    schema(schema_simple.F)
    schema(schema_simple.F)
    schema(schema_simple.G)
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


@pytest.fixture(scope="module")
def schema_adv(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_advanced",
        schema_advanced.LOCALS_ADVANCED,
        connection=connection_test,
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


@pytest.fixture
def schema_ext(connection_test, enable_filepath_feature, mock_stores, mock_cache, prefix):
    schema = dj.Schema(
        prefix + "_extern",
        context=schema_external.LOCALS_EXTERNAL,
        connection=connection_test,
    )
    schema(schema_external.Simple)
    schema(schema_external.SimpleRemote)
    schema(schema_external.Seed)
    schema(schema_external.Dimension)
    schema(schema_external.Image)
    schema(schema_external.Attach)
    schema(schema_external.Filepath)
    schema(schema_external.FilepathS3)
    yield schema
    schema.drop()


@pytest.fixture(scope="module")
def schema_uuid(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_test1",
        context=schema_uuid_module.LOCALS_UUID,
        connection=connection_test,
    )
    schema(schema_uuid_module.Basic)
    schema(schema_uuid_module.Topic)
    schema(schema_uuid_module.Item)
    yield schema
    schema.drop()


@pytest.fixture(scope="module")
def schema_type_aliases(connection_test, prefix):
    """Schema for testing numeric type aliases."""
    schema = dj.Schema(
        prefix + "_type_aliases",
        context=schema_type_aliases_module.LOCALS_TYPE_ALIASES,
        connection=connection_test,
    )
    schema(schema_type_aliases_module.TypeAliasTable)
    schema(schema_type_aliases_module.TypeAliasPrimaryKey)
    schema(schema_type_aliases_module.TypeAliasNullable)
    yield schema
    schema.drop()


# --- Table fixtures ---


@pytest.fixture
def test(schema_any):
    yield schema.TTest()


@pytest.fixture
def test2(schema_any):
    yield schema.TTest2()


@pytest.fixture
def test_extra(schema_any):
    yield schema.TTestExtra()


@pytest.fixture
def test_no_extra(schema_any):
    yield schema.TTestNoExtra()


@pytest.fixture
def user(schema_any):
    return schema.User()


@pytest.fixture
def lang(schema_any):
    yield schema.Language()


@pytest.fixture
def languages(lang) -> List:
    og_contents = lang.contents
    languages = og_contents.copy()
    yield languages
    lang.contents = og_contents


@pytest.fixture
def subject(schema_any):
    yield schema.Subject()


@pytest.fixture
def experiment(schema_any):
    return schema.Experiment()


@pytest.fixture
def ephys(schema_any):
    return schema.Ephys()


@pytest.fixture
def img(schema_any):
    return schema.Image()


@pytest.fixture
def trial(schema_any):
    return schema.Trial()


@pytest.fixture
def channel(schema_any):
    return schema.Ephys.Channel()


@pytest.fixture
def trash(schema_any):
    return schema.UberTrash()


# --- Object storage fixtures ---


@pytest.fixture
def object_storage_config(tmpdir_factory):
    """Create object storage configuration for testing."""
    location = str(tmpdir_factory.mktemp("object_storage"))
    return {
        "project_name": "test_project",
        "protocol": "file",
        "location": location,
        "token_length": 8,
    }


@pytest.fixture
def mock_object_storage(object_storage_config, monkeypatch):
    """Mock object storage configuration in datajoint config."""
    original_object_storage = getattr(dj.config, "_object_storage", None)

    class MockObjectStorageSettings:
        def __init__(self, config):
            self.project_name = config["project_name"]
            self.protocol = config["protocol"]
            self.location = config["location"]
            self.token_length = config.get("token_length", 8)
            self.partition_pattern = config.get("partition_pattern")
            self.bucket = config.get("bucket")
            self.endpoint = config.get("endpoint")
            self.access_key = config.get("access_key")
            self.secret_key = config.get("secret_key")
            self.secure = config.get("secure", True)
            self.container = config.get("container")

    mock_settings = MockObjectStorageSettings(object_storage_config)
    monkeypatch.setattr(dj.config, "object_storage", mock_settings)

    yield object_storage_config

    if original_object_storage is not None:
        monkeypatch.setattr(dj.config, "object_storage", original_object_storage)


@pytest.fixture
def schema_obj(connection_test, prefix, mock_object_storage):
    """Schema for object type tests."""
    schema = dj.Schema(
        prefix + "_object",
        context=schema_object.LOCALS_OBJECT,
        connection=connection_test,
    )
    schema(schema_object.ObjectFile)
    schema(schema_object.ObjectFolder)
    schema(schema_object.ObjectMultiple)
    schema(schema_object.ObjectWithOther)
    yield schema
    schema.drop()
