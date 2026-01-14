"""
Pytest configuration for DataJoint tests.

Tests are organized by their dependencies:
- Unit tests: No external dependencies, run with `pytest -m "not requires_mysql"`
- Integration tests: Require MySQL/MinIO, marked with @pytest.mark.requires_mysql

Containers are automatically started via testcontainers when needed.
Just run: pytest tests/

To use external containers instead (e.g., docker-compose), set:
    DJ_USE_EXTERNAL_CONTAINERS=1
    DJ_HOST=localhost DJ_PORT=3306 S3_ENDPOINT=localhost:9000 pytest

To run only unit tests (no Docker required):
    pytest -m "not requires_mysql"
"""

import logging
import os
from os import remove
from typing import Dict, List

import certifi
import pytest
import urllib3

import datajoint as dj
from datajoint.errors import DataJointError

from . import schema, schema_advanced, schema_external, schema_object, schema_simple
from . import schema_uuid as schema_uuid_module
from . import schema_type_aliases as schema_type_aliases_module

logger = logging.getLogger(__name__)


# =============================================================================
# Pytest Hooks
# =============================================================================


def pytest_collection_modifyitems(config, items):
    """Auto-mark integration tests based on their fixtures."""
    # Tests that use these fixtures require MySQL
    mysql_fixtures = {
        "connection_root",
        "connection_root_bare",
        "connection_test",
        "schema_any",
        "schema_any_fresh",
        "schema_simp",
        "schema_adv",
        "schema_ext",
        "schema_uuid",
        "schema_type_aliases",
        "schema_obj",
        "db_creds_root",
        "db_creds_test",
    }
    # Tests that use these fixtures require MinIO
    minio_fixtures = {
        "minio_client",
        "s3fs_client",
        "s3_creds",
        "stores_config",
        "mock_stores",
    }

    for item in items:
        # Get all fixtures this test uses (directly or indirectly)
        try:
            fixturenames = set(item.fixturenames)
        except AttributeError:
            continue

        # Auto-add marks based on fixture usage
        if fixturenames & mysql_fixtures:
            item.add_marker(pytest.mark.requires_mysql)
        if fixturenames & minio_fixtures:
            item.add_marker(pytest.mark.requires_minio)


# =============================================================================
# Container Fixtures - Auto-start MySQL and MinIO via testcontainers
# =============================================================================

# Check if we should use external containers (for CI or manual docker-compose)
USE_EXTERNAL_CONTAINERS = os.environ.get("DJ_USE_EXTERNAL_CONTAINERS", "").lower() in ("1", "true", "yes")


@pytest.fixture(scope="session")
def mysql_container():
    """Start MySQL container for the test session (or use external)."""
    if USE_EXTERNAL_CONTAINERS:
        # Use external container - return None, credentials come from env
        logger.info("Using external MySQL container")
        yield None
        return

    from testcontainers.mysql import MySqlContainer

    container = MySqlContainer(
        image="mysql:8.0",
        username="root",
        password="password",
        dbname="test",
    )
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(3306)
    logger.info(f"MySQL container started at {host}:{port}")

    yield container

    container.stop()
    logger.info("MySQL container stopped")


@pytest.fixture(scope="session")
def minio_container():
    """Start MinIO container for the test session (or use external)."""
    if USE_EXTERNAL_CONTAINERS:
        # Use external container - return None, credentials come from env
        logger.info("Using external MinIO container")
        yield None
        return

    from testcontainers.minio import MinioContainer

    container = MinioContainer(
        image="minio/minio:latest",
        access_key="datajoint",
        secret_key="datajoint",
    )
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(9000)
    logger.info(f"MinIO container started at {host}:{port}")

    yield container

    container.stop()
    logger.info("MinIO container stopped")


# =============================================================================
# Credential Fixtures - Derived from containers or environment
# =============================================================================


@pytest.fixture(scope="session")
def prefix():
    return os.environ.get("DJ_TEST_DB_PREFIX", "djtest")


@pytest.fixture(scope="session")
def db_creds_root(mysql_container) -> Dict:
    """Root database credentials from container or environment."""
    if mysql_container is not None:
        # From testcontainer
        host = mysql_container.get_container_host_ip()
        port = mysql_container.get_exposed_port(3306)
        return dict(
            host=f"{host}:{port}",
            user="root",
            password="password",
        )
    else:
        # From environment (external container)
        host = os.environ.get("DJ_HOST", "localhost")
        port = os.environ.get("DJ_PORT", "3306")
        return dict(
            host=f"{host}:{port}" if port else host,
            user=os.environ.get("DJ_USER", "root"),
            password=os.environ.get("DJ_PASS", "password"),
        )


@pytest.fixture(scope="session")
def db_creds_test(mysql_container) -> Dict:
    """Test user database credentials from container or environment."""
    if mysql_container is not None:
        # From testcontainer
        host = mysql_container.get_container_host_ip()
        port = mysql_container.get_exposed_port(3306)
        return dict(
            host=f"{host}:{port}",
            user="datajoint",
            password="datajoint",
        )
    else:
        # From environment (external container)
        host = os.environ.get("DJ_HOST", "localhost")
        port = os.environ.get("DJ_PORT", "3306")
        return dict(
            host=f"{host}:{port}" if port else host,
            user=os.environ.get("DJ_TEST_USER", "datajoint"),
            password=os.environ.get("DJ_TEST_PASSWORD", "datajoint"),
        )


@pytest.fixture(scope="session")
def s3_creds(minio_container) -> Dict:
    """S3/MinIO credentials from container or environment."""
    if minio_container is not None:
        # From testcontainer
        host = minio_container.get_container_host_ip()
        port = minio_container.get_exposed_port(9000)
        return dict(
            endpoint=f"{host}:{port}",
            access_key="datajoint",
            secret_key="datajoint",
            bucket="datajoint.test",
        )
    else:
        # From environment (external container)
        return dict(
            endpoint=os.environ.get("S3_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("S3_ACCESS_KEY", "datajoint"),
            secret_key=os.environ.get("S3_SECRET_KEY", "datajoint"),
            bucket=os.environ.get("S3_BUCKET", "datajoint.test"),
        )


# =============================================================================
# DataJoint Configuration
# =============================================================================


@pytest.fixture(scope="session")
def configure_datajoint(db_creds_root):
    """Configure DataJoint to use test database.

    This fixture is NOT autouse - it only runs when a test requests
    a fixture that depends on it (e.g., connection_root_bare).
    """
    # Parse host:port from credentials
    host_port = db_creds_root["host"]
    if ":" in host_port:
        host, port = host_port.rsplit(":", 1)
    else:
        host, port = host_port, "3306"

    dj.config["database.host"] = host
    dj.config["database.port"] = int(port)
    dj.config["safemode"] = False

    logger.info(f"Configured DataJoint to use MySQL at {host}:{port}")


# =============================================================================
# Connection Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def connection_root_bare(db_creds_root, configure_datajoint):
    """Bare root connection without user setup."""
    connection = dj.Connection(**db_creds_root)
    yield connection


@pytest.fixture(scope="session")
def connection_root(connection_root_bare, prefix):
    """Root database connection with test users created."""
    conn_root = connection_root_bare

    # Create MySQL users (MySQL 8.0+ syntax - we only support 8.0+)
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

    # MySQL 8.0+ syntax
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

    connection = dj.Connection(**db_creds_test)
    yield connection
    connection_root.query(f"""DROP USER `{db_creds_test["user"]}`""")
    connection.close()


# =============================================================================
# S3/MinIO Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def stores_config(s3_creds, tmpdir_factory):
    """Configure object storage stores for tests."""
    return {
        "raw": dict(protocol="file", location=str(tmpdir_factory.mktemp("raw"))),
        "repo": dict(
            stage=str(tmpdir_factory.mktemp("repo")),
            protocol="file",
            location=str(tmpdir_factory.mktemp("repo")),
        ),
        "repo-s3": dict(
            protocol="s3",
            endpoint=s3_creds["endpoint"],
            access_key=s3_creds["access_key"],
            secret_key=s3_creds["secret_key"],
            bucket=s3_creds.get("bucket", "datajoint-test"),
            location="dj/repo",
            stage=str(tmpdir_factory.mktemp("repo-s3")),
            secure=False,  # MinIO runs without SSL in tests
        ),
        "local": dict(protocol="file", location=str(tmpdir_factory.mktemp("local"))),
        "share": dict(
            protocol="s3",
            endpoint=s3_creds["endpoint"],
            access_key=s3_creds["access_key"],
            secret_key=s3_creds["secret_key"],
            bucket=s3_creds.get("bucket", "datajoint-test"),
            location="dj/store/repo",
            secure=False,  # MinIO runs without SSL in tests
        ),
    }


@pytest.fixture
def mock_stores(stores_config):
    """Configure stores for tests using unified stores system."""
    # Save original configuration
    og_stores = dict(dj.config.stores)

    # Set test configuration
    dj.config.stores.clear()
    for name, config in stores_config.items():
        dj.config.stores[name] = config

    yield

    # Restore original configuration
    dj.config.stores.clear()
    dj.config.stores.update(og_stores)


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
def s3fs_client(s3_creds):
    """Initialize s3fs filesystem for MinIO."""
    import s3fs

    return s3fs.S3FileSystem(
        endpoint_url=f"http://{s3_creds['endpoint']}",
        key=s3_creds["access_key"],
        secret=s3_creds["secret_key"],
    )


@pytest.fixture(scope="session")
def minio_client(s3_creds, s3fs_client, teardown=False):
    """S3 filesystem with test bucket created (legacy name for compatibility)."""
    bucket = s3_creds["bucket"]

    # Create bucket if it doesn't exist
    try:
        s3fs_client.mkdir(bucket)
    except Exception:
        # Bucket may already exist
        pass

    yield s3fs_client

    if not teardown:
        return
    # Clean up objects and bucket
    try:
        files = s3fs_client.ls(bucket, detail=False)
        for f in files:
            s3fs_client.rm(f)
        s3fs_client.rmdir(bucket)
    except Exception:
        pass


# =============================================================================
# Cleanup Fixtures
# =============================================================================


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
    # schema.jobs returns a list of Job objects for existing job tables
    for job in schema_any.jobs:
        try:
            job.delete()
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


# =============================================================================
# Schema Fixtures
# =============================================================================


@pytest.fixture(scope="module")
def schema_any(connection_test, prefix):
    schema_any = dj.Schema(prefix + "_test1", schema.LOCALS_ANY, connection=connection_test)
    assert schema.LOCALS_ANY, "LOCALS_ANY is empty"
    # Clean up any existing job tables (schema.jobs returns a list)
    for job in schema_any.jobs:
        try:
            job.delete()
        except DataJointError:
            pass
    # Allow native PK fields for legacy test tables (Experiment, Trial)
    original_value = dj.config.jobs.allow_new_pk_fields_in_computed_tables
    dj.config.jobs.allow_new_pk_fields_in_computed_tables = True
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
    # Restore original config value after all tables are declared
    dj.config.jobs.allow_new_pk_fields_in_computed_tables = original_value
    yield schema_any
    # Clean up job tables before dropping schema (if schema still exists)
    if schema_any.exists:
        for job in schema_any.jobs:
            try:
                job.delete()
            except DataJointError:
                pass
    schema_any.drop()


@pytest.fixture
def schema_any_fresh(connection_test, prefix):
    """Function-scoped schema_any for tests that need fresh schema state."""
    schema_any = dj.Schema(prefix + "_test1_fresh", schema.LOCALS_ANY, connection=connection_test)
    assert schema.LOCALS_ANY, "LOCALS_ANY is empty"
    # Clean up any existing job tables
    for job in schema_any.jobs:
        try:
            job.delete()
        except DataJointError:
            pass
    # Allow native PK fields for legacy test tables (Experiment, Trial)
    original_value = dj.config.jobs.allow_new_pk_fields_in_computed_tables
    dj.config.jobs.allow_new_pk_fields_in_computed_tables = True
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
    # Restore original config value after all tables are declared
    dj.config.jobs.allow_new_pk_fields_in_computed_tables = original_value
    yield schema_any
    # Clean up job tables before dropping schema (if schema still exists)
    if schema_any.exists:
        for job in schema_any.jobs:
            try:
                job.delete()
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
def schema_ext(connection_test, mock_stores, mock_cache, prefix):
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


# =============================================================================
# Table Fixtures
# =============================================================================


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


# =============================================================================
# Object Storage Fixtures
# =============================================================================


@pytest.fixture
def object_storage_config(tmpdir_factory):
    """Create object storage configuration for testing."""
    base_location = str(tmpdir_factory.mktemp("object_storage"))
    # Location now includes project context
    location = f"{base_location}/test_project"
    # Create the directory (StorageBackend validates it exists)
    from pathlib import Path
    Path(location).mkdir(parents=True, exist_ok=True)
    return {
        "protocol": "file",
        "location": location,
        "token_length": 8,
    }


@pytest.fixture
def mock_object_storage(object_storage_config):
    """Mock object storage configuration in datajoint config using unified stores."""
    # Save original values
    original_stores = dict(dj.config.stores)

    # Configure default store for tests
    dj.config.stores["default"] = "local"
    dj.config.stores["local"] = {
        "protocol": object_storage_config["protocol"],
        "location": object_storage_config["location"],
        "token_length": object_storage_config.get("token_length", 8),
    }

    yield object_storage_config

    # Restore original values
    dj.config.stores.clear()
    dj.config.stores.update(original_stores)


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
