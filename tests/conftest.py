import atexit
import json
import logging
import os
import shutil
import signal
import time
from os import environ, remove
from pathlib import Path
from typing import Dict, List

import certifi
import docker
import minio
import networkx as nx
import pytest
import requests
import urllib3
from packaging import version

import datajoint as dj
from datajoint import errors
from datajoint.errors import (
    ADAPTED_TYPE_SWITCH,
    FILEPATH_FEATURE_SWITCH,
    DataJointError,
)

from . import schema, schema_adapted, schema_advanced, schema_external, schema_simple
from . import schema_uuid as schema_uuid_module

# Configure logging for container management
logger = logging.getLogger(__name__)


def pytest_sessionstart(session):
    """Called after the Session object has been created and configured."""
    # This runs very early, before most fixtures, but we don't have container info yet
    pass


def pytest_configure(config):
    """Called after command line options have been parsed."""
    # This runs before pytest_sessionstart but still too early for containers
    pass


# Global container registry for cleanup
_active_containers = set()
_docker_client = None


def _get_docker_client():
    """Get or create docker client"""
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def _cleanup_containers():
    """Clean up any remaining containers"""
    if _active_containers:
        logger.info(
            f"Emergency cleanup: {len(_active_containers)} containers to clean up"
        )
        try:
            client = _get_docker_client()
            for container_id in list(_active_containers):
                try:
                    container = client.containers.get(container_id)
                    container.remove(force=True)
                    logger.info(
                        f"Emergency cleanup: removed container {container_id[:12]}"
                    )
                except docker.errors.NotFound:
                    logger.debug(f"Container {container_id[:12]} already removed")
                except Exception as e:
                    logger.error(
                        f"Error cleaning up container {container_id[:12]}: {e}"
                    )
                finally:
                    _active_containers.discard(container_id)
        except Exception as e:
            logger.error(f"Error during emergency cleanup: {e}")
    else:
        logger.debug("No containers to clean up")


def _register_container(container):
    """Register a container for cleanup"""
    _active_containers.add(container.id)
    logger.debug(f"Registered container {container.id[:12]} for cleanup")


def _unregister_container(container):
    """Unregister a container from cleanup"""
    _active_containers.discard(container.id)
    logger.debug(f"Unregistered container {container.id[:12]} from cleanup")


# Register cleanup functions
atexit.register(_cleanup_containers)


def _signal_handler(signum, frame):
    """Handle signals to ensure container cleanup"""
    logger.warning(
        f"Received signal {signum}, performing emergency container cleanup..."
    )
    _cleanup_containers()

    # Restore default signal handler and re-raise the signal
    # This allows pytest to handle the cancellation normally
    signal.signal(signum, signal.SIG_DFL)
    os.kill(os.getpid(), signum)


# Register signal handlers for graceful cleanup, but only for non-interactive scenarios
# In pytest, we'll rely on fixture teardown and atexit handlers primarily
try:
    import pytest

    # If we're here, pytest is available, so only register SIGTERM (for CI/batch scenarios)
    signal.signal(signal.SIGTERM, _signal_handler)
    # Don't intercept SIGINT (Ctrl+C) to allow pytest's normal cancellation behavior
except ImportError:
    # If pytest isn't available, register both handlers
    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)


@pytest.fixture(scope="session")
def docker_client():
    """Docker client for managing containers."""
    return _get_docker_client()


@pytest.fixture(scope="session")
def mysql_container(docker_client):
    """Start MySQL container and wait for it to be healthy."""
    mysql_ver = os.environ.get("MYSQL_VER", "8.0")
    container_name = f"datajoint_test_mysql_{os.getpid()}"

    logger.info(f"Starting MySQL container {container_name} with version {mysql_ver}")

    # Remove existing container if it exists
    try:
        existing = docker_client.containers.get(container_name)
        logger.info(f"Removing existing MySQL container {container_name}")
        existing.remove(force=True)
    except docker.errors.NotFound:
        logger.debug(f"No existing MySQL container {container_name} found")

    # Start MySQL container
    container = docker_client.containers.run(
        f"datajoint/mysql:{mysql_ver}",
        name=container_name,
        environment={"MYSQL_ROOT_PASSWORD": "password"},
        command="mysqld --default-authentication-plugin=mysql_native_password",
        ports={"3306/tcp": None},  # Let Docker assign random port
        detach=True,
        remove=True,
        healthcheck={
            "test": ["CMD", "mysqladmin", "ping", "-h", "localhost"],
            "timeout": 30000000000,  # 30s in nanoseconds
            "retries": 5,
            "interval": 15000000000,  # 15s in nanoseconds
        },
    )

    # Register container for cleanup
    _register_container(container)
    logger.info(f"MySQL container {container_name} started with ID {container.id[:12]}")

    # Wait for health check
    max_wait = 120  # 2 minutes
    start_time = time.time()
    logger.info(
        f"Waiting for MySQL container {container_name} to become healthy (max {max_wait}s)"
    )

    while time.time() - start_time < max_wait:
        container.reload()
        health_status = container.attrs["State"]["Health"]["Status"]
        logger.debug(f"MySQL container {container_name} health status: {health_status}")
        if health_status == "healthy":
            break
        time.sleep(2)
    else:
        logger.error(
            f"MySQL container {container_name} failed to become healthy within {max_wait}s"
        )
        container.remove(force=True)
        raise RuntimeError("MySQL container failed to become healthy")

    # Get the mapped port
    port_info = container.attrs["NetworkSettings"]["Ports"]["3306/tcp"]
    if port_info:
        host_port = port_info[0]["HostPort"]
        logger.info(
            f"MySQL container {container_name} is healthy and accessible on localhost:{host_port}"
        )
    else:
        raise RuntimeError("Failed to get MySQL port mapping")

    yield container, "localhost", int(host_port)

    # Cleanup
    logger.info(f"Cleaning up MySQL container {container_name}")
    _unregister_container(container)
    container.remove(force=True)
    logger.info(f"MySQL container {container_name} removed")


@pytest.fixture(scope="session")
def minio_container(docker_client):
    """Start MinIO container and wait for it to be healthy."""
    minio_ver = os.environ.get("MINIO_VER", "RELEASE.2025-02-28T09-55-16Z")
    container_name = f"datajoint_test_minio_{os.getpid()}"

    logger.info(f"Starting MinIO container {container_name} with version {minio_ver}")

    # Remove existing container if it exists
    try:
        existing = docker_client.containers.get(container_name)
        logger.info(f"Removing existing MinIO container {container_name}")
        existing.remove(force=True)
    except docker.errors.NotFound:
        logger.debug(f"No existing MinIO container {container_name} found")

    # Start MinIO container
    container = docker_client.containers.run(
        f"minio/minio:{minio_ver}",
        name=container_name,
        environment={"MINIO_ACCESS_KEY": "datajoint", "MINIO_SECRET_KEY": "datajoint"},
        command=["server", "--address", ":9000", "/data"],
        ports={"9000/tcp": None},  # Let Docker assign random port
        detach=True,
        remove=True,
    )

    # Register container for cleanup
    _register_container(container)
    logger.info(f"MinIO container {container_name} started with ID {container.id[:12]}")

    # Get the mapped port
    container.reload()
    port_info = container.attrs["NetworkSettings"]["Ports"]["9000/tcp"]
    if port_info:
        host_port = port_info[0]["HostPort"]
        logger.info(f"MinIO container {container_name} mapped to localhost:{host_port}")
    else:
        raise RuntimeError("Failed to get MinIO port mapping")

    # Wait for MinIO to be ready
    minio_url = f"http://localhost:{host_port}"
    max_wait = 60
    start_time = time.time()
    logger.info(
        f"Waiting for MinIO container {container_name} to become ready (max {max_wait}s)"
    )

    while time.time() - start_time < max_wait:
        try:
            response = requests.get(f"{minio_url}/minio/health/live", timeout=5)
            if response.status_code == 200:
                logger.info(
                    f"MinIO container {container_name} is ready and accessible at {minio_url}"
                )
                break
        except requests.exceptions.RequestException:
            logger.debug(f"MinIO container {container_name} not ready yet, retrying...")
            pass
        time.sleep(2)
    else:
        logger.error(
            f"MinIO container {container_name} failed to become ready within {max_wait}s"
        )
        container.remove(force=True)
        raise RuntimeError("MinIO container failed to become ready")

    yield container, "localhost", int(host_port)

    # Cleanup
    logger.info(f"Cleaning up MinIO container {container_name}")
    _unregister_container(container)
    container.remove(force=True)
    logger.info(f"MinIO container {container_name} removed")


@pytest.fixture(scope="session")
def prefix():
    return os.environ.get("DJ_TEST_DB_PREFIX", "djtest")


@pytest.fixture(scope="session")
def monkeysession():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture(scope="module")
def monkeymodule():
    with pytest.MonkeyPatch.context() as mp:
        yield mp


@pytest.fixture
def enable_adapted_types(monkeypatch):
    monkeypatch.setenv(ADAPTED_TYPE_SWITCH, "TRUE")
    yield
    monkeypatch.delenv(ADAPTED_TYPE_SWITCH, raising=True)


@pytest.fixture
def enable_filepath_feature(monkeypatch):
    monkeypatch.setenv(FILEPATH_FEATURE_SWITCH, "TRUE")
    yield
    monkeypatch.delenv(FILEPATH_FEATURE_SWITCH, raising=True)


@pytest.fixture(scope="session")
def db_creds_test(mysql_container) -> Dict:
    _, host, port = mysql_container
    # Set environment variables for DataJoint at module level
    os.environ["DJ_TEST_HOST"] = host
    os.environ["DJ_TEST_PORT"] = str(port)

    # Also update DataJoint's test configuration directly
    dj.config["database.test.host"] = host
    dj.config["database.test.port"] = port

    return dict(
        host=f"{host}:{port}",
        user=os.getenv("DJ_TEST_USER", "datajoint"),
        password=os.getenv("DJ_TEST_PASSWORD", "datajoint"),
    )


@pytest.fixture(scope="session", autouse=True)
def configure_datajoint_for_containers(mysql_container):
    """Configure DataJoint to use pytest-managed containers. Runs automatically for all tests."""
    _, host, port = mysql_container

    # Set environment variables FIRST - these will be inherited by subprocesses
    logger.info(f"🔧 Setting environment: DJ_HOST={host}, DJ_PORT={port}")
    os.environ["DJ_HOST"] = host
    os.environ["DJ_PORT"] = str(port)

    # Verify the environment variables were set
    logger.info(
        f"🔧 Environment after setting: DJ_HOST={os.environ.get('DJ_HOST')}, DJ_PORT={os.environ.get('DJ_PORT')}"
    )

    # Also update DataJoint's configuration directly for in-process connections
    dj.config["database.host"] = host
    dj.config["database.port"] = port

    logger.info(f"🔧 Configured DataJoint to use MySQL container at {host}:{port}")
    return host, port  # Return values so other fixtures can use them


@pytest.fixture(scope="session")
def db_creds_root(mysql_container) -> Dict:
    _, host, port = mysql_container
    return dict(
        host=f"{host}:{port}",
        user=os.getenv("DJ_USER", "root"),
        password=os.getenv("DJ_PASS", "password"),
    )


@pytest.fixture(scope="session")
def connection_root_bare(db_creds_root):
    connection = dj.Connection(**db_creds_root)
    yield connection


@pytest.fixture(scope="session")
def connection_root(connection_root_bare, prefix):
    """Root user database connection."""
    dj.config["safemode"] = False
    conn_root = connection_root_bare
    # Create MySQL users
    if version.parse(
        conn_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
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
    cur = conn_root.query('SHOW DATABASES LIKE "{}\\_%%"'.format(prefix))
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
def connection_test(connection_root, prefix, db_creds_test):
    """Test user database connection."""
    database = f"{prefix}%%"
    permission = "ALL PRIVILEGES"

    # Create MySQL users
    if version.parse(
        connection_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
        # create user if necessary on mysql8
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
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
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


@pytest.fixture(scope="session")
def s3_creds(minio_container) -> Dict:
    _, host, port = minio_container
    # Set environment variable for S3 endpoint at module level
    os.environ["S3_ENDPOINT"] = f"{host}:{port}"
    return dict(
        endpoint=f"{host}:{port}",
        access_key=os.environ.get("S3_ACCESS_KEY", "datajoint"),
        secret_key=os.environ.get("S3_SECRET_KEY", "datajoint"),
        bucket=os.environ.get("S3_BUCKET", "datajoint.test"),
    )


@pytest.fixture(scope="session")
def stores_config(s3_creds, tmpdir_factory):
    stores_config = {
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
        "local": dict(
            protocol="file", location=tmpdir_factory.mktemp("local"), subfolding=(1, 1)
        ),
        "share": dict(
            s3_creds, protocol="s3", location="dj/store/repo", subfolding=(2, 4)
        ),
    }
    return stores_config


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


@pytest.fixture
def schema_any(connection_test, prefix):
    schema_any = dj.Schema(
        prefix + "_test1", schema.LOCALS_ANY, connection=connection_test
    )
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

    # clear previous contents if any.
    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))
    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    yield a, b, c, d, e


@pytest.fixture
def schema_simp(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_relational", schema_simple.LOCALS_SIMPLE, connection=connection_test
    )
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


@pytest.fixture
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
def schema_ext(
    connection_test, enable_filepath_feature, mock_stores, mock_cache, prefix
):
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


@pytest.fixture
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


@pytest.fixture(scope="session")
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


@pytest.fixture(scope="session")
def minio_client_bare(s3_creds):
    """Initialize MinIO with an endpoint and access/secret keys."""
    client = minio.Minio(
        endpoint=s3_creds["endpoint"],
        access_key=s3_creds["access_key"],
        secret_key=s3_creds["secret_key"],
        secure=False,
    )
    return client


@pytest.fixture(scope="session")
def minio_client(s3_creds, minio_client_bare, teardown=False):
    """Initialize a MinIO client and create buckets for testing session."""
    # Setup MinIO bucket
    aws_region = "us-east-1"
    try:
        minio_client_bare.make_bucket(s3_creds["bucket"], location=aws_region)
    except minio.error.S3Error as e:
        if e.code != "BucketAlreadyOwnedByYou":
            raise e

    yield minio_client_bare
    if not teardown:
        return

    # Teardown S3
    objs = list(minio_client_bare.list_objects(s3_creds["bucket"], recursive=True))
    objs = [
        minio_client_bare.remove_object(
            s3_creds["bucket"], o.object_name.encode("utf-8")
        )
        for o in objs
    ]
    minio_client_bare.remove_bucket(s3_creds["bucket"])


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
