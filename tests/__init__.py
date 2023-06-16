import datajoint as dj
from packaging import version
import pytest, os, urllib3, certifi, minio

PREFIX = "djtest"

CONN_INFO_ROOT = dict(
    host=(os.getenv("DJ_HOST")),
    user=(os.getenv("DJ_USER")),
    password=(os.getenv("DJ_PASS")),
)

S3_CONN_INFO = dict(
    endpoint=(os.getenv("DJ_HOST")),
    access_key="datajoint",
    secret_key="datajoint",
    bucket="datajoint.test",
)


@pytest.fixture
def connection_root():
    """Root user database connection."""
    dj.config["safemode"] = False
    connection = dj.Connection(
        host=(os.getenv("DJ_HOST")),
        user=(os.getenv("DJ_USER")),
        password=(os.getenv("DJ_PASS")),
    )
    yield connection
    dj.config["safemode"] = True
    connection.close()


@pytest.fixture
def connection_test(connection_root):
    """Test user database connection."""
    target = f"`{PREFIX}%%`.*"
    credentials = dict(
        host=(os.getenv("DJ_HOST")), user="datajoint", password="datajoint"
    )
    permission = "ALL PRIVILEGES"
    # Create MySQL users
    if version.parse(
        connection_root.query("select @@version;").fetchone()[0]
    ) >= version.parse("8.0.0"):
        # create user if necessary on mysql8
        connection_root.query(
            f"""
            CREATE USER IF NOT EXISTS '{credentials['user']}'@'%%'
            IDENTIFIED BY '{credentials['password']}';
            """
        )
        connection_root.query(
            f"""
            GRANT {permission} ON {target}
            TO '{credentials['user']}'@'%%';
            """
        )
    else:
        # grant permissions. For MySQL 5.7 this also automatically creates user
        # if not exists
        connection_root.query(
            f"""
            GRANT {permission} ON {target}
            TO '{credentials['user']}'@'%%'
            IDENTIFIED BY '{credentials['password']}';
            """
        )
    connection = (dj.Connection)(**credentials)
    yield connection
    connection_root.query(f"DROP USER `{credentials['user']}`")
    connection.close()


@pytest.fixture
def bucket():
    httpClient = urllib3.PoolManager(
        timeout=30,
        cert_reqs="CERT_REQUIRED",
        ca_certs=(certifi.where()),
        retries=urllib3.Retry(
            total=3, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504]
        ),
    )
    minioClient = minio.Minio(
        **{k: v for k, v in S3_CONN_INFO.items() if k != "bucket"},
        **{"secure": True, "http_client": httpClient},
    )
    minioClient.make_bucket((S3_CONN_INFO["bucket"]), location="us-east-1")
    yield
    minioClient.remove_bucket(S3_CONN_INFO["bucket"])
