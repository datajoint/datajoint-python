import os
from datajoint.axon import Session, get_s3_client
import json
import pytest
import boto3
from moto import mock_aws
import dotenv
dotenv.load_dotenv(dotenv.find_dotenv())


@pytest.fixture
def moto_account_id():
    """Default account ID for moto"""
    return "123456789012"


@pytest.fixture
def keycloak_client_secret():
    secret = os.getenv("OAUTH_CLIENT_SECRET")
    if not secret:
        pytest.skip("No client secret found")
    else:
        return secret


@pytest.fixture
def keycloak_client_id():
    return os.getenv("OAUTH_CLIENT_ID", "works")


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="function")
def iam_client(aws_credentials):
    """
    Return a mocked S3 client
    """
    with mock_aws():
        yield boto3.client("iam", region_name="us-east-1")


@pytest.fixture
def s3_policy(iam_client):
    """Create a policy with S3 read access using boto3."""
    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "arn:aws:s3:::mybucket/*",
            }
        ],
    }
    return iam_client.create_policy(
        PolicyName="test-policy",
        Path="/",
        PolicyDocument=json.dumps(policy_doc),
        Description="Test policy",
    )

@pytest.fixture
def s3_role(moto_account_id, s3_policy):
    """Create a mock role and policy document for testing"""
    return "123456789012"


@mock_aws
@pytest.mark.skip
class TestSession:
    def test_can_init(self, s3_role, keycloak_client_id, keycloak_client_secret, moto_account_id):
        session = Session(
            aws_account_id=moto_account_id,
            s3_role=s3_role,
            auth_client_id=keycloak_client_id,
            auth_client_secret=keycloak_client_secret,
        )
        assert session.bearer_token, "Bearer token not set"

def test_get_s3_client(s3_role, keycloak_client_id, keycloak_client_secret, moto_account_id):
    client = get_s3_client(
        auth_client_id=keycloak_client_id,
        auth_client_secret=keycloak_client_secret,
        aws_account_id=moto_account_id,
        s3_role=s3_role,
        bearer_token=None,
    )
    assert client

