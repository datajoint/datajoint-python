from datajoint.axon import Session
import pytest
import boto3
from moto import mock_aws


@pytest.fixture
def moto_account_id():
    """Default account ID for moto"""
    return "123456789012"

@mock_aws
class TestSession:
    def test_can_init(self):
        session = Session(
            aws_account_id=moto_account_id,
            s3_role="test-role",
            auth_client_id="test-client-id",
            auth_client_secret="test-client-secret",
        )
        assert session.bearer_token, "Bearer token not set"
