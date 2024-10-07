from datajoint.axon import Session
import pytest
import moto3


class TestSession:
    def test_can_init(self):
        session = Session(
            aws_account_id="123456789012",
            s3_role="test-role",
            auth_client_id="test-client-id",
            auth_client_secret="test-client-secret",
        )
        assert session.bearer_token, "Bearer token not set"
