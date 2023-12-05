import pytest
import urllib3
import certifi
from nose.tools import assert_true, raises
from .schema_external import SimpleRemote
from datajoint.errors import DataJointError
from datajoint.hash import uuid_from_buffer
from datajoint.blob import pack
from . import S3_CONN_INFO
from minio import Minio


@pytest.fixture(scope='session')
def stores_config():
    stores_config = {
        "raw": dict(protocol="file", location=tempfile.mkdtemp()),
        "repo": dict(
            stage=tempfile.mkdtemp(), protocol="file", location=tempfile.mkdtemp()
        ),
        "repo-s3": dict(
            S3_CONN_INFO, protocol="s3", location="dj/repo", stage=tempfile.mkdtemp()
        ),
        "local": dict(protocol="file", location=tempfile.mkdtemp(), subfolding=(1, 1)),
        "share": dict(
            S3_CONN_INFO, protocol="s3", location="dj/store/repo", subfolding=(2, 4)
        ),
    }
    return stores_config


@pytest.fixture
def schema_ext(connection_test, stores_config, enable_filepath_feature):
    schema = dj.Schema(PREFIX + "_extern", context=LOCALS_EXTERNAL, connection=connection_test)
    dj.config["stores"] = stores_config
    dj.config["cache"] = tempfile.mkdtemp()

    schema(Simple)
    schema(SimpleRemote)
    schema(Seed)
    schema(Dimension)
    schema(Image)
    schema(Attach)

    # dj.errors._switch_filepath_types(True)
    schema(Filepath)
    schema(FilepathS3)
    # dj.errors._switch_filepath_types(False)
    yield schema
    schema.drop()


class TestS3:
    def test_connection(self, http_client, minio_client):
        assert minio_client.bucket_exists(S3_CONN_INFO["bucket"])

    def test_connection_secure(self, minio_client):
        assert minio_client.bucket_exists(S3_CONN_INFO["bucket"])

    def test_remove_object_exception(self):
        # TODO: mv to failing block
        with pytest.raises(DataJointError):
            # https://github.com/datajoint/datajoint-python/issues/952

            # Insert some test data and remove it so that the external table is populated
            test = [1, [1, 2, 3]]
            SimpleRemote.insert1(test)
            SimpleRemote.delete()

            # Save the old external table minio client
            old_client = schema.external["share"].s3.client

            # Apply our new minio client which has a user that does not exist
            schema.external["share"].s3.client = Minio(
                S3_CONN_INFO["endpoint"],
                access_key="jeffjeff",
                secret_key="jeffjeff",
                secure=False,
            )

            # This method returns a list of errors
            error_list = schema.external["share"].delete(
                delete_external_files=True, errors_as_string=False
            )

            # Teardown
            schema.external["share"].s3.client = old_client
            schema.external["share"].delete(delete_external_files=True)

            # Raise the error we want if the error matches the expected uuid
            if str(error_list[0][0]) == str(uuid_from_buffer(pack(test[1]))):
                raise error_list[0][2]
