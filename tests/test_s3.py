import pytest
from .schema_external import SimpleRemote
from datajoint.errors import DataJointError
from datajoint.hash import uuid_from_buffer
from datajoint.blob import pack
from minio import Minio


def test_connection(http_client, minio_client, s3_creds):
    assert minio_client.bucket_exists(s3_creds["bucket"])


def test_connection_secure(minio_client, s3_creds):
    assert minio_client.bucket_exists(s3_creds["bucket"])


def test_remove_object_exception(schema_ext, s3_creds):
    # https://github.com/datajoint/datajoint-python/issues/952

    # Insert some test data and remove it so that the external table is populated
    test = [1, [1, 2, 3]]
    SimpleRemote.insert1(test)
    SimpleRemote.delete()

    # Save the old external table minio client
    old_client = schema_ext.external["share"].s3.client

    # Apply our new minio client which has a user that does not exist
    schema_ext.external["share"].s3.client = Minio(
        s3_creds["endpoint"],
        access_key="jeffjeff",
        secret_key="jeffjeff",
        secure=False,
    )

    # This method returns a list of errors
    error_list = schema_ext.external["share"].delete(
        delete_external_files=True, errors_as_string=False
    )

    # Teardown
    schema_ext.external["share"].s3.client = old_client
    schema_ext.external["share"].delete(delete_external_files=True)

    with pytest.raises(DataJointError):
        # Raise the error we want if the error matches the expected uuid
        if str(error_list[0][0]) == str(uuid_from_buffer(pack(test[1]))):
            raise error_list[0][2]
