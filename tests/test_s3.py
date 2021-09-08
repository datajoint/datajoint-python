from . import S3_CONN_INFO
from minio import Minio
import minio
import json
import urllib3
import certifi
from nose.tools import assert_true, raises
from .schema_external import schema, SimpleRemote
from datajoint.errors import DataJointError

class TestS3:

    @staticmethod
    def test_connection():

        # Initialize httpClient with relevant timeout.
        http_client = urllib3.PoolManager(
                                          timeout=30, cert_reqs='CERT_REQUIRED',
                                          ca_certs=certifi.where(),
                                          retries=urllib3.Retry(total=3, backoff_factor=0.2,
                                                                status_forcelist=[
                                                                                  500, 502,
                                                                                  503, 504]))

        # Initialize minioClient with an endpoint and access/secret keys.
        minio_client = Minio(
            S3_CONN_INFO['endpoint'],
            access_key=S3_CONN_INFO['access_key'],
            secret_key=S3_CONN_INFO['secret_key'],
            secure=False,
            http_client=http_client)

        assert_true(minio_client.bucket_exists(S3_CONN_INFO['bucket']))
        
    @staticmethod
    def test_connection_secure():

        # Initialize httpClient with relevant timeout.
        http_client = urllib3.PoolManager(
                                          timeout=30, cert_reqs='CERT_REQUIRED',
                                          ca_certs=certifi.where(),
                                          retries=urllib3.Retry(total=3, backoff_factor=0.2,
                                                                status_forcelist=[
                                                                                  500, 502,
                                                                                  503, 504]))

        # Initialize minioClient with an endpoint and access/secret keys.
        minio_client = Minio(
            S3_CONN_INFO['endpoint'],
            access_key=S3_CONN_INFO['access_key'],
            secret_key=S3_CONN_INFO['secret_key'],
            secure=True,
            http_client=http_client)

        assert_true(minio_client.bucket_exists(S3_CONN_INFO['bucket']))
    @staticmethod
    @raises(DataJointError)
    def test_remove_object_exception():
        # https://github.com/datajoint/datajoint-python/issues/952
        
        # Initialize minioClient with an endpoint and access/secret keys.
        minio_client = Minio(
            'minio:9000',
            access_key= 'jeffjeff',
            secret_key= 'jeffjeff',
            secure=False)

        minio_admin = minio.MinioAdmin(target = 'myminio')
        
        # Create new user
        minio_admin.user_add(access_key = 'jeffjeff', secret_key = 'jeffjeff')

        # json for test policy for permissionless user
        testpolicy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "s3:GetBucketLocation",
                "s3:ListBucket",
                "s3:ListBucketMultipartUploads",
                "s3:ListAllMyBuckets"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::datajoint.test",
                "arn:aws:s3:::datajoint.migrate"
            ],
            "Sid": ""
        },
        {
            "Action": [
                "s3:GetObject",
                "s3:ListMultipartUploadParts"
            ],
            "Effect": "Allow",
            "Resource": [
                "arn:aws:s3:::datajoint.test/*",
                "arn:aws:s3:::datajoint.migrate/*"
            ],
            "Sid": ""
        }
    ]
}

        # Write test json to tmp directory so we can use it to create a new user policy
        with open('/tmp/policy.json', 'w') as f:
            f.write(json.dumps(testpolicy))
        
        # Add the policy and apply it to the user
        minio_admin.policy_add(policy_name = 'test', policy_file = '/tmp/policy.json')
        minio_admin.policy_set(policy_name = 'test', user = 'jeffjeff')

        # Insert some test data and remove it so that the external table is populated
        test = [1,[1,2,3]]
        SimpleRemote.insert1(test)
        SimpleRemote.delete()

        # Save the old external table minio client
        old_client = schema.external['share'].s3.client

        # Apply our new minio client to the external table that has permissions restrictions
        schema.external['share'].s3.client = minio_client
        
        # This method returns a list of errors
        error_list = schema.external['share'].delete(delete_external_files = True, errors_as_string = False)

        # Teardown
        minio_admin.user_remove(access_key = 'jeffjeff')
        minio_admin.policy_remove('test')
        schema.external['share'].s3.client = old_client

        # Raise the error we want
        raise error_list[0][2]