from . import S3_CONN_INFO

from minio import Minio
import urllib3
import certifi
from nose.tools import assert_true


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
