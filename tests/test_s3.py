from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from . import CONN_INFO
# from .schema import *
# import datajoint as dj

from minio import Minio
import urllib3
import certifi

class TestS3:

    @staticmethod
    def test_connection():

        # Initialize httpClient with relevant timeout.
        httpClient = urllib3.PoolManager(
                timeout=30,
                        cert_reqs='CERT_REQUIRED',
                        ca_certs=certifi.where(),
                        retries=urllib3.Retry(
                            total=3,
                            backoff_factor=0.2,
                            status_forcelist=[500, 502, 503, 504]
                        )
            )

        # Initialize minioClient with an endpoint and access/secret keys.
        minioClient = Minio(CONN_INFO['host'] + ':9000',
                            access_key=CONN_INFO['user'],
                            secret_key=CONN_INFO['password'],
                            secure=False,
                            http_client=httpClient)

        buckets = minioClient.list_buckets()
        for bucket in buckets:
            print(bucket.name, bucket.creation_date)
