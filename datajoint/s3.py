"""
This module contains logic related to S3 file storage
"""

import logging

import boto3
from botocore.exceptions import ClientError

from . import DataJointError

logger = logging.getLogger(__name__)


def bucket(**kwargs):
    ''' factory function '''
    return Bucket.get_bucket(**kwargs)


class Bucket:
    """
    A dj.Bucket object manages a connection to an AWS S3 Bucket.

    Currently, basic CRUD operations are supported; of note permissions and
    object versioning are not currently supported.

    To prevent session creation overhead, session establishment only occurs
    with the first remote operation, and bucket objects will be cached and
    reused when requested via the get_bucket() class method.
    """
    _bucket_cache = {}

    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None,
                 bucket=None):
        """
        Create a Bucket object.

        Note this bypasses the bucket session cache which should be used in
        most cases via `get_bucket()`

        :param aws_access_key_id: AWS Access Key ID
        :param aws_secret_access_key: AWS Secret Key
        :param bucket: name of remote bucket
        """
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self._s3 = None
        self._bucket = bucket

    @staticmethod
    def get_bucket(aws_access_key_id=None, aws_secret_access_key=None,
                   bucket=None, reset=False):
        """
        Returns Bucket object to be shared by multiple modules.

        If the connection is not yet established or reset=True, a new
        connection is set up.

        :param aws_access_key_id: AWS Access Key ID
        :param aws_secret_access_key: AWS Secret Key
        :param bucket: name of remote bucket
        :param reset: whether the connection should be reset or not
        """
        if bucket not in Bucket._bucket_cache or reset:
            b = Bucket(aws_access_key_id, aws_secret_access_key, bucket)
            Bucket._bucket_cache[bucket] = b
            return b
        else:
            return Bucket._bucket_cache[bucket]

    def connect(self):
        if self._s3 is None:
            self._s3 = self._session.resource('s3')

    def stat(self, rpath=None):
        """
        Check if a file exists in the bucket.

        :param rpath: remote path within bucket
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).load()
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise DataJointError(
                    'Error checking remote file {r} ({e})'.format(r=rpath, e=e)
                )
            return False

        return True

    def put(self, lpath=None, rpath=None):
        """
        Upload a file to the bucket.

        :param rpath: remote path within bucket
        :param lpath: local path
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).upload_file(lpath)
        except Exception as e:
            raise DataJointError(
                'Error uploading file {l} to {r} ({e})'.format(
                    l=lpath, r=rpath, e=e)
            )

        return True

    def get(self, rpath=None, lpath=None):
        """
        Retrieve a file from the bucket.

        :param rpath: remote path within bucket
        :param lpath: local path
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).download_file(lpath)
        except Exception as e:
            raise DataJointError(
                'Error downloading file {r} to {l} ({e})'.format(
                    r=rpath, l=lpath, e=e)
            )

        return True

    def delete(self, rpath):
        '''
        Delete a single remote object.
        Note: will return True even if object doesn't exist;
        for explicit verification combine with a .stat() call.

        :param rpath: remote path within bucket
        '''
        try:
            self.connect()
            r = self._s3.Object(self._bucket, rpath).delete()
            # XXX: if/when does 'False' occur? - s3 returns ok if no file...
            return r['ResponseMetadata']['HTTPStatusCode'] == 204
        except Exception as e:
            raise DataJointError(
                'error deleting file {r} ({e})'.format(r=rpath, e=e)
            )
