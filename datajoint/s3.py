"""
This module contains logic related to S3 file storage
"""

import logging
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

from . import DataJointError
from .hash import long_hash
from .blob import pack, unpack

from .external import ExternalFileHandler

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

    def put(self, obj=None, rpath=None):
        """
        Upload a 'bytes-like-object' to the bucket.

        :param obj: local object
        :param rpath: remote path within bucket
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).upload_fileobj(obj)
        except Exception as e:
            # XXX: risk of concatenating huge object? hmm.
            raise DataJointError(
                'Error uploading object {o} to {r} ({e})'.format(
                    o=obj, r=rpath, e=e)
            )

        return True

    def putfile(self, lpath=None, rpath=None):
        """
        Upload a file to the bucket.

        :param lpath: local path
        :param rpath: remote path within bucket
        """
        try:
            with open(lpath, 'rb') as obj:
                self.put(obj, rpath)
        except Exception as e:
            raise DataJointError(
                'Error uploading file {l} to {r} ({e})'.format(
                    l=lpath, r=rpath, e=e)
            )

        return True

    def get(self, rpath=None, obj=None):
        """
        Retrieve a file from the bucket into a 'bytes-like-object'

        :param rpath: remote path within bucket
        :param obj: local object
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).download_fileobj(obj)
        except Exception as e:
            # XXX: risk of concatenating huge object? hmm.
            raise DataJointError(
                'Error downloading object {r} to {o} ({e})'.format(
                    r=rpath, o=obj, e=e)
            )

        return True

    def getfile(self, rpath=None, lpath=None):
        """
        Retrieve a file from the bucket.

        :param rpath: remote path within bucket
        :param lpath: local path
        """
        try:
            with open(lpath, 'wb') as obj:
                self.get(rpath, obj)
        except Exception as e:
            # XXX: risk of concatenating huge object? hmm.
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


class S3FileHandler(ExternalFileHandler):

    # BytesIO(obj)
    # TODO: flip bucket to use upload_fileobj when required

    def __init__(self, store, database):
        super().__init__(store, database)

        required = ('bucket', 'location', 'aws_access_key_id',
                    'aws_secret_access_key',)

        missing = list(i for i in required if i not in self._spec)

        if len(missing):
            raise DataJointError(
                'Store "{store}" incorrectly configured for "s3"'.format(
                    store=store), 'missing', *missing)

        self._bucket = bucket(
            aws_access_key_id=self._spec['aws_access_key_id'],
            aws_secret_access_key=self._spec['aws_secret_access_key'],
            bucket=self._spec['bucket'])

        self._location = self._spec['location']

    def make_path(self, hash):
        return self._location + '/' + hash  # s3 - no path.join

    def put(self, obj):
        (blob, hash) = self.hash_obj(obj)
        rpath = self.make_path(hash)
        self._bucket.put(obj, rpath)
        return (blob, hash)

    def get(self, hash):
        rpath = self.make_path(hash)
        return unpack(self._bucket.get(rpath, BytesIO()))


ExternalFileHandler._handlers['s3'] = S3FileHandler
