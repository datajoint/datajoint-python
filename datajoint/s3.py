"""
This module contains logic related to external file storage
"""

import logging
from getpass import getpass

import boto3
from botocore.exceptions import ClientError

from . import config
from . import DataJointError

logger = logging.getLogger(__name__)


def bucket(aws_access_key_id=None, aws_secret_access_key=None, reset=False):
    """
    Returns a boto3 AWS session object to be shared by multiple modules.
    If the connection is not yet established or reset=True, a new
    connection is set up. If connection information is not provided,
    it is taken from config which takes the information from
    dj_local_conf.json. If the password is not specified in that file
    datajoint prompts for the password.

    :param aws_access_key_id: AWS Access Key ID
    :param aws_secret_access_key: AWS Secret Key
    :param reset: whether the connection should be reset or not
    """
    if not hasattr(bucket, 'bucket') or reset:
        aws_access_key_id = aws_access_key_id \
            if aws_access_key_id is not None \
            else config['external.aws_access_key_id']

        aws_secret_access_key = aws_secret_access_key \
            if aws_secret_access_key is not None \
            else config['external.aws_secret_access_key']

        if aws_access_key_id is None:  # pragma: no cover
            aws_access_key_id = input("Please enter AWS Access Key ID: ")

        if aws_secret_access_key is None:  # pragma: no cover
            aws_secret_access_key = getpass(
                "Please enter AWS Secret Access Key: "
            )

        bucket.bucket = Bucket(aws_access_key_id, aws_secret_access_key)
    return bucket.bucket


class Bucket:
    """
    A dj.Bucket object manages a connection to an AWS S3 Bucket.

    Currently, basic CRUD operations are supported; of note permissions and
    object versioning are not currently supported.

    Most of the parameters below should be set in the local configuration file.

    :param aws_access_key_id: AWS Access Key ID
    :param aws_secret_access_key: AWS Secret Key
    """

    def __init__(self, aws_access_key_id, aws_secret_access_key):
        self._session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        self._s3 = None
        try:
            self._bucket = config['external.location'].split("s3://")[1]
        except (AttributeError, IndexError, KeyError) as e:
            raise DataJointError(
                'external.location not properly configured: {l}'.format(
                    l=config['external.location'])
                ) from None

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
