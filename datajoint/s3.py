"""
This module contains logic related to s3 file storage
"""

import logging
from io import BytesIO

import boto3
from botocore.exceptions import ClientError

from . import config
from . import DataJointError

logger = logging.getLogger(__name__)


class Bucket:
    """
    A dj.Bucket object manages a connection to an AWS S3 Bucket.

    Currently, basic CRUD operations are supported; of note permissions and
    object versioning are not currently supported.

    Most of the parameters below should be set in the local configuration file.

    :param name: S3 Bucket Name
    :param key_id: AWS Access Key ID
    :param key: AWS Secret Key
    """

    def __init__(self, name, key_id, key):

        self._session = boto3.Session(aws_access_key_id=key_id,
                                      aws_secret_access_key=key)
        self._s3 = None
        self._bucket = name

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
        Upload a file to the bucket.

        :param obj: local 'file-like' object
        :param rpath: remote path within bucket
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).upload_fileobj(obj)
        except Exception as e:
            raise DataJointError(
                'Error uploading file {o} to {r} ({e})'.format(
                    o=obj, r=rpath, e=e))

        return True

    def get(self, rpath=None, obj=None):
        """
        Retrieve a file from the bucket.

        :param rpath: remote path within bucket
        :param obj: local 'file-like' object
        """
        try:
            self.connect()
            self._s3.Object(self._bucket, rpath).download_fileobj(obj)
        except Exception as e:
            raise DataJointError(
                'Error downloading file {r} to {o} ({e})'.format(
                    r=rpath, o=obj, e=e))

        return obj

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
                'error deleting file {r} ({e})'.format(r=rpath, e=e))


def bucket(aws_bucket_name, aws_access_key_id, aws_secret_access_key):
    """
    Returns a dj.Bucket object to be shared by multiple modules.
    If the connection is not yet established or reset=True, a new
    connection is set up. If connection information is not provided,
    it is taken from config which takes the information from
    dj_local_conf.json.

    :param aws_bucket_name: S3 bucket name
    :param aws_access_key_id: AWS Access Key ID
    :param aws_secret_access_key: AWS Secret Key
    """
    if not hasattr(bucket, 'bucket'):
        bucket.bucket = {}

    if aws_bucket_name in bucket.bucket:
        return bucket.bucket[aws_bucket_name]

    b = Bucket(aws_bucket_name, aws_access_key_id, aws_secret_access_key)

    bucket.bucket[aws_bucket_name] = b

    return b


def get_config(store):
    try:
        spec = config[store]
        bucket_name = spec['bucket']
        key_id = spec['aws_access_key_id']
        key = spec['aws_secret_access_key']
        location = spec['location']
    except KeyError as e:
        raise DataJointError(
            'Store {s} misconfigured for s3 {e}.'.format(s=store, e=e))

    return bucket_name, key_id, key, location


def make_rpath_name(location, database, blob_hash):
    rpath = '{l}/{d}/{h}'.format(l=location, d=database, h=blob_hash)
    # s3 is '' rooted; prevent useless '/' top-level 'directory'
    return rpath[1:] if rpath[0] == '/' else rpath


def put(db, store, blob, blob_hash):
    name, kid, key, loc = get_config(store)
    b = bucket(name, kid, key)
    rpath = make_rpath_name(loc, db, blob_hash)
    if not b.stat(rpath):
        b.put(BytesIO(blob), rpath)


def get(db, store, blob_hash):
    name, kid, key, loc = get_config(store)
    b = bucket(name, kid, key)
    rpath = make_rpath_name(loc, db, blob_hash)
    return b.get(rpath, BytesIO()).getvalue()


def delete(db, store, blob_hash):
    name, kid, key, loc = get_config(store)
    b = bucket(name, kid, key)
    rpath = make_rpath_name(loc, db, blob_hash)
    b.delete(rpath)
