"""
AWS S3 operations
"""
from io import BytesIO
import minio   # https://docs.minio.io/docs/python-client-api-reference
import warnings
import uuid
import logging
from pathlib import Path
from . import errors

logger = logging.getLogger(__name__)


class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """
    def __init__(self, endpoint, bucket, access_key, secret_key, *, secure=False, **_):
        self.client = minio.Minio(endpoint, access_key=access_key, secret_key=secret_key,
                                  secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            raise errors.BucketInaccessible('Inaccessible s3 bucket %s' % bucket) from None

    def put(self, name, buffer):
        logger.debug('put: {}:{}'.format(self.bucket, name))
        return self.client.put_object(
            self.bucket, str(name), BytesIO(buffer), length=len(buffer))

    def fput(self, local_file, name, metadata=None):
        logger.debug('fput: {} -> {}:{}'.format(self.bucket, local_file, name))
        return self.client.fput_object(
            self.bucket, str(name), str(local_file), metadata=metadata)

    def get(self, name):
        logger.debug('get: {}:{}'.format(self.bucket, name))
        try:
            return self.client.get_object(self.bucket, str(name)).data
        except minio.error.S3Error as e:
            if e.code == 'NoSuchKey':
                raise errors.MissingExternalFile('Missing s3 key %s' % name) from None
            else:
                raise e

    def fget(self, name, local_filepath):
        """get file from object name to local filepath"""
        logger.debug('fget: {}:{}'.format(self.bucket, name))
        name = str(name)
        stat = self.client.stat_object(self.bucket, name)
        meta = {k.lower().lstrip('x-amz-meta'): v for k, v in stat.metadata.items()}
        data = self.client.get_object(self.bucket, name)
        local_filepath = Path(local_filepath)
        local_filepath.parent.mkdir(parents=True, exist_ok=True)
        with local_filepath.open('wb') as f:
            for d in data.stream(1 << 16):
                f.write(d)
        if 'contents_hash' in meta:
            return uuid.UUID(meta['contents_hash'])

    def exists(self, name):
        logger.debug('exists: {}:{}'.format(self.bucket, name))
        try:
            self.client.stat_object(self.bucket, str(name))
        except minio.error.S3Error as e:
            if e.code == 'NoSuchKey':
                return False
            else:
                raise e
        return True

    def get_size(self, name):
        logger.debug('get_size: {}:{}'.format(self.bucket, name))
        try:
            return self.client.stat_object(self.bucket, str(name)).size
        except minio.error.S3Error as e:
            if e.code == 'NoSuchKey':
                raise errors.MissingExternalFile from None
            else:
                raise e

    def remove_object(self, name):
        logger.debug('remove_object: {}:{}'.format(self.bucket, name))
        try:
            self.client.remove_object(self.bucket, str(name))
        except minio.ResponseError:
            return errors.DataJointError('Failed to delete %s from s3 storage' % name)
