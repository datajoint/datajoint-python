"""
AWS S3 operations
"""
from io import BytesIO
import minio   # https://docs.minio.io/docs/python-client-api-reference
import warnings
import uuid
from pathlib import Path
from . import errors


class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """
    def __init__(self, endpoint, bucket, access_key, secret_key, *, secure=False, **_):
        self.client = minio.Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            warnings.warn('Creating bucket "%s"' % self.bucket)
            self.client.make_bucket(self.bucket)

    def put(self, name, buffer):
        return self.client.put_object(
            self.bucket, str(name), BytesIO(buffer), length=len(buffer))

    def fput(self, local_file, name, metadata=None):
        return self.client.fput_object(
            self.bucket, str(name), str(local_file), metadata=metadata)

    def get(self, name):
        try:
            return self.client.get_object(self.bucket, str(name)).data
        except minio.error.NoSuchKey:
            raise errors.MissingExternalFile('Missing s3 key %s' % name) from None

    def fget(self, name, local_filepath):
        """get file from object name to local filepath"""
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
        try:
            self.client.stat_object(self.bucket, str(name))
        except minio.error.NoSuchKey:
            return False
        return True

    def get_size(self, name):
        try:
            return self.client.stat_object(self.bucket, str(name)).size
        except minio.error.NoSuchKey:
            raise errors.MissingExternalFile from None

    def remove_object(self, name):
        try:
            self.client.remove_objects(self.bucket, str(name))
        except minio.ResponseError:
            return errors.DataJointError('Failed to delete %s from s3 storage' % name)
