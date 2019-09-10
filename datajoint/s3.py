"""
AWS S3 operations
"""
from io import BytesIO
import minio   # https://docs.minio.io/docs/python-client-api-reference
import warnings
import uuid
import os
from . import errors

class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """
    def __init__(self, endpoint, bucket, access_key, secret_key, location, **_):
        self.client = minio.Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            warnings.warn('Creating bucket "%s"' % self.bucket)
            self.client.make_bucket(self.bucket)
        self.remote_path = location.lstrip('/')

    def put(self, relative_name, buffer):
        return self.client.put_object(
            self.bucket, '/'.join((self.remote_path, relative_name)), BytesIO(buffer), length=len(buffer))

    def fput(self, relative_name, local_file, **meta):
        return self.client.fput_object(
            self.bucket, '/'.join((self.remote_path, relative_name)), local_file, metadata=meta or None)

    def get(self, relative_name):
        try:
            return self.client.get_object(self.bucket, '/'.join((self.remote_path, relative_name))).data
        except minio.error.NoSuchKey:
            raise errors.MissingExternalFile from None

    def fget(self, relative_name, local_filepath):
        """get file from object name to local filepath"""
        name = '/'.join((self.remote_path, relative_name))
        stat = self.client.stat_object(self.bucket, name)
        meta = {k.lower().lstrip('x-amz-meta'): v for k, v in stat.metadata.items()}
        data = self.client.get_object(self.bucket, name)
        os.makedirs(os.path.split(local_filepath)[0], exist_ok=True)
        with open(local_filepath, 'wb') as f:
            for d in data.stream(1 << 16):
                f.write(d)
        return uuid.UUID(meta['contents_hash'])

    def partial_get(self, relative_name, offset, size):
        try:
            return self.client.get_partial_object(
                self.bucket, '/'.join((self.remote_path, relative_name)), offset, size).data
        except minio.error.NoSuchKey:
            raise errors.MissingExternalFile from None

    def get_size(self, relative_name):
        try:
            return self.client.stat_object(self.bucket, '/'.join((self.remote_path, relative_name))).size
        except minio.error.NoSuchKey:
            raise errors.MissingExternalFile from None

    def list_objects(self, folder=''):
        return self.client.list_objects(self.bucket, '/'.join((self.remote_path, folder, '')), recursive=True)

    def remove_objects(self, objects_iter):

        failed_deletes = self.client.remove_objects(self.bucket, objects_iter=objects_iter)
        return list(failed_deletes)
