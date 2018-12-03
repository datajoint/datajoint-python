"""
AWS S3 operations
"""
from io import BytesIO
import minio   # https://docs.minio.io/docs/python-client-api-reference
import warnings
import itertools


class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """
    def __init__(self, endpoint, bucket, access_key, secret_key, location, database, **_):
        self.client = minio.Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        self.bucket = bucket
        self.remote_path = '/'.join((location.lstrip('/'), database))

    def put(self, blob_hash, blob):
        try:
            self.client.put_object(self.bucket, '/'.join((self.remote_path, blob_hash)), BytesIO(blob), len(blob))
        except minio.error.NoSuchBucket:
            warnings.warn('Creating bucket "%s"' % self.bucket)
            self.client.make_bucket(self.bucket)
            self.put(blob_hash, blob)

    def get(self, blob_hash):
        try:
            return self.client.get_object(self.bucket, '/'.join((self.remote_path, blob_hash))).data
        except minio.error.NoSuchKey:
            return None

    def clean(self, exclude, max_count=None):
        """
        Delete all objects except for those in the exclude
        :param exclude: a list of blob_hashes to skip.
        :param max_count: maximum number of object to delete
        :return: generator of objects that failed to delete
        """
        return self.client.remove_objects(self.bucket, itertools.islice(
            (x.object_name for x in self.client.list_objects(self.bucket, self.remote_path + '/')
             if x not in exclude), max_count))
