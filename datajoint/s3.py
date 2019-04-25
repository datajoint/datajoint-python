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

    def partial_get(self, blob_hash, offset, size):
        try:
            return self.client.get_partial_object(self.bucket, '/'.join((self.remote_path, blob_hash)), offset, size).data
        except minio.error.NoSuchKey:
            return None

    def get_size(self, blob_hash):
        try:
            return self.client.stat_object(self.bucket, '/'.join((self.remote_path, blob_hash))).size
        except minio.error.NoSuchKey:
            return None

    def clean(self, exclude, max_count=None, verbose=False):
        """
        Delete all objects except for those in the exclude
        :param exclude: a list of blob_hashes to skip.
        :param max_count: maximum number of object to delete
        :param verbose: If True, print deleted objects
        :return: list of objects that failed to delete
        """
        count = itertools.count()
        if verbose:
            def out(name):
                next(count)
                print(name)
                return name
        else:
            def out(name):
                next(count)
                return name

        if verbose:
            print('Deleting...')

        names = (out(x.object_name)
                 for x in self.client.list_objects(self.bucket, self.remote_path + '/', recursive=True)
                 if x.object_name.split('/')[-1] not in exclude)

        failed_deletes = list(
            self.client.remove_objects(self.bucket, itertools.islice(names, max_count)))

        print('Deleted: %i S3 objects' % next(count))
        return failed_deletes
