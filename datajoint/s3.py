"""
AWS S3 operations
"""
from io import BytesIO
from minio import Minio   # https://docs.minio.io/docs/python-client-api-reference

class Folder:
    """
    An S3 instance manipulates a folder of objects in AWS S3
    """
    def __init__(self, endpoint, bucket, access_key, secret_key, location, database, **_):
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)
        self.bucket = bucket
        self.remote_path = '/'.join((location.lstrip('/'), database))

    def make_bucket(self):
        self.client.make_bucket(self.bucket)

    def put(self, blob_hash, blob):
        self.client.put_object(self.bucket, '/'.join((self.remote_path, blob_hash)), BytesIO(blob), len(blob))

    def get(self, blob_hash):
        return self.get_object(self.bucket, '/'.join((self.remote_path, blob_hash))).data

    def clean(self, except_list):
        """
        Delete all objects except for those in the except_list
        :param except_list: a list of blob_hashes to skip.
        """
        raise NotImplementedError
