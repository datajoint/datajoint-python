"""
AWS S3 operations
"""

from io import BytesIO
import minio  # https://docs.minio.io/docs/python-client-api-reference
import urllib3
import uuid
import logging
from pathlib import Path
from . import errors

logger = logging.getLogger(__name__.split(".")[0])


class Folder:
    """
    A Folder instance manipulates a flat folder of objects within an S3-compatible object store
    """

    def __init__(
        self,
        endpoint,
        bucket,
        access_key,
        secret_key,
        *,
        secure=False,
        proxy_server=None,
        **_
    ):
        # from https://docs.min.io/docs/python-client-api-reference
        self.client = minio.Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            http_client=(
                urllib3.ProxyManager(
                    proxy_server,
                    timeout=urllib3.Timeout.DEFAULT_TIMEOUT,
                    cert_reqs="CERT_REQUIRED",
                    retries=urllib3.Retry(
                        total=5,
                        backoff_factor=0.2,
                        status_forcelist=[500, 502, 503, 504],
                    ),
                )
                if proxy_server
                else None
            ),
        )
        self.bucket = bucket
        if not self.client.bucket_exists(bucket):
            raise errors.BucketInaccessible("Inaccessible s3 bucket %s" % bucket)

    def put(self, name, buffer):
        logger.debug("put: {}:{}".format(self.bucket, name))
        return self.client.put_object(
            self.bucket, str(name), BytesIO(buffer), length=len(buffer)
        )

    def fput(self, local_file, name, metadata=None):
        logger.debug("fput: {} -> {}:{}".format(self.bucket, local_file, name))
        return self.client.fput_object(
            self.bucket, str(name), str(local_file), metadata=metadata
        )

    def get(self, name):
        logger.debug("get: {}:{}".format(self.bucket, name))
        try:
            with self.client.get_object(self.bucket, str(name)) as result:
                data = [d for d in result.stream()]
            return b"".join(data)
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise errors.MissingExternalFile("Missing s3 key %s" % name)
            else:
                raise e

    def fget(self, name, local_filepath):
        """get file from object name to local filepath"""
        logger.debug("fget: {}:{}".format(self.bucket, name))
        name = str(name)
        stat = self.client.stat_object(self.bucket, name)
        meta = {k.lower().lstrip("x-amz-meta"): v for k, v in stat.metadata.items()}
        data = self.client.get_object(self.bucket, name)
        local_filepath = Path(local_filepath)
        local_filepath.parent.mkdir(parents=True, exist_ok=True)
        with local_filepath.open("wb") as f:
            for d in data.stream(1 << 16):
                f.write(d)
        if "contents_hash" in meta:
            return uuid.UUID(meta["contents_hash"])

    def exists(self, name):
        logger.debug("exists: {}:{}".format(self.bucket, name))
        try:
            self.client.stat_object(self.bucket, str(name))
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                return False
            else:
                raise e
        return True

    def get_size(self, name):
        logger.debug("get_size: {}:{}".format(self.bucket, name))
        try:
            return self.client.stat_object(self.bucket, str(name)).size
        except minio.error.S3Error as e:
            if e.code == "NoSuchKey":
                raise errors.MissingExternalFile
            raise e

    def remove_object(self, name):
        logger.debug("remove_object: {}:{}".format(self.bucket, name))
        try:
            self.client.remove_object(self.bucket, str(name))
        except minio.error.MinioException:
            raise errors.DataJointError("Failed to delete %s from s3 storage" % name)
