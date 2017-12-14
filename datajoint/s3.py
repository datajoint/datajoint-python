"""
AWS S3 operations
"""
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

sessions = {}   # a dictionary of reused S3 sessions


class S3:
    """
    An S3 instance manipulates one specific object stored in AWS S3
    """
    def __init__(self, bucket, aws_access_key_id, aws_secret_access_key, location, database, blob_hash):
        cred = (aws_access_key_id, aws_secret_access_key)
        if cred not in sessions:
            # cache sessions
            session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                    aws_secret_access_key=aws_secret_access_key)
            sessions[cred] = session.resource('s3')
        remote_path = '/'.join((location.lstrip('/'), database, blob_hash))
        self.object = sessions[cred].Object(bucket, remote_path)

    def __bool__(self):
        # True if object is found
        try:
            self.object.load()
        except ClientError as e:
            if e.response['Error']['Code'] != "404":
                raise
            return False
        else:
            return True

    def put(self, blob):
        if not self:
            self.object.upload_fileobj(BytesIO(blob))

    def get(self):
        obj = BytesIO()
        self.download_fileobj(obj)
        return obj.getvalue()

    def delete(self):
        r = self.object.delete()
        return r['ResponseMetadata']['HTTPStatusCode'] == 204
