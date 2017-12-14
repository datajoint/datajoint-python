"""
s3 storage operations
"""
from io import BytesIO
import boto3
from botocore.exceptions import ClientError

sessions = {}   # a dictionary of stored S3 sessions for reuse (in case they are expensive to establish)


def get_s3_object(bucket, aws_access_key_id, aws_secret_access_key, location, database, blob_hash):
    # create an S3 object or return the existing copy
    cred = (aws_access_key_id, aws_secret_access_key)
    if cred not in sessions:
        session = boto3.Session(aws_access_key_id=aws_access_key_id,
                                aws_secret_access_key=aws_secret_access_key)
        sessions[cred] = session.resource('s3')
    remote_path = '/'.join((location.lstrip('/'), database, blob_hash))
    return sessions[cred].Object(bucket, remote_path)


def exists(**spec):
    try:
        get_s3_object(**spec).load()
    except ClientError as e:
        if e.response['Error']['Code'] != "404":
            raise
        return False
    return True


def put(blob, **spec):
    if not exists(**spec):
        get_s3_object(**spec).upload_fileobj(BytesIO(blob))


def get(**spec):
    obj = BytesIO()
    get_s3_object(**spec).download_fileobj(obj)
    return obj.getvalue()


def delete(**spec):
    r = get_s3_object(**spec).delete()
    return r['ResponseMetadata']['HTTPStatusCode'] == 204
