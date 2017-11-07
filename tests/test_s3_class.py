
"""
Test of dj.Bucket() using moto *MOCKED* S3 library
Using real s3 could incur cost, requires appropriate credentials managment;
but probably should be done at some point once best methodology is determined.

"""

import os
from unittest import TestCase

import boto3
from moto import mock_s3

import datajoint as dj

# Verify moto is itself functional
# BEGIN via Moto Docs


class MotoTest:
    '''
    Simple example to verify moto is itself working
    '''

    def __init__(self, name, value):
        self.name = name
        self.value = value

    def save(self):
        s3 = boto3.client('s3', region_name='us-east-1')
        s3.put_object(Bucket='mybucket', Key=self.name, Body=self.value)


@mock_s3
def test_moto_test():
    # Create Bucket so that test can run
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='mybucket')

    model_instance = MotoTest('steve', 'is awesome')
    model_instance.save()

    body = conn.Object('mybucket', 'steve').get()['Body'].read().decode()

    assert body == 'is awesome'

# END via Moto Docs


@mock_s3
def test_dj_bucket_factory():
    '''
    Test the dj.bucket() singleton/factory function.
    '''
    b = dj.Bucket.get_bucket(aws_access_key_id=None,
                             aws_secret_access_key=None,
                             bucket='test')
    assert dj.bucket(aws_access_key_id=None, aws_secret_access_key=None,
                     bucket='test') == b


@mock_s3
class DjBucketTest(TestCase):

    def setUp(self):
        b = dj.Bucket.get_bucket(aws_access_key_id=None,
                                 aws_secret_access_key=None,
                                 bucket='test')

        # create moto's virtual bucket
        b.connect()  # note: implicit test of b.connect(), which is trivial
        b._s3.create_bucket(Bucket='test')
        self._bucket = b

        # todo:
        # - appropriate remote filename (e.g. mkstemp())
        # - appropriate local temp filename (e.g. mkstemp())
        self._lfile = __file__
        self._rfile = 'DjBucketTest-TEMP_NO_EDIT_WILL_ZAP.py'
        self._lfile_cpy = self._rfile

        self._zaptmpfile()

    def tearDown(self):
        self._zaptmpfile()

    def _zaptmpfile(self):
        try:
            os.remove(self._lfile_cpy)
        except FileNotFoundError:
            pass

    def test_bucket_methods(self):
        '''
        Test dj.Bucket.(putfile,stat,getfile,delete,)()
        Currently done in one test to simplify interdependencies.

        Note:
        putfile/getfile are currently implemented in terms of put/get
        and so put/get not directly tested.

        TODO: Test failure modes.
        '''

        # ensure no initial files
        assert self._bucket.delete(self._rfile) is True
        assert self._bucket.stat(self._rfile) is False
        assert os.path.exists(self._lfile_cpy) is False

        # test put
        assert self._bucket.putfile(self._lfile, self._rfile) is True

        # test stat
        assert self._bucket.stat(self._rfile) is True

        # test get
        assert self._bucket.getfile(self._rfile, self._lfile_cpy) is True
        assert os.path.exists(self._lfile_cpy) is True

        # test delete
        assert self._bucket.delete(self._rfile) is True

        # verify delete
        assert self._bucket.stat(self._rfile) is False
