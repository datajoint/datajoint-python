import numpy as np

from unittest import TestCase
from numpy.testing import assert_array_equal
from nose.tools import assert_true, assert_equal

from moto import mock_s3

import datajoint as dj
from datajoint.external import ExternalTable

from . schema_s3 import schema


@mock_s3
class DjS3MockTest(TestCase):

    def setUp(self):
        # create moto's virtual bucket
        cfg = dj.config['external-s3']
        b = dj.s3.bucket(
            aws_bucket_name=cfg['bucket'],
            aws_access_key_id=cfg['aws_access_key_id'],
            aws_secret_access_key=cfg['aws_secret_access_key'])
        b.connect()
        b._s3.create_bucket(Bucket=b._bucket)

    def test_s3_methods(self):
        ext = ExternalTable(schema.connection, schema.database)
        ext.delete_quick()
        input_ = np.random.randn(3, 7, 8)
        count = 7
        extra = 3
        for i in range(count):
            hash1 = ext.put('external-s3', input_)
        for i in range(extra):
            hash2 = ext.put('external-s3', np.random.randn(4, 3, 2))

        assert_true(all(hash in ext.fetch('hash') for hash in (hash1, hash2)))
        assert_equal(len(ext), 1 + extra)

        output_ = ext.get(hash1)
        assert_array_equal(input_, output_)
