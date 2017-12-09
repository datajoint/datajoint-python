import os

from unittest import TestCase, SkipTest

from nose.tools import assert_true, assert_equal

import numpy as np
from numpy.testing import assert_array_equal

import datajoint as dj
from datajoint.external import ExternalTable

from . schema_s3 import schema


'''
Test *real* S3 access.

Requires environment variables:

  - AWS_ACCESS_KEY_ID
  - AWS_SECRET_ACCESS_KEY
  - AWS_BUCKET

be properly configured; will raise SkipTest (and not test) if they are not.

See also DJS3MockTest for a mock test implementation -
Not included in this module due to apparent side effects w/moto's mock_s3;

Similarly, running both in a suite together does not work if moto tests run
first in sequence - until a workaround for this can be determined, this file is
named 'test_s3_00_real', and other real AWS tests should be ordered correctly
in order to ensure proper run sequence.
'''


class DjS3TestReal(TestCase):

    def setUp(self):
        testvars = {'AWS_ACCESS_KEY_ID': 'aws_access_key_id',
                    'AWS_SECRET_ACCESS_KEY': 'aws_secret_access_key',
                    'AWS_BUCKET': 'bucket'}

        updates = dict(((testvars[k], os.environ.get(k))
                        for k in testvars if k in os.environ))

        if len(updates) != len(testvars):
            raise SkipTest

        dj.config['external-s3'].update(updates)

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
