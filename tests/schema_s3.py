"""
a schema for testing external attributes
"""
import tempfile

import numpy as np

import datajoint as dj

from . import PREFIX, CONN_INFO

schema = dj.schema(PREFIX + '_s3', locals(),
                   connection=dj.conn(**CONN_INFO))

dj.config['external-s3'] = {
    'protocol': 's3',
    'bucket': 'testbucket.datajoint.io',
    'location': '/datajoint-projects/test-external-s3',
    'aws_access_key_id': '1234567',
    'aws_secret_access_key': 'deadbeef'}

dj.config['cache'] = tempfile.mkdtemp('dj-cache')


@schema
class Simple(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : external-s3
    """


@schema
class Seed(dj.Lookup):
    definition = """
    seed :  int
    """
    contents = zip(range(4))


@schema
class Dimension(dj.Lookup):
    definition = """
    dim  : int
    ---
    dimensions  : blob
    """
    contents = (
        [0, [100, 50]],
        [1, [3, 4, 8, 6]])


@schema
class Image(dj.Computed):
    definition = """
    # table for storing
    -> Seed
    -> Dimension
    ----
    img  : external-s3    #  configured in dj.config['external-s3']
    """

    def make(self, key):
        np.random.seed(key['seed'])
        self.insert1(dict(key, img=np.random.rand(
            *(Dimension() & key).fetch1('dimensions'))))
