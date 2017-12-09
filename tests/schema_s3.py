"""
a schema for testing external attributes
"""

import datajoint as dj

from . import PREFIX, CONN_INFO
import numpy as np

schema = dj.schema(PREFIX + '_s3', locals(),
                   connection=dj.conn(**CONN_INFO))


dj.config['external'] = {
    'protocol': 'file',
    'location': 'dj-store/external'}

dj.config['external-raw'] = {
    'protocol': 'file',
    'location': 'dj-store/raw'}

dj.config['external-compute'] = {
    'protocol': 's3',
    'location': '/datajoint-projects/test',
    'user': 'djtest',
    'token': '2e05709792545ce'}

dj.config['external-s3'] = {
    'protocol': 's3',
    'bucket': 'testbucket.datajoint.io',
    'location': '/datajoint-projects/test-external-s3',
    'aws_access_key_id': '1234567',
    'aws_secret_access_key': 'deadbeef'}

dj.config['external-cache-s3'] = {
    'protocol': 'cache-s3',
    'bucket': 'testbucket.datajoint.io',
    'location': '/datajoint-projects/test-external-cache-s3',
    'aws_access_key_id': '1234567',
    'aws_secret_access_key': 'deadbeef'}

dj.config['external-cache'] = {
    'protocol': 'cache',
    'location': './cache'}


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
