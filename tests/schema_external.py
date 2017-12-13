"""
a schema for testing external attributes
"""

import tempfile
import datajoint as dj

from . import PREFIX, CONN_INFO
import numpy as np

schema = dj.schema(PREFIX + '_extern', locals(), connection=dj.conn(**CONN_INFO))


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

dj.config['cache'] = tempfile.mkdtemp('dj-cache')


@schema
class Simple(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : external-raw
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
    img  : external-raw    #  objects are stored as specified by dj.config['external-raw']
    neg : external    # objects are stored as specified by dj.config['external']
    """

    def make(self, key):
        np.random.seed(key['seed'])
        img = np.random.rand(*(Dimension() & key).fetch1('dimensions'))
        self.insert1(dict(key, img=img, neg=-img.astype(np.float32)))
