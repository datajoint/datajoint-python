"""
a schema for testing external attributes
"""

import tempfile
import datajoint as dj

from . import PREFIX, CONN_INFO
import numpy as np

schema = dj.schema(PREFIX + '_extern', connection=dj.conn(**CONN_INFO))


dj.config['stores'] = {
    'local': {
        'protocol': 'file',
        'location': tempfile.mkdtemp(),
        'subfolding': (1, 1)
    },

    'raw': {
        'protocol': 's3',
        'location': tempfile.mkdtemp()},

    'repo': {
        'stage_path': tempfile.mkdtemp(),
        'protocol': 'file',
        'location': tempfile.mkdtemp()}
}

dj.config['cache'] = tempfile.mkdtemp()


@schema
class Simple(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : blob@local
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
    img : blob@raw     #  objects are stored as specified by dj.config['stores'][raw']
    neg : blob@local   # objects are stored as specified by dj.config['stores']['local']
    """

    def make(self, key):
        np.random.seed(key['seed'])
        img = np.random.rand(*(Dimension() & key).fetch1('dimensions'))
        self.insert1(dict(key, img=img, neg=-img.astype(np.float32)))


@schema
class Attach(dj.Manual):
    definition = """
    # table for storing attachments
    attach : int
    ----
    img : attach@raw    #  attachments are stored as specified by dj.config['stores']['raw']
    txt : attach      #  attachments are stored directly in the database
    """

@schema
class Filepath(dj.Manual):
    definition = """
    # table for file management 
    fnum : int 
    ---
    img : filepath@repo  # managed files 
    """