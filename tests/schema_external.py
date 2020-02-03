"""
a schema for testing external attributes
"""

import tempfile
import datajoint as dj

from . import PREFIX, CONN_INFO, S3_CONN_INFO
import numpy as np

schema = dj.Schema(PREFIX + '_extern', connection=dj.conn(**CONN_INFO))
dj.config['enable_python_native_blobs'] = True


stores_config = {

    'raw': dict(
        protocol='file',
        location=tempfile.mkdtemp()),

    'repo': dict(
        stage=tempfile.mkdtemp(),
        protocol='file',
        location=tempfile.mkdtemp()),

    'repo_s3': dict(
        S3_CONN_INFO,
        protocol='s3',
        location='dj/repo',
        stage=tempfile.mkdtemp()),

    'local': dict(
        protocol='file',
        location=tempfile.mkdtemp(),
        subfolding=(1, 1)),

    'share': dict(
        S3_CONN_INFO,
        protocol='s3',
        location='dj/store/repo',
        subfolding=(2, 4))
}

dj.config['stores'] = stores_config

dj.config['cache'] = tempfile.mkdtemp()


@schema
class Simple(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : blob@local
    """


@schema
class SimpleRemote(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : blob@share
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
    img : blob@share     #  objects are stored as specified by dj.config['stores']['share']
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
    img : attach@share    #  attachments are stored as specified by: dj.config['stores']['raw']
    txt : attach      #  attachments are stored directly in the database
    """


dj.errors._switch_filepath_types(True)


@schema
class Filepath(dj.Manual):
    definition = """
    # table for file management 
    fnum : int # test comment containing :
    ---
    img : filepath@repo  # managed files 
    """


@schema
class FilepathS3(dj.Manual):
    definition = """
    # table for file management 
    fnum : int 
    ---
    img : filepath@repo_s3  # managed files 
    """

dj.errors._switch_filepath_types(False)