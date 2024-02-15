"""
A schema for testing external attributes
"""

import tempfile
import inspect
import datajoint as dj
import numpy as np


class Simple(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : blob@local
    """


class SimpleRemote(dj.Manual):
    definition = """
    simple  : int
    ---
    item  : blob@share
    """


class Seed(dj.Lookup):
    definition = """
    seed :  int
    """
    contents = zip(range(4))


class Dimension(dj.Lookup):
    definition = """
    dim  : int
    ---
    dimensions  : blob
    """
    contents = ([0, [100, 50]], [1, [3, 4, 8, 6]])


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
        np.random.seed(key["seed"])
        img = np.random.rand(*(Dimension() & key).fetch1("dimensions"))
        self.insert1(dict(key, img=img, neg=-img.astype(np.float32)))


class Attach(dj.Manual):
    definition = """
    # table for storing attachments
    attach : int
    ----
    img : attach@share    #  attachments are stored as specified by: dj.config['stores']['raw']
    txt : attach      #  attachments are stored directly in the database
    """


class Filepath(dj.Manual):
    definition = """
    # table for file management
    fnum : int # test comment containing :
    ---
    img : filepath@repo  # managed files
    """


class FilepathS3(dj.Manual):
    definition = """
    # table for file management
    fnum : int
    ---
    img : filepath@repo-s3  # managed files
    """


LOCALS_EXTERNAL = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_EXTERNAL)
