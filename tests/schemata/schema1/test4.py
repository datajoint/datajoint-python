"""
Test 1 Schema definition - fully bound and has connection object
"""
__author__ = 'fabee'

import datajoint as dj


class Matrix(dj.Relation):
    definition = """
    test4.Matrix (manual)        # Some numpy array

    matrix_id       : int       # unique matrix id
    ---
    data                        :  longblob   #  data
    comment                     :  varchar(1000) # comment
    """
