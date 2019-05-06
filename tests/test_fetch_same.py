from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from . import PREFIX, CONN_INFO
# from .schema import *
import datajoint as dj

import numpy as np

class TestFetchSame:

    @staticmethod
    def test_object_conversion():

        Schema = dj.schema(PREFIX + '_fetch_same', connection=dj.conn(**CONN_INFO))

        @Schema
        class ProjData(dj.Manual):
            definition = """
            id : int                 
            ---
            resp : float       
            sim  : float         
            """

        data_ins = [
            {'id' : 0, 'resp' : 20.33, 'sim' : 45.324},
            {'id' : 1, 'resp' : 94.3,'sim' : 34.23},
            {'id' : 2, 'resp' : 1.90,'sim' : 10.23}    
        ]

        ProjData().insert(data_ins)

        trials_new = ProjData.proj(new='resp-sim')
        new = trials_new.fetch('new')

        assert_equal(new.dtype,np.float64)

    @staticmethod
    def test_object_conversion_all():

        Schema = dj.schema(PREFIX + '_fetch_same', connection=dj.conn(**CONN_INFO))

        @Schema
        class ProjData(dj.Manual):
            definition = """
            # Testing blobs
            id : int                 
            ---
            resp : float       
            sim  : float         
            """

        trials_new = ProjData.proj(prop='resp-sim')
        new = trials_new.fetch()

        assert_equal(new['prop'].dtype,np.float64)   
    
