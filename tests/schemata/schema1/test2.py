"""
Test 2 Schema definition -  has conn but not bound
"""
__author__ = 'eywalker'

import datajoint as dj
#from ..schema2 import test2 as test1


class Experiments(dj.Base):
    _table_def = """
    test2.Experiments (manual)     # Basic subject info
    -> test1.Subjects
    experiment_id       : int      # unique experiment id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """