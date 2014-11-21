"""
Test 2 Schema definition -  has conn but not bound
"""
__author__ = 'eywalker'

import datajoint as dj



class Experiments(dj.Base):
    """
    test2.Experiments (manual)     # Basic subject info

    experiment_id       : int      # unique experiment id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """