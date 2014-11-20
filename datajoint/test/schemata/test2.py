"""
Test 2 Schema definition -  has conn but not bound
"""
__author__ = 'eywalker'

import datajoint as dj


class Subjects(dj.Base):
    """
    test2.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """