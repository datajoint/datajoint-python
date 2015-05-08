"""
Test 2 Schema definition
"""
__author__ = 'eywalker'

import datajoint as dj



class Subjects(dj.Base):
    definition = """
    schema2.Subjects (manual)     # Basic subject info
    pop_id       : int      # unique experiment id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """