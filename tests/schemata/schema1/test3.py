"""
Test 3 Schema definition - no binding, no conn
"""
__author__ = 'eywalker'

import datajoint as dj


class Subjects(dj.Base):
    _table_def = """
    test3.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """