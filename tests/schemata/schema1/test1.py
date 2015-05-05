"""
Test 1 Schema definition - fully bound and has connection object
"""
__author__ = 'eywalker'

import datajoint as dj


class Subjects(dj.Base):
    definition = """
    test1.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """
