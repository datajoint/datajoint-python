"""
Test 3 Schema definition - no binding, no conn

To be bound at the package level
"""
__author__ = 'eywalker'

import datajoint as dj


class Subjects(dj.Base):
    definition = """
    schema1.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    dob              : date     # date of birth
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """

