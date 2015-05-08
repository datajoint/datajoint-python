"""
Test 1 Schema definition
"""
__author__ = 'eywalker'

import datajoint as dj
from .. import schema2

class Subjects(dj.Base):
    definition = """
    test1.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """

# test reference to another table in same schema
class Experiments(dj.Base):
    definition = """
    test1.Experiments (imported)   # Experiment info
    -> test1.Subjects
    exp_id     : int            # unique id for experiment
    ---
    exp_data_file   : varchar(255) # data file
    """

# refers to a table in dj_test2 (bound to test2) but without a class
class Session(dj.Base):
    definition = """
    test1.Session (manual)     # Experiment sessions
    -> test1.Subjects
    -> test2.Experimenter
    session_id     : int       # unique session id
    ---
    session_comment        : varchar(255)    # comment about the session
    """

class Match(dj.Base):
    definition = """
    test1.Match (manual)     # Match between subject and color
    -> schema2.Subjects
    ---
    dob    : date     # date of birth
    """


class Empty(dj.Base):
    pass
