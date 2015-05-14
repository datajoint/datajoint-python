"""
Test 1 Schema definition
"""
__author__ = 'eywalker'

import datajoint as dj
from .. import schema2


class Subjects(dj.Relation):
    definition = """
    test1.Subjects (manual)     # Basic subject info

    subject_id       : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """


class Trials(dj.Relation):
    definition = """
    test1.Trials (manual)                      # info about trials

    -> test1.Subjects
    trial_id                   : int
    ---
    outcome                    : int           # result of experiment

    notes=""                   : varchar(4096) # other comments
    trial_ts=CURRENT_TIMESTAMP : timestamp    # automatic
    """


# test reference to another table in same schema
class Experiments(dj.Relation):
    definition = """
    test1.Experiments (imported)   # Experiment info
    -> test1.Subjects
    exp_id     : int            # unique id for experiment
    ---
    exp_data_file   : varchar(255) # data file
    """


# refers to a table in dj_test2 (bound to test2) but without a class
class Sessions(dj.Relation):
    definition = """
    test1.Sessions (manual)     # Experiment sessions
    -> test1.Subjects
    -> test2.Experimenter
    session_id     : int       # unique session id
    ---
    session_comment        : varchar(255)    # comment about the session
    """


class Match(dj.Relation):
    definition = """
    test1.Match (manual)     # Match between subject and color
    -> schema2.Subjects
    ---
    dob    : date     # date of birth
    """


# this tries to reference a table in database directly without ORM
class TrainingSession(dj.Relation):
    definition = """
    test1.TrainingSession (manual)  # training sessions
    -> `dj_test2`.Experimenter
    session_id    : int      # training session id
    """


class Empty(dj.Relation):
    pass
