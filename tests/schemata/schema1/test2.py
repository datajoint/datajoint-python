"""
Test 2 Schema definition
"""
__author__ = 'eywalker'

import datajoint as dj
from . import test1 as alias
#from ..schema2 import test2 as test1


# references to another schema
class Experiments(dj.Relation):
    definition = """
    test2.Experiments (manual)     # Basic subject info
    -> test1.Subjects
    experiment_id       : int      # unique experiment id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """

# references to another schema
class Conditions(dj.Relation):
    definition = """
    test2.Conditions (manual)     # Subject conditions
    -> alias.Subjects
    condition_name              : varchar(255)    # description of the condition
    """

class FoodPreference(dj.Relation):
    definition = """
    test2.FoodPreference (manual)   # Food preference of each subject
    -> animals.Subjects
    preferred_food           : enum('banana', 'apple', 'oranges')
    """

class Session(dj.Relation):
    definition = """
    test2.Session (manual)     # Experiment sessions
    -> test1.Subjects
    -> test2.Experimenter
    session_id     : int       # unique session id
    ---
    session_comment        : varchar(255)    # comment about the session
    """