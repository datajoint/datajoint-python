"""
Test 1 Schema definition
"""
__author__ = 'eywalker'

import datajoint as dj
# from .. import schema2
from .. import PREFIX

testschema = dj.schema(PREFIX + '_test1', locals())

@testschema
class Subjects(dj.Manual):
    definition = """
    #Basic subject info

    subject_id                  : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """

# test for shorthand
@testschema
class Animals(dj.Manual):
    definition = """
    # Listing of all info

    -> Subjects
    ---
    animal_dob      :date       # date of birth
    """

@testschema
class Trials(dj.Manual):
    definition = """
    # info about trials

    -> Subjects
    trial_id                   : int
    ---
    outcome                    : int           # result of experiment

    notes=""                   : varchar(4096) # other comments
    trial_ts=CURRENT_TIMESTAMP : timestamp     # automatic
    """

@testschema
class Matrix(dj.Manual):
    definition = """
    # Some numpy array

    matrix_id       : int       # unique matrix id
    ---
    data                        :  longblob   #  data
    comment                     :  varchar(1000) # comment
    """


@testschema
class SquaredScore(dj.Computed):
    definition = """
    # cumulative outcome of trials

    -> Subjects
    -> Trials
    ---
    squared                    : int         # squared result of Trials outcome
    """

    @property
    def populate_relation(self):
        return Subjects() * Trials()

    def _make_tuples(self, key):
        tmp = (Trials() & key).fetch1()
        tmp2 = SquaredSubtable() & key

        self.insert(dict(key, squared=tmp['outcome']**2))

        ss = SquaredSubtable()

        for i in range(10):
            key['dummy'] = i
            ss.insert(key)

@testschema
class WrongImplementation(dj.Computed):
    definition = """
    # ignore

    -> Subjects
    -> Trials
    ---
    dummy                    : int         # ignore
    """

    @property
    def populate_relation(self):
        return {'subject_id':2}

    def _make_tuples(self, key):
        pass

class ErrorGenerator(dj.Computed):
    definition = """
    # ignore

    -> Subjects
    -> Trials
    ---
    dummy                    : int         # ignore
    """

    @property
    def populate_relation(self):
        return Subjects() * Trials()

    def _make_tuples(self, key):
        raise Exception("This is for testing")

@testschema
class SquaredSubtable(dj.Subordinate, dj.Manual):
    definition = """
    # cumulative outcome of trials

    -> SquaredScore
    dummy                      : int         # dummy primary attribute
    ---
    """
#
#
# # test reference to another table in same schema
# class Experiments(dj.Relation):
#     definition = """
#     test1.Experiments (imported)   # Experiment info
#     -> test1.Subjects
#     exp_id     : int               # unique id for experiment
#     ---
#     exp_data_file   : varchar(255) # data file
#     """
#
#
# # refers to a table in dj_test2 (bound to test2) but without a class
# class Sessions(dj.Relation):
#     definition = """
#     test1.Sessions (manual)     # Experiment sessions
#     -> test1.Subjects
#     -> test2.Experimenter
#     session_id     : int        # unique session id
#     ---
#     session_comment        : varchar(255)    # comment about the session
#     """
#
#
# class Match(dj.Relation):
#     definition = """
#     test1.Match (manual)     # Match between subject and color
#     -> schema2.Subjects
#     ---
#     dob    : date     # date of birth
#     """
#
#
# # this tries to reference a table in database directly without ORM
# class TrainingSession(dj.Relation):
#     definition = """
#     test1.TrainingSession (manual)  # training sessions
#     -> `dj_test2`.Experimenter
#     session_id    : int      # training session id
#     """
#
#
# class Empty(dj.Relation):
#     pass
