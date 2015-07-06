"""
Test schema definition
"""

import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))

@schema
class Subjects(dj.Manual):
    definition = """
    #Basic subject
    subject_id                  : int      # unique subject id
    ---
    real_id                     :  varchar(40)    #  real-world name
    species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
    """

@schema
class Animals(dj.Manual):
    definition = """
    # information of non-human subjects
    -> Subjects
    ---
    animal_dob      :date       # date of birth
    """

@schema
class Trials(dj.Manual):
    definition = """
    # info about trials
    -> Subjects
    trial_id: int
    ---
    outcome: int            # result of experiment
    notes="": varchar(4096) # other comments
    trial_ts=CURRENT_TIMESTAMP: timestamp     # automatic
    """

@schema
class Matrix(dj.Manual):
    definition = """
    # Some numpy array
    matrix_id: int       # unique matrix id
    ---
    data:    longblob   #  data
    comment: varchar(1000) # comment
    """

@schema
class SquaredScore(dj.Computed):
    definition = """
    # cumulative outcome of trials
    -> Subjects
    -> Trials
    ---
    squared: int  # squared result of Trials outcome
    """

    def _make_tuples(self, key):
        outcome = (Trials() & key).fetch1()['outcome']
        self.insert1(dict(key, squared=outcome ** 2))
        ss = SquaredSubtable()
        for i in range(10):
            ss.insert1(dict(key, dummy=i))


@schema
class ErrorGenerator(dj.Computed):
    definition = """
    # ignore
    -> Subjects
    -> Trials
    ---
    dummy: int # ignore
    """

    def _make_tuples(self, key):
        raise Exception("This is for testing")


@schema
class SquaredSubtable(dj.Subordinate, dj.Manual):
    definition = """
    # cumulative outcome of trials
    -> SquaredScore
    dummy: int  # dummy primary attribute
    ---
    """