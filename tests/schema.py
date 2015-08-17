"""
Test schema definition
"""

import random
import numpy as np
import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))


@schema
class User(dj.Lookup):
    definition = """      # lab members
    username: varchar(12)
    """
    contents = [['Jake'], ['Cathryn'], ['Shan'], ['Fabian'], ['Edgar'], ['George'], ['Dimitri']]


@schema
class Subject(dj.Manual):
    definition = """  # Basic information about animal subjects used in experiments
    subject_id   :int  #  unique subject id
    ---
    real_id            :varchar(40)  # real-world name. Omit if the same as subject_id
    species = "mouse"  :enum('mouse', 'monkey', 'human')
    date_of_birth      :date
    subject_notes      :varchar(4000)
    unique index (real_id, species)
    """

    contents = [
        [1551, '1551', 'mouse', '2015-04-01', 'genetically engineered super mouse'],
        [10, 'Curious George', 'monkey', '2008-06-30', ''],
        [1552, '1552', 'mouse', '2015-06-15', ''],
        [1553, '1553', 'mouse', '2016-07-01', '']]

    def _prepare(self):
        self.insert(self.contents, ignore_errors=True)


@schema
class Language(dj.Lookup):

    definition = """
    # languages spoken by some of the developers

    name        : varchar(40) # name of the developer
    language    : varchar(40) # language
    ---
    """

    contents = [
            ('Fabian', 'English'),
            ('Edgar', 'English'),
            ('Dimitri', 'English'),
            ('Dimitri', 'Ukrainian'),
            ('Fabian', 'German'),
            ('Edgar', 'Japanese'),
    ]


@schema
class Experiment(dj.Imported):
    definition = """  # information about experiments
    -> Subject
    experiment_id  :smallint  # experiment number for this subject
    ---
    experiment_date  :date   # date when experiment was started
    -> User
    data_path=""     :varchar(255)  # file path to recorded data
    notes=""         :varchar(2048) # e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :timestamp   # automatic timestamp
    """

    fake_experiments_per_subject = 5

    def _make_tuples(self, key):
        """
        populate with random data
        """
        from datetime import date, timedelta
        users = User().fetch()['username']
        random.seed('Amazing Seed')
        for experiment_id in range(self.fake_experiments_per_subject):
            self.insert1(
                dict(key,
                     experiment_id=experiment_id,
                     experiment_date=(date.today()-timedelta(random.expovariate(1/30))).isoformat(),
                     username=random.choice(users)))


@schema
class Trial(dj.Imported):
    definition = """   # a trial within an experiment
    -> Experiment
    trial_id  :smallint   # trial number
    ---
    start_time                 :double      # (s)
    """

    def _make_tuples(self, key):
        """
        populate with random data (pretend reading from raw files)
        """
        random.seed('Amazing Seed')
        for trial_id in range(10):
            self.insert1(
                dict(key,
                     trial_id=trial_id,
                     start_time=random.random()*1e9
                     ))


@schema
class Ephys(dj.Imported):
    definition = """    # some kind of electrophysiological recording
    -> Trial
    ----
    sampling_frequency :double  # (Hz)
    duration           :double  # (s)
    """

    def _make_tuples(self, key):
        """
        populate with random data
        """
        random.seed('Amazing seed')
        row = dict(key,
                   sampling_frequency=6000,
                   duration=np.minimum(2, random.expovariate(1)))
        self.insert1(row)
        number_samples = round(row['duration'] * row['sampling_frequency'])
        EphysChannel().fill(key, number_samples=number_samples)


@schema
class EphysChannel(dj.Subordinate, dj.Imported):
    definition = """     # subtable containing individual channels
    -> Ephys
    channel    :tinyint unsigned   # channel number within Ephys
    ----
    voltage    :longblob
    """

    def fill(self, key, number_samples):
        """
        populate random trace of specified length
        """
        random.seed('Amazing seed')
        for channel in range(2):
            self.insert1(
                dict(key,
                     channel=channel,
                     voltage=np.float32(np.random.randn(number_samples))
                     ))
