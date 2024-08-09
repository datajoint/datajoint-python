"""
Sample schema with realistic tables for testing
"""

import random
import numpy as np
import datajoint as dj
import inspect


class TTest(dj.Lookup):
    """
    doc string
    """

    definition = """
    key   :   int     # key
    ---
    value   :   int     # value
    """
    contents = [(k, 2 * k) for k in range(10)]


class TTest2(dj.Manual):
    definition = """
    key   :   int     # key
    ---
    value   :   int     # value
    """


class TTest3(dj.Manual):
    definition = """
    key : int
    ---
    value : varchar(300)
    """


class NullableNumbers(dj.Manual):
    definition = """
    key : int
    ---
    fvalue = null : float
    dvalue = null : double
    ivalue = null : int
    """


class TTestExtra(dj.Manual):
    """
    clone of Test but with an extra field
    """

    definition = TTest.definition + "\nextra : int # extra int\n"


class TTestNoExtra(dj.Manual):
    """
    clone of Test but with no extra fields
    """

    definition = TTest.definition


class Auto(dj.Lookup):
    definition = """
    id  :int auto_increment
    ---
    name :varchar(12)
    """

    def fill(self):
        if not self:
            self.insert([dict(name="Godel"), dict(name="Escher"), dict(name="Bach")])


class User(dj.Lookup):
    definition = """      # lab members
    username: varchar(12)
    """
    contents = [
        ["Jake"],
        ["Cathryn"],
        ["Shan"],
        ["Fabian"],
        ["Edgar"],
        ["George"],
        ["Dimitri"],
    ]


class Subject(dj.Lookup):
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
        [1551, "1551", "mouse", "2015-04-01", "genetically engineered super mouse"],
        [10, "Curious George", "monkey", "2008-06-30", ""],
        [1552, "1552", "mouse", "2015-06-15", ""],
        [1553, "1553", "mouse", "2016-07-01", ""],
    ]


class Language(dj.Lookup):
    definition = """
    # languages spoken by some of the developers
    # additional comments are ignored
    name        : varchar(40) # name of the developer
    language    : varchar(40) # language
    """
    contents = [
        ("Fabian", "English"),
        ("Edgar", "English"),
        ("Dimitri", "English"),
        ("Dimitri", "Ukrainian"),
        ("Fabian", "German"),
        ("Edgar", "Japanese"),
    ]


class Experiment(dj.Imported):
    definition = """  # information about experiments
    -> Subject
    experiment_id  :smallint  # experiment number for this subject
    ---
    experiment_date  :date   # date when experiment was started
    -> [nullable] User
    data_path=""     :varchar(255)  # file path to recorded data
    notes=""         :varchar(2048) # e.g. purpose of experiment
    entry_time=CURRENT_TIMESTAMP :timestamp   # automatic timestamp
    """

    fake_experiments_per_subject = 5

    def make(self, key):
        """
        populate with random data
        """
        from datetime import date, timedelta

        users = [None, None] + list(User().fetch()["username"])
        random.seed("Amazing Seed4")
        self.insert(
            dict(
                key,
                experiment_id=experiment_id,
                experiment_date=(
                    date.today() - timedelta(random.expovariate(1 / 30))
                ).isoformat(),
                username=random.choice(users),
            )
            for experiment_id in range(self.fake_experiments_per_subject)
        )


class Trial(dj.Imported):
    definition = """   # a trial within an experiment
    -> Experiment.proj(animal='subject_id')
    trial_id  :smallint   # trial number
    ---
    start_time                 :double      # (s)
    """

    class Condition(dj.Part):
        definition = """   # trial conditions
        -> Trial
        cond_idx : smallint   # condition number
        ----
        orientation :  float   # degrees
        """

    def make(self, key):
        """populate with random data (pretend reading from raw files)"""
        random.seed("Amazing Seed")
        trial = self.Condition()
        for trial_id in range(10):
            key["trial_id"] = trial_id
            self.insert1(dict(key, start_time=random.random() * 1e9))
            trial.insert(
                dict(key, cond_idx=cond_idx, orientation=random.random() * 360)
                for cond_idx in range(30)
            )


class Ephys(dj.Imported):
    definition = """    # some kind of electrophysiological recording
    -> Trial
    ----
    sampling_frequency :double  # (Hz)
    duration           :decimal(7,3)  # (s)
    """

    class Channel(dj.Part):
        definition = """     # subtable containing individual channels
        -> master
        channel    :tinyint unsigned   # channel number within Ephys
        ----
        voltage    : longblob
        current = null : longblob   # optional current to test null handling
        """

    def _make_tuples(self, key):
        """
        populate with random data
        """
        random.seed(str(key))
        row = dict(
            key, sampling_frequency=6000, duration=np.minimum(2, random.expovariate(1))
        )
        self.insert1(row)
        number_samples = int(row["duration"] * row["sampling_frequency"] + 0.5)
        sub = self.Channel()
        sub.insert(
            dict(
                key,
                channel=channel,
                voltage=np.float32(np.random.randn(number_samples)),
            )
            for channel in range(2)
        )


class Image(dj.Manual):
    definition = """
    # table for testing blob inserts
    id           : int # image identifier
    ---
    img             : longblob # image
    """


class UberTrash(dj.Lookup):
    definition = """
    id : int
    ---
    """
    contents = [(1,)]


class UnterTrash(dj.Lookup):
    definition = """
    -> UberTrash
    my_id   : int
    ---
    """
    contents = [(1, 1), (1, 2)]


class SimpleSource(dj.Lookup):
    definition = """
    id : int  # id
    """
    contents = [(x,) for x in range(10)]


class SigIntTable(dj.Computed):
    definition = """
    -> SimpleSource
    """

    def _make_tuples(self, key):
        raise KeyboardInterrupt


class SigTermTable(dj.Computed):
    definition = """
    -> SimpleSource
    """

    def make(self, key):
        raise SystemExit("SIGTERM received")


class DjExceptionName(dj.Lookup):
    definition = """
    dj_exception_name:    char(64)
    """

    @property
    def contents(self):
        return [
            [member_name]
            for member_name, member_type in inspect.getmembers(dj.errors)
            if inspect.isclass(member_type) and issubclass(member_type, Exception)
        ]


class ErrorClass(dj.Computed):
    definition = """
    -> DjExceptionName
    """

    def make(self, key):
        exception_name = key["dj_exception_name"]
        raise getattr(dj.errors, exception_name)


class DecimalPrimaryKey(dj.Lookup):
    definition = """
    id  :  decimal(4,3)
    """
    contents = list(zip((0.1, 0.25, 3.99)))


class IndexRich(dj.Manual):
    definition = """
    -> Subject
    ---
    -> [unique, nullable] User.proj(first="username")
    first_date : date
    value : int
    index (first_date, value)
    """


#  Schema for issue 656
class ThingA(dj.Manual):
    definition = """
    a: int
    """


class ThingB(dj.Manual):
    definition = """
    b1: int
    b2: int
    ---
    b3: int
    """


class ThingC(dj.Manual):
    definition = """
    -> ThingA
    ---
    -> [unique, nullable] ThingB
    """


#  Additional tables for #1159
class ThingD(dj.Manual):
    definition = """
    d: int
    ---
    -> ThingC
    """


class ThingE(dj.Manual):
    definition = """
    -> ThingD
    """


class Parent(dj.Lookup):
    definition = """
    parent_id: int
    ---
    name: varchar(30)
    """
    contents = [(1, "Joe")]


class Child(dj.Lookup):
    definition = """
    -> Parent
    child_id: int
    ---
    name: varchar(30)
    """
    contents = [(1, 12, "Dan")]


# Related to issue #886 (8), #883 (5)
class ComplexParent(dj.Lookup):
    definition = "\n".join(["parent_id_{}: int".format(i + 1) for i in range(8)])
    contents = [tuple(i for i in range(8))]


class ComplexChild(dj.Lookup):
    definition = "\n".join(
        ["-> ComplexParent"] + ["child_id_{}: int".format(i + 1) for i in range(1)]
    )
    contents = [tuple(i for i in range(9))]


class SubjectA(dj.Lookup):
    definition = """
    subject_id: varchar(32)
    ---
    dob : date
    sex : enum('M', 'F', 'U')
    """
    contents = [
        ("mouse1", "2020-09-01", "M"),
        ("mouse2", "2020-03-19", "F"),
        ("mouse3", "2020-08-23", "F"),
    ]


class SessionA(dj.Lookup):
    definition = """
    -> SubjectA
    session_start_time: datetime
    ---
    session_dir=''  : varchar(32)
    """
    contents = [
        ("mouse1", "2020-12-01 12:32:34", ""),
        ("mouse1", "2020-12-02 12:32:34", ""),
        ("mouse1", "2020-12-03 12:32:34", ""),
        ("mouse1", "2020-12-04 12:32:34", ""),
    ]


class SessionStatusA(dj.Lookup):
    definition = """
    -> SessionA
    ---
    status: enum('in_training', 'trained_1a', 'trained_1b', 'ready4ephys')
    """
    contents = [
        ("mouse1", "2020-12-01 12:32:34", "in_training"),
        ("mouse1", "2020-12-02 12:32:34", "trained_1a"),
        ("mouse1", "2020-12-03 12:32:34", "trained_1b"),
        ("mouse1", "2020-12-04 12:32:34", "ready4ephys"),
    ]


class SessionDateA(dj.Lookup):
    definition = """
    -> SubjectA
    session_date:  date
    """
    contents = [
        ("mouse1", "2020-12-01"),
        ("mouse1", "2020-12-02"),
        ("mouse1", "2020-12-03"),
        ("mouse1", "2020-12-04"),
    ]


class Stimulus(dj.Lookup):
    definition = """
    id: int
    ---
    contrast: int
    brightness: int
    """


class Longblob(dj.Manual):
    definition = """
    id: int
    ---
    data: longblob
    """


LOCALS_ANY = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_ANY)
