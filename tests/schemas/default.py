import datajoint as dj, numpy as np, random, inspect, pytest
from .. import PREFIX, connection_test


@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + "_test1"), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def TTest(schema):
    @schema
    class TTest(dj.Lookup):
        __doc__ = """
        doc string
        """
        definition = """
        key   :   int     # key
        ---
        value   :   int     # value
        """
        contents = [(k, 2 * k) for k in range(10)]

    yield TTest
    TTest.drop()


@pytest.fixture
def TTest2(schema):
    @schema
    class TTest2(dj.Manual):
        definition = """
        key   :   int     # key
        ---
        value   :   int     # value
        """

    yield TTest2
    TTest2.drop()


@pytest.fixture
def TTest3(schema):
    @schema
    class TTest3(dj.Manual):
        definition = """
        key : int
        ---
        value : varchar(300)
        """

    yield TTest3
    TTest3.drop()


@pytest.fixture
def NullableNumbers(schema):
    @schema
    class NullableNumbers(dj.Manual):
        definition = """
        key : int
        ---
        fvalue = null : float
        dvalue = null : double
        ivalue = null : int
        """

    yield NullableNumbers
    NullableNumbers.drop()


@pytest.fixture
def TTestExtra(schema):
    @schema
    class TTestExtra(dj.Manual):
        __doc__ = """
        clone of TTest but with an extra field
        """
        definition = """
        key   :   int     # key
        ---
        value   :   int     # value
        extra : int # extra int
        """

    yield TTestExtra
    TTestExtra.drop()


@pytest.fixture
def TTestNoExtra(schema):
    @schema
    class TTestNoExtra(dj.Manual):
        __doc__ = """
        clone of TTest but with no extra fields
        """
        definition = """
        key   :   int     # key
        ---
        value   :   int     # value
        """

    yield TTestNoExtra
    TTestNoExtra.drop()


@pytest.fixture
def Auto(schema):
    @schema
    class Auto(dj.Lookup):
        definition = """
        id  :int auto_increment
        ---
        name :varchar(12)
        """

        def fill(self):
            if not self:
                self.insert(
                    [dict(name="Godel"), dict(name="Escher"), dict(name="Bach")]
                )

    yield Auto
    Auto.drop()


@pytest.fixture
def User(schema):
    @schema
    class User(dj.Lookup):
        definition = """
        # lab members
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

    yield User
    User.drop()


@pytest.fixture
def Subject(schema):
    @schema
    class Subject(dj.Lookup):
        definition = """
        # Basic information about animal subjects used in experiments
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

    yield Subject
    Subject.drop()


@pytest.fixture
def Language(schema):
    @schema
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

    yield Language
    Language.drop()


@pytest.fixture
def Experiment(schema, Subject, User):
    @schema
    class Experiment(dj.Imported):
        definition = """
        # information about experiments
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
            from datetime import date, timedelta

            users = [None, None] + list(User().fetch()["username"])
            self.insert(
                (
                    dict(
                        key,
                        experiment_id=experiment_id,
                        experiment_date=(
                            (
                                date.today()
                                - timedelta(random.expovariate(0.03333333333333333))
                            ).isoformat()
                        ),
                        username=(random.choice(users)),
                    )
                    for experiment_id in range(self.fake_experiments_per_subject)
                )
            )

    yield Experiment
    Experiment.drop()


@pytest.fixture
def Trial(schema, Experiment):
    @schema
    class Trial(dj.Imported):
        definition = """
        # a trial within an experiment
        -> Experiment.proj(animal='subject_id')
        trial_id  :smallint   # trial number
        ---
        start_time                 :double      # (s)
        """

        class Condition(dj.Part):
            definition = """
            # trial conditions
            -> Trial
            cond_idx : smallint   # condition number
            ----
            orientation :  float   # degrees
            """

        def make(self, key):
            """populate with random data (pretend reading from raw files)"""
            trial = self.Condition()
            for trial_id in range(10):
                key["trial_id"] = trial_id
                self.insert1(dict(key, start_time=(random.random() * 1000000000.0)))
                trial.insert(
                    (
                        dict(
                            key, cond_idx=cond_idx, orientation=(random.random() * 360)
                        )
                        for cond_idx in range(30)
                    )
                )

    yield Trial
    Trial.drop()


@pytest.fixture
def Ephys(schema, Trial):
    @schema
    class Ephys(dj.Imported):
        definition = """
        # some kind of electrophysiological recording
        -> Trial
        ----
        sampling_frequency :double  # (Hz)
        duration           :decimal(7,3)  # (s)
        """

        class Channel(dj.Part):
            definition = """
            # subtable containing individual channels
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
            row = dict(
                key,
                sampling_frequency=6000,
                duration=(np.minimum(2, random.expovariate(1))),
            )
            self.insert1(row)
            number_samples = int(row["duration"] * row["sampling_frequency"] + 0.5)
            sub = self.Channel()
            sub.insert(
                (
                    dict(
                        key,
                        channel=channel,
                        voltage=(np.float32(np.random.randn(number_samples))),
                    )
                    for channel in range(2)
                )
            )

    yield Ephys
    Ephys.drop()


@pytest.fixture
def Image(schema):
    @schema
    class Image(dj.Manual):
        definition = """
        # table for testing blob inserts
        id           : int # image identifier
        ---
        img             : longblob # image
        """

    yield Image
    Image.drop()


@pytest.fixture
def UberTrash(schema):
    @schema
    class UberTrash(dj.Lookup):
        definition = """
        id : int
        ---
        """
        contents = [(1,)]

    yield UberTrash
    UberTrash.drop()


@pytest.fixture
def UnterTrash(schema, UberTrash):
    @schema
    class UnterTrash(dj.Lookup):
        definition = """
        -> UberTrash
        my_id   : int
        ---
        """
        contents = [(1, 1), (1, 2)]

    yield UnterTrash
    UnterTrash.drop()


@pytest.fixture
def SimpleSource(schema):
    @schema
    class SimpleSource(dj.Lookup):
        definition = """
        id : int  # id
        """
        contents = ((x,) for x in range(10))

    yield SimpleSource
    SimpleSource.drop()


@pytest.fixture
def SigIntTable(schema, SimpleSource):
    @schema
    class SigIntTable(dj.Computed):
        definition = """
        -> SimpleSource
        """

        def _make_tuples(self, key):
            raise KeyboardInterrupt

    yield SigIntTable
    SigIntTable.drop()


@pytest.fixture
def SigTermTable(schema, SimpleSource):
    @schema
    class SigTermTable(dj.Computed):
        definition = """
        -> SimpleSource
        """

        def make(self, key):
            raise SystemExit("SIGTERM received")

    yield SigTermTable
    SigTermTable.drop()


@pytest.fixture
def DjExceptionName(schema):
    @schema
    class DjExceptionName(dj.Lookup):
        definition = """
        dj_exception_name:    char(64)
        """

        @property
        def contents(self):
            return [
                [member_name]
                for member_name, member_type in inspect.getmembers(dj.errors)
                if inspect.isclass(member_type)
                if issubclass(member_type, Exception)
            ]

    yield DjExceptionName
    DjExceptionName.drop()


@pytest.fixture
def ErrorClass(schema, DjExceptionName):
    @schema
    class ErrorClass(dj.Computed):
        definition = """
        -> DjExceptionName
        """

        def make(self, key):
            exception_name = key["dj_exception_name"]
            raise getattr(dj.errors, exception_name)

    yield ErrorClass
    ErrorClass.drop()


@pytest.fixture
def DecimalPrimaryKey(schema):
    @schema
    class DecimalPrimaryKey(dj.Lookup):
        definition = """
        id  :  decimal(4,3)
        """
        contents = zip((0.1, 0.25, 3.99))

    yield DecimalPrimaryKey
    DecimalPrimaryKey.drop()


@pytest.fixture
def IndexRich(schema, Subject, User):
    @schema
    class IndexRich(dj.Manual):
        definition = """
        -> Subject
        ---
        -> [unique, nullable] User.proj(first="username")
        first_date : date
        value : int
        index (first_date, value)
        """

    yield IndexRich
    IndexRich.drop()


@pytest.fixture
def ThingA(schema):
    @schema
    class ThingA(dj.Manual):
        definition = """
        a: int
        """

    yield ThingA
    ThingA.drop()


@pytest.fixture
def ThingB(schema):
    @schema
    class ThingB(dj.Manual):
        definition = """
        b1: int
        b2: int
        ---
        b3: int
        """

    yield ThingB
    ThingB.drop()


@pytest.fixture
def ThingC(schema, ThingA, ThingB):
    @schema
    class ThingC(dj.Manual):
        definition = """
        -> ThingA
        ---
        -> [unique, nullable] ThingB
        """

    yield ThingC
    ThingC.drop()


@pytest.fixture
def Parent(schema):
    @schema
    class Parent(dj.Lookup):
        definition = """
        parent_id: int
        ---
        name: varchar(30)
        """
        contents = [(1, "Joe")]

    yield Parent
    Parent.drop()


@pytest.fixture
def Child(schema, Parent):
    @schema
    class Child(dj.Lookup):
        definition = """
        -> Parent
        child_id: int
        ---
        name: varchar(30)
        """
        contents = [(1, 12, "Dan")]

    yield Child
    Child.drop()


@pytest.fixture
def ComplexParent(schema):
    @schema
    class ComplexParent(dj.Lookup):
        definition = "\n".join(["parent_id_{}: int".format(i + 1) for i in range(8)])
        contents = [tuple((i for i in range(8)))]

    yield ComplexParent
    ComplexParent.drop()


@pytest.fixture
def ComplexChild(schema, ComplexParent):
    @schema
    class ComplexChild(dj.Lookup):
        definition = "\n".join(
            ["-> ComplexParent"] + ["child_id_{}: int".format(i + 1) for i in range(1)]
        )
        contents = [tuple((i for i in range(9)))]

    yield ComplexChild
    ComplexChild.drop()


@pytest.fixture
def SubjectA(schema):
    @schema
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

    yield SubjectA
    SubjectA.drop()


@pytest.fixture
def SessionA(schema, SubjectA):
    @schema
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

    yield SessionA
    SessionA.drop()


@pytest.fixture
def SessionStatusA(schema, SessionA):
    @schema
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

    yield SessionStatusA
    SessionStatusA.drop()


@pytest.fixture
def SessionDateA(schema, SubjectA):
    @schema
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

    yield SessionDateA
    SessionDateA.drop()


@pytest.fixture
def Stimulus(schema):
    @schema
    class Stimulus(dj.Lookup):
        definition = """
        id: int
        ---
        contrast: int
        brightness: int
        """

    yield Stimulus
    Stimulus.drop()


@pytest.fixture
def Longblob(schema):
    @schema
    class Longblob(dj.Manual):
        definition = """
        id: int
        ---
        data: longblob
        """

    yield Longblob
    Longblob.drop()
