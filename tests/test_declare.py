from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from .schema import *
import datajoint as dj
import inspect
from datajoint.declare import declare


auto = Auto()
auto.fill()
user = User()
subject = Subject()
experiment = Experiment()
trial = Trial()
ephys = Ephys()
channel = Ephys.Channel()


class TestDeclare:

    @staticmethod
    def test_schema_decorator():
        assert_true(issubclass(Subject, dj.Lookup))
        assert_true(not issubclass(Subject, dj.Part))

    @staticmethod
    def test_class_help():
        help(TTest)
        help(TTest2)
        assert_true(TTest.definition in TTest.__doc__)
        assert_true(TTest.definition in TTest2.__doc__)

    @staticmethod
    def test_instance_help():
        help(TTest())
        help(TTest2())
        assert_true(TTest().definition in TTest().__doc__)
        assert_true(TTest2().definition in TTest2().__doc__)

    @staticmethod
    def test_describe():
        """real_definition should match original definition"""
        rel = Experiment()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert_equal(s1, s2)

    @staticmethod
    def test_describe_indexes():
        """real_definition should match original definition"""
        rel = IndexRich()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert_equal(s1, s2)

    @staticmethod
    def test_describe_dependencies():
        """real_definition should match original definition"""
        rel = ThingC()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert_equal(s1, s2)


    @staticmethod
    def test_part():
        # Lookup and part with the same name.  See issue #365
        local_schema = dj.Schema(schema.database)

        @local_schema
        class Type(dj.Lookup):
            definition = """
            type :  varchar(255)
            """
            contents = zip(('Type1', 'Type2', 'Type3'))

        @local_schema
        class TypeMaster(dj.Manual):
            definition = """
            master_id : int
            """
            class Type(dj.Part):
                definition = """
                -> TypeMaster
                -> Type
                """

    @staticmethod
    def test_attributes():
        # test autoincrement declaration
        assert_list_equal(auto.heading.names, ['id', 'name'])
        assert_true(auto.heading.attributes['id'].autoincrement)

        # test attribute declarations
        assert_list_equal(subject.heading.names,
                          ['subject_id', 'real_id', 'species', 'date_of_birth', 'subject_notes'])
        assert_list_equal(subject.primary_key,
                          ['subject_id'])
        assert_true(subject.heading.attributes['subject_id'].numeric)
        assert_false(subject.heading.attributes['real_id'].numeric)

        assert_list_equal(experiment.heading.names,
                          ['subject_id', 'experiment_id', 'experiment_date',
                           'username', 'data_path',
                           'notes', 'entry_time'])
        assert_list_equal(experiment.primary_key,
                          ['subject_id', 'experiment_id'])

        assert_list_equal(trial.heading.names,   # tests issue #516
                          ['animal', 'experiment_id', 'trial_id', 'start_time'])
        assert_list_equal(trial.primary_key,
                          ['animal', 'experiment_id', 'trial_id'])

        assert_list_equal(ephys.heading.names,
                          ['animal', 'experiment_id', 'trial_id', 'sampling_frequency', 'duration'])
        assert_list_equal(ephys.primary_key,
                          ['animal', 'experiment_id', 'trial_id'])

        assert_list_equal(channel.heading.names,
                          ['animal', 'experiment_id', 'trial_id', 'channel', 'voltage', 'current'])
        assert_list_equal(channel.primary_key,
                          ['animal', 'experiment_id', 'trial_id', 'channel'])
        assert_true(channel.heading.attributes['voltage'].is_blob)

    @staticmethod
    def test_dependencies():
        assert_true(experiment.full_table_name in set(user.children(primary=False)))
        assert_equal(set(experiment.parents(primary=False)), {user.full_table_name})

        assert_true(experiment.full_table_name in subject.descendants())
        assert_true(subject.full_table_name in experiment.ancestors())

        assert_true(trial.full_table_name in experiment.descendants())
        assert_true(experiment.full_table_name in trial.ancestors())

        assert_equal(set(trial.children(primary=True)),
                     {ephys.full_table_name, trial.Condition.full_table_name})
        assert_equal(set(ephys.parents(primary=True)), {trial.full_table_name})

        assert_equal(set(ephys.children(primary=True)), {channel.full_table_name})
        assert_equal(set(channel.parents(primary=True)), {ephys.full_table_name})

    @staticmethod
    @raises(dj.DataJointError)
    def test_bad_attribute_name():

        @schema
        class BadName(dj.Manual):
            definition = """
            Bad_name : int
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_bad_fk_rename():
        """issue #381"""

        @schema
        class A(dj.Manual):
            definition = """
            a : int
            """

        @schema
        class B(dj.Manual):
            definition = """
            b -> A    # invalid, the new syntax is (b) -> A
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_primary_nullable_foreign_key():
        @schema
        class Q(dj.Manual):
            definition = """
            -> [nullable] Experiment
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_invalid_foreign_key_option():
        @schema
        class R(dj.Manual):
            definition = """
            -> Experiment
            ----
            -> [optional] User
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_unsupported_datatype():

        @schema
        class Q(dj.Manual):
            definition = """
            experiment : int
            ---
            description : text
            """

    @staticmethod
    def test_int_datatype():

        @schema
        class Owner(dj.Manual):
            definition = """
            ownerid : int
            ---
            car_count : integer
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_unsupported_int_datatype():

        @schema
        class Driver(dj.Manual):
            definition = """
            driverid : tinyint
            ---
            car_count : tinyinteger
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_long_table_name():
        """
        test issue #205 -- reject table names over 64 characters in length
        """

        @schema
        class WhyWouldAnyoneCreateATableNameThisLong(dj.Manual):
            definition = """
            master : int
            """

            class WithSuchALongPartNameThatItCrashesMySQL(dj.Part):
                definition = """
                -> (master)
                """
