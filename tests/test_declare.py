import pytest
from .schema import *
import datajoint as dj
import inspect
from datajoint.declare import declare


class TestDeclare:

    @classmethod
    def setup_class(cls):
        cls.auto = Auto()
        cls.auto.fill()
        cls.user = User()
        cls.subject = Subject()
        cls.experiment = Experiment()
        cls.trial = Trial()
        cls.ephys = Ephys()
        cls.channel = Ephys.Channel()

    def test_schema_decorator(self, schema_any):
        assert issubclass(Subject, dj.Lookup)
        assert not issubclass(Subject, dj.Part)

    def test_class_help(self, schema_any):
        help(TTest)
        help(TTest2)
        assert TTest.definition in TTest.__doc__
        assert TTest.definition in TTest2.__doc__

    def test_instance_help(self, schema_any):
        help(TTest())
        help(TTest2())
        assert TTest().definition in TTest().__doc__
        assert TTest2().definition in TTest2().__doc__

    def test_describe(self, schema_any):
        """real_definition should match original definition"""
        rel = Experiment()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert s1 == s2

    def test_describe_indexes(self, schema_any):
        """real_definition should match original definition"""
        rel = IndexRich()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert s1 == s2

    def test_describe_dependencies(self, schema_any):
        """real_definition should match original definition"""
        rel = ThingC()
        context = inspect.currentframe().f_globals
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, rel.describe(), context)
        assert s1 == s2

    def test_part(self, schema_any):
        # Lookup and part with the same name.  See issue #365
        local_schema = dj.Schema(schema.database)

        @local_schema
        class Type(dj.Lookup):
            definition = """
            type :  varchar(255)
            """
            contents = zip(("Type1", "Type2", "Type3"))

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

    def test_attributes(self, schema_any):
        # test autoincrement declaration
        assert auto.heading.names == ["id", "name"]
        assert auto.heading.attributes["id"].autoincrement

        # test attribute declarations
        assert (
            subject.heading.names ==
            ["subject_id", "real_id", "species", "date_of_birth", "subject_notes"])
        assert subject.primary_key == ["subject_id"]
        assert subject.heading.attributes["subject_id"].numeric
        assert not subject.heading.attributes["real_id"].numeric

        assert (
            experiment.heading.names ==
            [
                "subject_id",
                "experiment_id",
                "experiment_date",
                "username",
                "data_path",
                "notes",
                "entry_time",
            ])
        assert experiment.primary_key == ["subject_id", "experiment_id"]

        assert (
            trial.heading.names ==  # tests issue #516
            ["animal", "experiment_id", "trial_id", "start_time"])
        assert trial.primary_key == ["animal", "experiment_id", "trial_id"]

        assert (
            ephys.heading.names ==
            ["animal", "experiment_id", "trial_id", "sampling_frequency", "duration"])
        assert ephys.primary_key == ["animal", "experiment_id", "trial_id"]

        assert (
            channel.heading.names ==
            ["animal", "experiment_id", "trial_id", "channel", "voltage", "current"])
        assert (
            channel.primary_key == ["animal", "experiment_id", "trial_id", "channel"])
        assert channel.heading.attributes["voltage"].is_blob

    def test_dependencies(self, schema_any):
        assert experiment.full_table_name in user.children(primary=False)
        assert set(experiment.parents(primary=False)) == {user.full_table_name}
        assert experiment.full_table_name in user.children(primary=False)
        assert set(experiment.parents(primary=False)) == {user.full_table_name}
        assert (
            set(
                s.full_table_name
                for s in experiment.parents(primary=False, as_objects=True)
            ) ==
            {user.full_table_name})

        assert experiment.full_table_name in subject.descendants()
        assert (experiment.full_table_name
            in {s.full_table_name for s in subject.descendants(as_objects=True)})
        assert subject.full_table_name in experiment.ancestors()
        assert (subject.full_table_name
            in {s.full_table_name for s in experiment.ancestors(as_objects=True)})

        assert trial.full_table_name in experiment.descendants()
        assert (trial.full_table_name
            in {s.full_table_name for s in experiment.descendants(as_objects=True)})
        assert experiment.full_table_name in trial.ancestors()
        assert (experiment.full_table_name
            in {s.full_table_name for s in trial.ancestors(as_objects=True)})

        assert (
            set(trial.children(primary=True)) ==
            {ephys.full_table_name, trial.Condition.full_table_name})
        assert set(trial.parts()) == {trial.Condition.full_table_name}
        assert (
            set(s.full_table_name for s in trial.parts(as_objects=True)) ==
            {trial.Condition.full_table_name})
        assert set(ephys.parents(primary=True)) == {trial.full_table_name}
        assert (
            set(
                s.full_table_name for s in ephys.parents(primary=True, as_objects=True)
            ) ==
            {trial.full_table_name})
        assert set(ephys.children(primary=True)) == {channel.full_table_name}
        assert (
            set(
                s.full_table_name for s in ephys.children(primary=True, as_objects=True)
            ) ==
            {channel.full_table_name})
        assert set(channel.parents(primary=True)) == {ephys.full_table_name}
        assert (
            set(
                s.full_table_name
                for s in channel.parents(primary=True, as_objects=True)
            ) ==
            {ephys.full_table_name})

    def test_descendants_only_contain_part_table(self, schema_any):
        """issue #927"""

        @schema
        class A(dj.Manual):
            definition = """
            a: int
            """

        @schema
        class B(dj.Manual):
            definition = """
            -> A
            b: int
            """

        @schema
        class Master(dj.Manual):
            definition = """
            table_master: int
            """

            class Part(dj.Part):
                definition = """
                -> master
                -> B
                """

        assert A.descendants() == [
            "`djtest_test1`.`a`",
            "`djtest_test1`.`b`",
            "`djtest_test1`.`master__part`",
        ]

    @raises(dj.DataJointError)
    def test_bad_attribute_name(self):
        @schema
        class BadName(dj.Manual):
            definition = """
            Bad_name : int
            """

    @raises(dj.DataJointError)
    def test_bad_fk_rename(self):
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

    @raises(dj.DataJointError)
    def test_primary_nullable_foreign_key(self):
        @schema
        class Q(dj.Manual):
            definition = """
            -> [nullable] Experiment
            """

    @raises(dj.DataJointError)
    def test_invalid_foreign_key_option(self):
        @schema
        class R(dj.Manual):
            definition = """
            -> Experiment
            ----
            -> [optional] User
            """

    @raises(dj.DataJointError)
    def test_unsupported_datatype(self):
        @schema
        class Q(dj.Manual):
            definition = """
            experiment : int
            ---
            description : text
            """

    def test_int_datatype(self):
        @schema
        class Owner(dj.Manual):
            definition = """
            ownerid : int
            ---
            car_count : integer
            """

    @raises(dj.DataJointError)
    def test_unsupported_int_datatype(self):
        @schema
        class Driver(dj.Manual):
            definition = """
            driverid : tinyint
            ---
            car_count : tinyinteger
            """

    @raises(dj.DataJointError)
    def test_long_table_name(self):
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
