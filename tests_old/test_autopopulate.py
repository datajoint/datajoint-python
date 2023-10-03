from nose.tools import assert_equal, assert_false, assert_true, raises
from . import schema, PREFIX
from datajoint import DataJointError
import datajoint as dj


class TestPopulate:
    """
    Test base relations: insert, delete
    """

    def setUp(self):
        self.user = schema.User()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.Ephys.Channel()

    def tearDown(self):
        # delete automatic tables just in case
        self.channel.delete_quick()
        self.ephys.delete_quick()
        self.trial.Condition.delete_quick()
        self.trial.delete_quick()
        self.experiment.delete_quick()

    def test_populate(self):
        # test simple populate
        assert_true(self.subject, "root tables are empty")
        assert_false(self.experiment, "table already filled?")
        self.experiment.populate()
        assert_true(
            len(self.experiment)
            == len(self.subject) * self.experiment.fake_experiments_per_subject
        )

        # test restricted populate
        assert_false(self.trial, "table already filled?")
        restriction = self.subject.proj(animal="subject_id").fetch("KEY")[0]
        d = self.trial.connection.dependencies
        d.load()
        self.trial.populate(restriction)
        assert_true(self.trial, "table was not populated")
        key_source = self.trial.key_source
        assert_equal(len(key_source & self.trial), len(key_source & restriction))
        assert_equal(len(key_source - self.trial), len(key_source - restriction))

        # test subtable populate
        assert_false(self.ephys)
        assert_false(self.channel)
        self.ephys.populate()
        assert_true(self.ephys)
        assert_true(self.channel)

    def test_populate_with_success_count(self):
        # test simple populate
        assert_true(self.subject, "root tables are empty")
        assert_false(self.experiment, "table already filled?")
        ret = self.experiment.populate()
        success_count = ret["success_count"]
        assert_equal(len(self.experiment.key_source & self.experiment), success_count)

        # test restricted populate
        assert_false(self.trial, "table already filled?")
        restriction = self.subject.proj(animal="subject_id").fetch("KEY")[0]
        d = self.trial.connection.dependencies
        d.load()
        ret = self.trial.populate(restriction, suppress_errors=True)
        success_count = ret["success_count"]
        assert_equal(len(self.trial.key_source & self.trial), success_count)

    def test_populate_exclude_error_and_ignore_jobs(self):
        # test simple populate
        assert_true(self.subject, "root tables are empty")
        assert_false(self.experiment, "table already filled?")

        keys = self.experiment.key_source.fetch("KEY", limit=2)
        for idx, key in enumerate(keys):
            if idx == 0:
                schema.schema.jobs.ignore(self.experiment.table_name, key)
            else:
                schema.schema.jobs.error(self.experiment.table_name, key, "")

        self.experiment.populate(reserve_jobs=True)
        assert_equal(
            len(self.experiment.key_source & self.experiment),
            len(self.experiment.key_source) - 2,
        )

    def test_allow_direct_insert(self):
        assert_true(self.subject, "root tables are empty")
        key = self.subject.fetch("KEY", limit=1)[0]
        key["experiment_id"] = 1000
        key["experiment_date"] = "2018-10-30"
        self.experiment.insert1(key, allow_direct_insert=True)

    def test_multi_processing(self):
        assert self.subject, "root tables are empty"
        assert not self.experiment, "table already filled?"
        self.experiment.populate(processes=2)
        assert (
            len(self.experiment)
            == len(self.subject) * self.experiment.fake_experiments_per_subject
        )

    def test_max_multi_processing(self):
        assert self.subject, "root tables are empty"
        assert not self.experiment, "table already filled?"
        self.experiment.populate(processes=None)
        assert (
            len(self.experiment)
            == len(self.subject) * self.experiment.fake_experiments_per_subject
        )

    @raises(DataJointError)
    def test_allow_insert(self):
        assert_true(self.subject, "root tables are empty")
        key = self.subject.fetch("KEY")[0]
        key["experiment_id"] = 1001
        key["experiment_date"] = "2018-10-30"
        self.experiment.insert1(key)

    def test_load_dependencies(self):
        schema = dj.Schema(f"{PREFIX}_load_dependencies_populate")

        @schema
        class ImageSource(dj.Lookup):
            definition = """
            image_source_id: int
            """
            contents = [(0,)]

        @schema
        class Image(dj.Imported):
            definition = """
            -> ImageSource
            ---
            image_data: longblob
            """

            def make(self, key):
                self.insert1(dict(key, image_data=dict()))

        Image.populate()

        @schema
        class Crop(dj.Computed):
            definition = """
            -> Image
            ---
            crop_image: longblob
            """

            def make(self, key):
                self.insert1(dict(key, crop_image=dict()))

        Crop.populate()
