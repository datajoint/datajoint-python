import pytest
from datajoint import DataJointError
import datajoint as dj
import pymysql
from . import schema


def test_populate(trial, subject, experiment, ephys, channel):
    # test simple populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    experiment.populate()
    assert len(experiment) == len(subject) * experiment.fake_experiments_per_subject

    # test restricted populate
    assert not trial, "table already filled?"
    restriction = subject.proj(animal="subject_id").fetch("KEY")[0]
    d = trial.connection.dependencies
    d.load()
    trial.populate(restriction)
    assert trial, "table was not populated"
    key_source = trial.key_source
    assert len(key_source & trial) == len(key_source & restriction)
    assert len(key_source - trial) == len(key_source - restriction)

    # test subtable populate
    assert not ephys
    assert not channel
    ephys.populate()
    assert ephys
    assert channel


def test_populate_with_success_count(subject, experiment, trial):
    # test simple populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    ret = experiment.populate()
    success_count = ret["success_count"]
    assert len(experiment.key_source & experiment) == success_count

    # test restricted populate
    assert not trial, "table already filled?"
    restriction = subject.proj(animal="subject_id").fetch("KEY")[0]
    d = trial.connection.dependencies
    d.load()
    ret = trial.populate(restriction, suppress_errors=True)
    success_count = ret["success_count"]
    assert len(trial.key_source & trial) == success_count


def test_populate_exclude_error_and_ignore_jobs(schema_any, subject, experiment):
    # test simple populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"

    keys = experiment.key_source.fetch("KEY", limit=2)
    for idx, key in enumerate(keys):
        if idx == 0:
            schema_any.jobs.ignore(experiment.table_name, key)
        else:
            schema_any.jobs.error(experiment.table_name, key, "")

    experiment.populate(reserve_jobs=True)
    assert len(experiment.key_source & experiment) == len(experiment.key_source) - 2


def test_allow_direct_insert(subject, experiment):
    assert subject, "root tables are empty"
    key = subject.fetch("KEY", limit=1)[0]
    key["experiment_id"] = 1000
    key["experiment_date"] = "2018-10-30"
    experiment.insert1(key, allow_direct_insert=True)


@pytest.mark.parametrize("processes", [None, 2])
def test_multi_processing(subject, experiment, processes):
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    experiment.populate(processes=None)
    assert len(experiment) == len(subject) * experiment.fake_experiments_per_subject


def test_allow_insert(subject, experiment):
    assert subject, "root tables are empty"
    key = subject.fetch("KEY")[0]
    key["experiment_id"] = 1001
    key["experiment_date"] = "2018-10-30"
    with pytest.raises(DataJointError):
        experiment.insert1(key)


def test_load_dependencies(prefix):
    schema = dj.Schema(f"{prefix}_load_dependencies_populate")

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
