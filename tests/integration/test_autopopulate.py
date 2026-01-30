import platform
import pytest

import datajoint as dj
from datajoint import DataJointError


def test_populate(clean_autopopulate, trial, subject, experiment, ephys, channel):
    # test simple populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    experiment.populate()
    assert len(experiment) == len(subject) * experiment.fake_experiments_per_subject

    # test restricted populate
    assert not trial, "table already filled?"
    restriction = subject.proj(animal="subject_id").keys()[0]
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


def test_populate_with_success_count(clean_autopopulate, subject, experiment, trial):
    # test simple populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    ret = experiment.populate()
    success_count = ret["success_count"]
    assert len(experiment.key_source & experiment) == success_count

    # test restricted populate
    assert not trial, "table already filled?"
    restriction = subject.proj(animal="subject_id").keys()[0]
    d = trial.connection.dependencies
    d.load()
    ret = trial.populate(restriction, suppress_errors=True)
    success_count = ret["success_count"]
    assert len(trial.key_source & trial) == success_count


def test_populate_max_calls(clean_autopopulate, subject, experiment, trial):
    # test populate with max_calls limit
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    n = 3
    total_keys = len(experiment.key_source)
    assert total_keys > n
    ret = experiment.populate(max_calls=n)
    assert n == ret["success_count"]


def test_populate_exclude_error_and_ignore_jobs(clean_autopopulate, subject, experiment):
    # test that error and ignore jobs are excluded from populate
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"

    # Refresh jobs to create pending entries
    # Use delay=-1 to ensure jobs are immediately schedulable (avoids race condition with CURRENT_TIMESTAMP(3))
    experiment.jobs.refresh(delay=-1)

    keys = experiment.jobs.pending.keys(limit=2)
    for idx, key in enumerate(keys):
        if idx == 0:
            experiment.jobs.ignore(key)
        else:
            # Create an error job by first reserving then setting error
            experiment.jobs.reserve(key)
            experiment.jobs.error(key, "test error")

    # Populate should skip error and ignore jobs
    experiment.populate(reserve_jobs=True, refresh=False)
    assert len(experiment.key_source & experiment) == len(experiment.key_source) - 2


def test_allow_direct_insert(clean_autopopulate, subject, experiment):
    assert subject, "root tables are empty"
    key = subject.keys(limit=1)[0]
    key["experiment_id"] = 1000
    key["experiment_date"] = "2018-10-30"
    experiment.insert1(key, allow_direct_insert=True)


@pytest.mark.skipif(
    platform.system() == "Darwin",
    reason="multiprocessing with spawn method (macOS default) cannot pickle thread locks",
)
@pytest.mark.parametrize("processes", [None, 2])
def test_multi_processing(clean_autopopulate, subject, experiment, processes):
    assert subject, "root tables are empty"
    assert not experiment, "table already filled?"
    experiment.populate(processes=processes)
    assert len(experiment) == len(subject) * experiment.fake_experiments_per_subject


def test_allow_insert(clean_autopopulate, subject, experiment):
    assert subject, "root tables are empty"
    key = subject.keys()[0]
    key["experiment_id"] = 1001
    key["experiment_date"] = "2018-10-30"
    with pytest.raises(DataJointError):
        experiment.insert1(key)


def test_load_dependencies(prefix, connection_test):
    schema = dj.Schema(f"{prefix}_load_dependencies_populate", connection=connection_test)

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
        image_data: <blob>
        """

        def make(self, key):
            self.insert1(dict(key, image_data=dict()))

    Image.populate()

    @schema
    class Crop(dj.Computed):
        definition = """
        -> Image
        ---
        crop_image: <blob>
        """

        def make(self, key):
            self.insert1(dict(key, crop_image=dict()))

    Crop.populate()


def test_make_kwargs_regular(prefix, connection_test):
    """Test that make_kwargs are passed to regular make method."""
    schema = dj.Schema(f"{prefix}_make_kwargs_regular", connection=connection_test)

    @schema
    class Source(dj.Lookup):
        definition = """
        source_id: int
        """
        contents = [(1,), (2,)]

    @schema
    class Computed(dj.Computed):
        definition = """
        -> Source
        ---
        multiplier: int
        result: int
        """

        def make(self, key, multiplier=1):
            self.insert1(dict(key, multiplier=multiplier, result=key["source_id"] * multiplier))

    # Test without make_kwargs
    Computed.populate(Source & "source_id = 1")
    assert (Computed & "source_id = 1").fetch1("result") == 1

    # Test with make_kwargs
    Computed.populate(Source & "source_id = 2", make_kwargs={"multiplier": 10})
    assert (Computed & "source_id = 2").fetch1("multiplier") == 10
    assert (Computed & "source_id = 2").fetch1("result") == 20


def test_make_kwargs_tripartite(prefix, connection_test):
    """Test that make_kwargs are passed to make_fetch in tripartite pattern (issue #1350)."""
    schema = dj.Schema(f"{prefix}_make_kwargs_tripartite", connection=connection_test)

    @schema
    class Source(dj.Lookup):
        definition = """
        source_id: int
        ---
        value: int
        """
        contents = [(1, 100), (2, 200)]

    @schema
    class TripartiteComputed(dj.Computed):
        definition = """
        -> Source
        ---
        scale: int
        result: int
        """

        def make_fetch(self, key, scale=1):
            """Fetch data with optional scale parameter."""
            value = (Source & key).fetch1("value")
            return (value, scale)

        def make_compute(self, key, value, scale):
            """Compute result using fetched value and scale."""
            return (value * scale, scale)

        def make_insert(self, key, result, scale):
            """Insert computed result."""
            self.insert1(dict(key, scale=scale, result=result))

    # Test without make_kwargs (scale defaults to 1)
    TripartiteComputed.populate(Source & "source_id = 1")
    row = (TripartiteComputed & "source_id = 1").fetch1()
    assert row["scale"] == 1
    assert row["result"] == 100  # 100 * 1

    # Test with make_kwargs (scale = 5)
    TripartiteComputed.populate(Source & "source_id = 2", make_kwargs={"scale": 5})
    row = (TripartiteComputed & "source_id = 2").fetch1()
    assert row["scale"] == 5
    assert row["result"] == 1000  # 200 * 5
