from nose.tools import assert_true, assert_false
from . import schema


subjects = schema.Subject()


def test_reserve_job():
    assert_true(subjects)
    table_name = 'fake_table'
    # reserve jobs
    for key in subjects.fetch.keys():
        assert_true(schema.schema.jobs.reserve(table_name, key),
                    'failed to reserve a job')
    # refuse jobs
    for key in subjects.fetch.keys():
        assert_false(schema.schema.jobs.reserve(table_name, key),
                     'failed to respect reservation')
    # complete jobs
    for key in subjects.fetch.keys():
        schema.schema.jobs.complete(table_name, key)
    assert_false(schema.schema.jobs,
                 'failed to free jobs')
    # reserve jobs again
    for key in subjects.fetch.keys():
        assert_true(schema.schema.jobs.reserve(table_name, key),
                    'failed to reserve new jobs')
    # finish with error
    for key in subjects.fetch.keys():
        schema.schema.jobs.error(table_name, key, "error message")
    # refuse jobs with errors
    for key in subjects.fetch.keys():
        assert_false(schema.schema.jobs.reserve(table_name, key),
                     'failed to ignore error jobs')
    # clear error jobs
    (schema.schema.jobs & dict(status="error")).delete()
    assert_false(schema.schema.jobs,
                 'failed to clear error jobs')
