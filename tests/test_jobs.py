from nose.tools import assert_true, assert_false
from . import schema
from datajoint.jobs import ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX
import random
import string


subjects = schema.Subject()


def test_reserve_job():
    # clean jobs table
    schema.schema.jobs.delete()

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

def test_long_error_message():
    # clear out jobs table
    schema.schema.jobs.delete()

    # create long error message
    long_error_message = ''.join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100))
    short_error_message = ''.join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH // 2))
    assert_true(subjects)
    table_name = 'fake_table'

    key = subjects.fetch.keys()[0]

    # test long error message
    schema.schema.jobs.reserve(table_name, key)
    schema.schema.jobs.error(table_name, key, long_error_message)
    error_message = schema.schema.jobs.fetch1['error_message']
    assert_true(len(error_message) == ERROR_MESSAGE_LENGTH, 'error message is longer than max allowed')
    assert_true(error_message.endswith(TRUNCATION_APPENDIX), 'appropriate ending missing for truncated error message')
    schema.schema.jobs.delete()

    # test long error message
    schema.schema.jobs.reserve(table_name, key)
    schema.schema.jobs.error(table_name, key, short_error_message)
    error_message = schema.schema.jobs.fetch1['error_message']
    assert_true(error_message == short_error_message, 'error messages do not agree')
    assert_false(error_message.endswith(TRUNCATION_APPENDIX), 'error message should not be truncated')
    schema.schema.jobs.delete()