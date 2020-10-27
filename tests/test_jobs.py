from nose.tools import assert_true, assert_false, assert_equals
from . import schema
from datajoint.jobs import ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX
import random
import string
import datajoint as dj
dj.config['enable_python_native_blobs'] = True

subjects = schema.Subject()


def test_reserve_job():

    schema.schema.jobs.delete()
    assert_true(subjects)
    table_name = 'fake_table'

    # reserve jobs
    for key in subjects.fetch('KEY'):
        assert_true(schema.schema.jobs.reserve(table_name, key), 'failed to reserve a job')

    # refuse jobs
    for key in subjects.fetch('KEY'):
        assert_false(schema.schema.jobs.reserve(table_name, key), 'failed to respect reservation')

    # complete jobs
    for key in subjects.fetch('KEY'):
        schema.schema.jobs.complete(table_name, key)
    assert_false(schema.schema.jobs, 'failed to free jobs')

    # reserve jobs again
    for key in subjects.fetch('KEY'):
        assert_true(schema.schema.jobs.reserve(table_name, key), 'failed to reserve new jobs')

    # finish with error
    for key in subjects.fetch('KEY'):
        schema.schema.jobs.error(table_name, key, "error message")

    # refuse jobs with errors
    for key in subjects.fetch('KEY'):
        assert_false(schema.schema.jobs.reserve(table_name, key), 'failed to ignore error jobs')

    # clear error jobs
    (schema.schema.jobs & dict(status="error")).delete()
    assert_false(schema.schema.jobs, 'failed to clear error jobs')


def test_restrictions():
    jobs = schema.schema.jobs
    jobs.delete()
    jobs.reserve('a', {'key': 'a1'})
    jobs.reserve('a', {'key': 'a2'})
    jobs.reserve('b', {'key': 'b1'})
    jobs.error('a', {'key': 'a2'}, 'error')
    jobs.error('b', {'key': 'b1'}, 'error')

    assert_true(len(jobs & {'table_name': "a"}) == 2)
    assert_true(len(jobs & {'status': "error"}) == 2)
    assert_true(len(jobs & {'table_name': "a", 'status': "error"}) == 1)
    jobs.delete()


def test_sigint():
    # clear out job table
    schema.schema.jobs.delete()
    try:
        schema.SigIntTable().populate(reserve_jobs=True)
    except KeyboardInterrupt:
        pass

    status, error_message = schema.schema.jobs.fetch1('status', 'error_message')
    assert_equals(status, 'error')
    assert_equals(error_message, 'KeyboardInterrupt')
    schema.schema.jobs.delete()


def test_sigterm():
    # clear out job table
    schema.schema.jobs.delete()
    try:
        schema.SigTermTable().populate(reserve_jobs=True)
    except SystemExit:
        pass

    status, error_message = schema.schema.jobs.fetch1('status', 'error_message')
    assert_equals(status, 'error')
    assert_equals(error_message, 'SystemExit: SIGTERM received')
    schema.schema.jobs.delete()


def test_suppress_dj_errors():
    """ test_suppress_dj_errors: dj errors suppressible w/o native py blobs """
    schema.schema.jobs.delete()
    with dj.config(enable_python_native_blobs=False):
        schema.ErrorClass.populate(reserve_jobs=True, suppress_errors=True)
    assert_true(len(schema.DjExceptionName()) == len(schema.schema.jobs) > 0)


def test_long_error_message():
    # clear out jobs table
    schema.schema.jobs.delete()

    # create long error message
    long_error_message = ''.join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100))
    short_error_message = ''.join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH // 2))
    assert_true(subjects)
    table_name = 'fake_table'

    key = subjects.fetch('KEY')[0]

    # test long error message
    schema.schema.jobs.reserve(table_name, key)
    schema.schema.jobs.error(table_name, key, long_error_message)
    error_message = schema.schema.jobs.fetch1('error_message')
    assert_true(len(error_message) == ERROR_MESSAGE_LENGTH, 'error message is longer than max allowed')
    assert_true(error_message.endswith(TRUNCATION_APPENDIX), 'appropriate ending missing for truncated error message')
    schema.schema.jobs.delete()

    # test long error message
    schema.schema.jobs.reserve(table_name, key)
    schema.schema.jobs.error(table_name, key, short_error_message)
    error_message = schema.schema.jobs.fetch1('error_message')
    assert_true(error_message == short_error_message, 'error messages do not agree')
    assert_false(error_message.endswith(TRUNCATION_APPENDIX), 'error message should not be truncated')
    schema.schema.jobs.delete()
