import pytest
from . import schema
from datajoint.jobs import ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX
import random
import string
import datajoint as dj


def test_reserve_job(subject, schema_any):
    assert subject
    table_name = "fake_table"

    # reserve jobs
    for key in subject.fetch("KEY"):
        assert schema_any.jobs.reserve(table_name, key), "failed to reserve a job"

    # refuse jobs
    for key in subject.fetch("KEY"):
        assert not schema_any.jobs.reserve(
            table_name, key
        ), "failed to respect reservation"

    # complete jobs
    for key in subject.fetch("KEY"):
        schema_any.jobs.complete(table_name, key)
    assert not schema_any.jobs, "failed to free jobs"

    # reserve jobs again
    for key in subject.fetch("KEY"):
        assert schema_any.jobs.reserve(table_name, key), "failed to reserve new jobs"

    # finish with error
    for key in subject.fetch("KEY"):
        schema_any.jobs.error(table_name, key, "error message")

    # refuse jobs with errors
    for key in subject.fetch("KEY"):
        assert not schema_any.jobs.reserve(
            table_name, key
        ), "failed to ignore error jobs"

    # clear error jobs
    (schema_any.jobs & dict(status="error")).delete()
    assert not schema_any.jobs, "failed to clear error jobs"


def test_restrictions(schema_any):
    jobs = schema_any.jobs
    jobs.delete()
    jobs.reserve("a", {"key": "a1"})
    jobs.reserve("a", {"key": "a2"})
    jobs.reserve("b", {"key": "b1"})
    jobs.error("a", {"key": "a2"}, "error")
    jobs.error("b", {"key": "b1"}, "error")

    assert len(jobs & {"table_name": "a"}) == 2
    assert len(jobs & {"status": "error"}) == 2
    assert len(jobs & {"table_name": "a", "status": "error"}) == 1
    jobs.delete()


def test_sigint(schema_any):
    try:
        schema.SigIntTable().populate(reserve_jobs=True)
    except KeyboardInterrupt:
        pass

    assert len(schema_any.jobs.fetch()), "SigInt jobs table is empty"
    status, error_message = schema_any.jobs.fetch1("status", "error_message")
    assert status == "error"
    assert error_message == "KeyboardInterrupt"


def test_sigterm(schema_any):
    try:
        schema.SigTermTable().populate(reserve_jobs=True)
    except SystemExit:
        pass

    assert len(schema_any.jobs.fetch()), "SigTerm jobs table is empty"
    status, error_message = schema_any.jobs.fetch1("status", "error_message")
    assert status == "error"
    assert error_message == "SystemExit: SIGTERM received"


def test_suppress_dj_errors(schema_any):
    """test_suppress_dj_errors: dj errors suppressible w/o native py blobs"""
    with dj.config(enable_python_native_blobs=False):
        schema.ErrorClass.populate(reserve_jobs=True, suppress_errors=True)
    assert len(schema.DjExceptionName()) == len(schema_any.jobs) > 0


def test_long_error_message(subject, schema_any):
    # create long error message
    long_error_message = "".join(
        random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100)
    )
    short_error_message = "".join(
        random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH // 2)
    )
    assert subject
    table_name = "fake_table"

    key = subject.fetch("KEY", limit=1)[0]

    # test long error message
    schema_any.jobs.reserve(table_name, key)
    schema_any.jobs.error(table_name, key, long_error_message)
    error_message = schema_any.jobs.fetch1("error_message")
    assert (
        len(error_message) == ERROR_MESSAGE_LENGTH
    ), "error message is longer than max allowed"
    assert error_message.endswith(
        TRUNCATION_APPENDIX
    ), "appropriate ending missing for truncated error message"
    schema_any.jobs.delete()

    # test long error message
    schema_any.jobs.reserve(table_name, key)
    schema_any.jobs.error(table_name, key, short_error_message)
    error_message = schema_any.jobs.fetch1("error_message")
    assert error_message == short_error_message, "error messages do not agree"
    assert not error_message.endswith(
        TRUNCATION_APPENDIX
    ), "error message should not be truncated"
    schema_any.jobs.delete()


def test_long_error_stack(subject, schema_any):
    # create long error stack
    STACK_SIZE = (
        89942  # Does not fit into small blob (should be 64k, but found to be higher)
    )
    long_error_stack = "".join(
        random.choice(string.ascii_letters) for _ in range(STACK_SIZE)
    )
    assert subject
    table_name = "fake_table"

    key = subject.fetch("KEY", limit=1)[0]

    # test long error stack
    schema_any.jobs.reserve(table_name, key)
    schema_any.jobs.error(table_name, key, "error message", long_error_stack)
    error_stack = schema_any.jobs.fetch1("error_stack")
    assert error_stack == long_error_stack, "error stacks do not agree"
