"""Tests for per-table Job management (AutoPopulate 2.0)."""

import random
import string

import datajoint as dj
from datajoint.jobs import ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX

from tests import schema


def test_reserve_job(clean_jobs, subject, experiment):
    """Test job reservation, completion, and error workflows."""
    assert subject

    # Refresh jobs to create pending entries
    experiment.jobs.refresh()
    pending_count = len(experiment.jobs.pending)
    assert pending_count > 0, "no pending jobs created"

    # Reserve all pending jobs
    keys = experiment.jobs.pending.keys()
    for key in keys:
        assert experiment.jobs.reserve(key), "failed to reserve a job"

    # Try to reserve already-reserved jobs - should fail
    for key in keys:
        assert not experiment.jobs.reserve(key), "failed to respect reservation"

    # Complete jobs
    for key in keys:
        experiment.jobs.complete(key)

    # Check jobs are completed (or deleted if keep_completed=False)
    if dj.config.jobs.keep_completed:
        assert len(experiment.jobs.completed) == len(keys)
    else:
        assert len(experiment.jobs) == 0, "failed to free jobs"

    # Refresh again to create new pending jobs
    experiment.jobs.refresh()
    keys = experiment.jobs.pending.keys()

    # Reserve and mark as error
    for key in keys:
        experiment.jobs.reserve(key)
        experiment.jobs.error(key, "error message")

    # Try to reserve error jobs - should fail
    for key in keys:
        assert not experiment.jobs.reserve(key), "failed to ignore error jobs"

    # Clear error jobs
    experiment.jobs.errors.delete()
    assert len(experiment.jobs) == 0, "failed to clear error jobs"


def test_job_status_filters(clean_jobs, subject, experiment):
    """Test job status filter properties."""
    # Refresh to create pending jobs
    experiment.jobs.refresh()

    # All should be pending
    total = len(experiment.jobs)
    assert total > 0
    assert len(experiment.jobs.pending) == total
    assert len(experiment.jobs.reserved) == 0
    assert len(experiment.jobs.errors) == 0

    # Reserve some jobs
    keys = experiment.jobs.pending.keys(limit=2)
    for key in keys:
        experiment.jobs.reserve(key)

    assert len(experiment.jobs.reserved) == 2

    # Mark one as error
    experiment.jobs.error(keys[0], "test error")
    assert len(experiment.jobs.errors) == 1


def test_sigint(clean_jobs, schema_any):
    """Test that KeyboardInterrupt is recorded as error."""
    sig_int_table = schema.SigIntTable()
    try:
        sig_int_table.populate(reserve_jobs=True)
    except KeyboardInterrupt:
        pass

    assert len(sig_int_table.jobs.errors) > 0, "SigInt job error not recorded"
    status, error_message = sig_int_table.jobs.errors.fetch1("status", "error_message")
    assert status == "error"
    assert "KeyboardInterrupt" in error_message


def test_sigterm(clean_jobs, schema_any):
    """Test that SystemExit is recorded as error."""
    sig_term_table = schema.SigTermTable()
    try:
        sig_term_table.populate(reserve_jobs=True)
    except SystemExit:
        pass

    assert len(sig_term_table.jobs.errors) > 0, "SigTerm job error not recorded"
    status, error_message = sig_term_table.jobs.errors.fetch1("status", "error_message")
    assert status == "error"
    assert "SIGTERM" in error_message or "SystemExit" in error_message


def test_suppress_dj_errors(clean_jobs, schema_any):
    """Test that DataJoint errors are suppressible."""
    error_class = schema.ErrorClass()
    error_class.populate(reserve_jobs=True, suppress_errors=True)
    assert len(schema.DjExceptionName()) == len(error_class.jobs.errors) > 0


def test_long_error_message(clean_jobs, subject, experiment):
    """Test that long error messages are truncated."""
    # Create long and short error messages
    long_error_message = "".join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100))
    short_error_message = "".join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH // 2))

    # Refresh to create pending jobs
    experiment.jobs.refresh()
    key = experiment.jobs.pending.keys(limit=1)[0]

    # Test long error message truncation
    experiment.jobs.reserve(key)
    experiment.jobs.error(key, long_error_message)
    error_message = experiment.jobs.errors.fetch1("error_message")
    assert len(error_message) == ERROR_MESSAGE_LENGTH, "error message is longer than max allowed"
    assert error_message.endswith(TRUNCATION_APPENDIX), "appropriate ending missing for truncated error message"
    experiment.jobs.delete()

    # Refresh and test short error message (not truncated)
    experiment.jobs.refresh()
    key = experiment.jobs.pending.keys(limit=1)[0]
    experiment.jobs.reserve(key)
    experiment.jobs.error(key, short_error_message)
    error_message = experiment.jobs.errors.fetch1("error_message")
    assert error_message == short_error_message, "error messages do not agree"
    assert not error_message.endswith(TRUNCATION_APPENDIX), "error message should not be truncated"


def test_long_error_stack(clean_jobs, subject, experiment):
    """Test that long error stacks are stored correctly."""
    # Create long error stack
    STACK_SIZE = 89942  # Does not fit into small blob (should be 64k, but found to be higher)
    long_error_stack = "".join(random.choice(string.ascii_letters) for _ in range(STACK_SIZE))

    # Refresh to create pending jobs
    experiment.jobs.refresh()
    key = experiment.jobs.pending.keys(limit=1)[0]

    # Test long error stack
    experiment.jobs.reserve(key)
    experiment.jobs.error(key, "error message", long_error_stack)
    error_stack = experiment.jobs.errors.fetch1("error_stack")
    assert error_stack == long_error_stack, "error stacks do not agree"


def test_populate_reserve_jobs_with_keep_completed(clean_jobs, subject, experiment):
    """Test populate(reserve_jobs=True) with keep_completed=True.

    Regression test for https://github.com/datajoint/datajoint-python/issues/1379
    The bug was that the `-` operator in jobs.refresh() didn't pass semantic_check=False,
    causing a DataJointError about different lineages when keep_completed=True.
    """
    # Clear experiment data to ensure there's work to do
    experiment.delete()

    with dj.config.override(jobs={"keep_completed": True, "add_job_metadata": True}):
        # Should not raise DataJointError about semantic matching
        experiment.populate(reserve_jobs=True)

        # Verify jobs completed successfully
        assert len(experiment) > 0, "No data was populated"
        assert len(experiment.jobs.errors) == 0, "Unexpected errors during populate"

        # With keep_completed=True, completed jobs should be retained
        assert len(experiment.jobs.completed) > 0, "Completed jobs not retained"


def test_jobs_refresh_with_keep_completed(clean_jobs, subject, experiment):
    """Test that jobs.refresh() works with keep_completed=True.

    Regression test for https://github.com/datajoint/datajoint-python/issues/1379
    """
    # Clear experiment data and jobs
    experiment.delete()
    experiment.jobs.delete()

    with dj.config.override(jobs={"keep_completed": True, "add_job_metadata": True}):
        # Refresh should create pending jobs without semantic matching error
        experiment.jobs.refresh()
        pending_before = len(experiment.jobs.pending)
        assert pending_before > 0, "No pending jobs created"

        # Manually reserve and complete a job
        key = experiment.jobs.pending.keys(limit=1)[0]
        experiment.jobs.reserve(key)
        experiment.jobs.complete(key)

        # Job should now be completed
        assert len(experiment.jobs.completed) == 1, "Job not marked as completed"

        # Calling refresh again should not raise semantic matching error
        experiment.jobs.refresh()  # This was failing before the fix
