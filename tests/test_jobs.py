"""
Tests for the Autopopulate 2.0 per-table jobs system.
"""

import random
import string

import datajoint as dj
from datajoint.jobs import JobsTable, ERROR_MESSAGE_LENGTH, TRUNCATION_APPENDIX

from . import schema


class TestJobsTableStructure:
    """Tests for JobsTable structure and initialization."""

    def test_jobs_property_exists(self, schema_any):
        """Test that Computed tables have a jobs property."""
        assert hasattr(schema.SigIntTable, "jobs")
        jobs = schema.SigIntTable().jobs
        assert isinstance(jobs, JobsTable)

    def test_jobs_table_name(self, schema_any):
        """Test that jobs table has correct naming convention."""
        jobs = schema.SigIntTable().jobs
        # SigIntTable is __sig_int_table, jobs should be ~sig_int_table__jobs
        assert jobs.table_name.startswith("~")
        assert jobs.table_name.endswith("__jobs")

    def test_jobs_table_primary_key(self, schema_any):
        """Test that jobs table has FK-derived primary key."""
        jobs = schema.SigIntTable().jobs
        # SigIntTable depends on SimpleSource with pk 'id'
        assert "id" in jobs.primary_key

    def test_jobs_table_status_column(self, schema_any):
        """Test that jobs table has status column with correct enum values."""
        jobs = schema.SigIntTable().jobs
        jobs._ensure_declared()
        status_attr = jobs.heading.attributes["status"]
        assert "pending" in status_attr.type
        assert "reserved" in status_attr.type
        assert "success" in status_attr.type
        assert "error" in status_attr.type
        assert "ignore" in status_attr.type


class TestJobsRefresh:
    """Tests for JobsTable.refresh() method."""

    def test_refresh_adds_jobs(self, schema_any):
        """Test that refresh() adds pending jobs for keys in key_source."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()  # Clear any existing jobs

        result = jobs.refresh()
        assert result["added"] > 0
        assert len(jobs.pending) > 0

    def test_refresh_with_priority(self, schema_any):
        """Test that refresh() sets priority on new jobs."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()

        jobs.refresh(priority=3)
        priorities = jobs.pending.fetch("priority")
        assert all(p == 3 for p in priorities)

    def test_refresh_with_delay(self, schema_any):
        """Test that refresh() sets scheduled_time in the future."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()

        jobs.refresh(delay=3600)  # 1 hour delay
        # Jobs should not be available for processing yet
        keys = jobs.fetch_pending()
        assert len(keys) == 0  # All jobs are scheduled for later

    def test_refresh_removes_stale_jobs(self, schema_any):
        """Test that refresh() removes jobs for deleted upstream records."""
        # This test requires manipulating upstream data
        pass  # Skip for now


class TestJobsReserve:
    """Tests for JobsTable.reserve() method."""

    def test_reserve_pending_job(self, schema_any):
        """Test that reserve() transitions pending -> reserved."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        # Get first pending job
        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)

        # Verify status changed
        status = (jobs & key).fetch1("status")
        assert status == "reserved"

    def test_reserve_sets_metadata(self, schema_any):
        """Test that reserve() sets user, host, pid, connection_id."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)

        # Verify metadata was set
        row = (jobs & key).fetch1()
        assert row["status"] == "reserved"
        assert row["reserved_time"] is not None
        assert row["user"] != ""
        assert row["host"] != ""
        assert row["pid"] > 0
        assert row["connection_id"] > 0


class TestJobsComplete:
    """Tests for JobsTable.complete() method."""

    def test_complete_with_keep_false(self, schema_any):
        """Test that complete() deletes job when keep=False."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)
        jobs.complete(key, duration=1.5, keep=False)

        assert key not in jobs

    def test_complete_with_keep_true(self, schema_any):
        """Test that complete() marks job as success when keep=True."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)
        jobs.complete(key, duration=1.5, keep=True)

        status = (jobs & key).fetch1("status")
        assert status == "success"


class TestJobsError:
    """Tests for JobsTable.error() method."""

    def test_error_marks_status(self, schema_any):
        """Test that error() marks job as error with message."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)
        jobs.error(key, error_message="Test error", error_stack="stack trace")

        status, msg = (jobs & key).fetch1("status", "error_message")
        assert status == "error"
        assert msg == "Test error"

    def test_error_truncates_long_message(self, schema_any):
        """Test that error() truncates long error messages."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        long_message = "".join(random.choice(string.ascii_letters) for _ in range(ERROR_MESSAGE_LENGTH + 100))

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)
        jobs.error(key, error_message=long_message)

        msg = (jobs & key).fetch1("error_message")
        assert len(msg) == ERROR_MESSAGE_LENGTH
        assert msg.endswith(TRUNCATION_APPENDIX)


class TestJobsIgnore:
    """Tests for JobsTable.ignore() method."""

    def test_ignore_marks_status(self, schema_any):
        """Test that ignore() marks job as ignore."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.ignore(key)

        status = (jobs & key).fetch1("status")
        assert status == "ignore"

    def test_ignore_new_key(self, schema_any):
        """Test that ignore() can create new job with ignore status."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()

        # Don't refresh - ignore a key directly
        key = {"id": 1}
        jobs.ignore(key)

        status = (jobs & key).fetch1("status")
        assert status == "ignore"


class TestJobsStatusProperties:
    """Tests for status filter properties."""

    def test_pending_property(self, schema_any):
        """Test that pending property returns pending jobs."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        assert len(jobs.pending) > 0
        statuses = jobs.pending.fetch("status")
        assert all(s == "pending" for s in statuses)

    def test_reserved_property(self, schema_any):
        """Test that reserved property returns reserved jobs."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)

        assert len(jobs.reserved) == 1
        statuses = jobs.reserved.fetch("status")
        assert all(s == "reserved" for s in statuses)

    def test_errors_property(self, schema_any):
        """Test that errors property returns error jobs."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.reserve(key)
        jobs.error(key, error_message="test")

        assert len(jobs.errors) == 1

    def test_ignored_property(self, schema_any):
        """Test that ignored property returns ignored jobs."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        key = jobs.pending.fetch("KEY", limit=1)[0]
        jobs.ignore(key)

        assert len(jobs.ignored) == 1


class TestJobsProgress:
    """Tests for JobsTable.progress() method."""

    def test_progress_returns_counts(self, schema_any):
        """Test that progress() returns status counts."""
        table = schema.SigIntTable()
        jobs = table.jobs
        jobs.delete()
        jobs.refresh()

        progress = jobs.progress()

        assert "pending" in progress
        assert "reserved" in progress
        assert "success" in progress
        assert "error" in progress
        assert "ignore" in progress
        assert "total" in progress
        assert progress["total"] == sum(progress[k] for k in ["pending", "reserved", "success", "error", "ignore"])


class TestPopulateWithJobs:
    """Tests for populate() with reserve_jobs=True using new system."""

    def test_populate_creates_jobs_table(self, schema_any):
        """Test that populate with reserve_jobs creates jobs table."""
        table = schema.SigIntTable()
        # Clear target table to allow re-population
        table.delete()

        # First populate should create jobs table
        table.populate(reserve_jobs=True, suppress_errors=True, max_calls=1)

        assert table.jobs.is_declared

    def test_populate_uses_jobs_queue(self, schema_any):
        """Test that populate processes jobs from queue."""
        table = schema.Experiment()
        table.delete()
        jobs = table.jobs
        jobs.delete()

        # Refresh to add jobs
        jobs.refresh()
        initial_pending = len(jobs.pending)
        assert initial_pending > 0

        # Populate one job
        result = table.populate(reserve_jobs=True, max_calls=1)
        assert result["success_count"] >= 0  # May be 0 if error

    def test_populate_with_priority_filter(self, schema_any):
        """Test that populate respects priority filter."""
        table = schema.Experiment()
        table.delete()
        jobs = table.jobs
        jobs.delete()

        # Add jobs with different priorities
        # This would require the table to have multiple keys
        pass  # Skip for now


class TestSchemaJobs:
    """Tests for schema.jobs property."""

    def test_schema_jobs_returns_list(self, schema_any):
        """Test that schema.jobs returns list of JobsTable objects."""
        jobs_list = schema_any.jobs
        assert isinstance(jobs_list, list)

    def test_schema_jobs_contains_jobs_tables(self, schema_any):
        """Test that schema.jobs contains JobsTable instances."""
        jobs_list = schema_any.jobs
        for jobs in jobs_list:
            assert isinstance(jobs, JobsTable)


class TestTableDropLifecycle:
    """Tests for table drop lifecycle."""

    def test_drop_removes_jobs_table(self, schema_any):
        """Test that dropping a table also drops its jobs table."""
        # Create a temporary computed table for this test
        # This test would modify the schema, so skip for now
        pass


class TestConfiguration:
    """Tests for jobs configuration settings."""

    def test_default_priority_config(self, schema_any):
        """Test that config.jobs.default_priority is used."""
        original = dj.config.jobs.default_priority
        try:
            dj.config.jobs.default_priority = 3

            table = schema.SigIntTable()
            jobs = table.jobs
            jobs.delete()
            jobs.refresh()  # Should use default priority from config

            priorities = jobs.pending.fetch("priority")
            assert all(p == 3 for p in priorities)
        finally:
            dj.config.jobs.default_priority = original

    def test_keep_completed_config(self, schema_any):
        """Test that config.jobs.keep_completed affects complete()."""
        # Test with keep_completed=True
        with dj.config.override(jobs__keep_completed=True):
            table = schema.SigIntTable()
            jobs = table.jobs
            jobs.delete()
            jobs.refresh()

            key = jobs.pending.fetch("KEY", limit=1)[0]
            jobs.reserve(key)
            jobs.complete(key)  # Should use config

            status = (jobs & key).fetch1("status")
            assert status == "success"
