"""The retrofit path for `_job_*` must reach the same column a declaration does.

`migrate.add_job_metadata_columns` writes the three hidden columns onto tables
declared before `config.jobs.add_job_metadata` was on.  It used to build the
SQL by hand -- backtick-quoted and typed `datetime(3)` -- which is a syntax
error on PostgreSQL and the wrong type there besides.  It now compiles the same
`JOB_METADATA_DEFINITION` lines `declare()` uses, so what these tests pin is
that the two paths converge: a migrated table and a declared one are
indistinguishable in the catalog.
"""

import pytest

import datajoint as dj
from datajoint.migrate import add_job_metadata_columns

JOB_COLUMNS = ("_job_start_time", "_job_duration", "_job_version")


def _column_types(table):
    """Backend type and recovered `original_type` per hidden column, from the catalog."""
    heading = table().heading
    heading._init_from_database()
    return {
        name: (heading._attributes[name].type, heading._attributes[name].original_type)
        for name in JOB_COLUMNS
        if name in heading._attributes
    }


@pytest.fixture
def declared_and_migrated(connection_by_backend, backend, prefix):
    """Two Computed tables with the same definition: one declared with the
    columns, one declared without them and then migrated."""
    original = dj.config.jobs.add_job_metadata

    dj.config.jobs.add_job_metadata = False
    off = dj.Schema(f"{prefix}_jobmeta_off_{backend}", connection=connection_by_backend)

    @off
    class Source(dj.Lookup):
        definition = """
        source_id : int16
        ---
        value : float32
        """
        contents = [(1, 1.0)]

    @off
    class Legacy(dj.Computed):
        definition = """
        -> Source
        ---
        result : float32
        """

        def make(self, key):
            self.insert1({**key, "result": 2.0})

    dj.config.jobs.add_job_metadata = True
    on = dj.Schema(f"{prefix}_jobmeta_on_{backend}", connection=connection_by_backend)

    @on
    class Source2(dj.Lookup):
        definition = """
        source_id : int16
        ---
        value : float32
        """
        contents = [(1, 1.0)]

    @on
    class Fresh(dj.Computed):
        definition = """
        -> Source2
        ---
        result : float32
        """

        def make(self, key):
            self.insert1({**key, "result": 2.0})

    try:
        yield Legacy, Fresh
    finally:
        off.drop()
        on.drop()
        dj.config.jobs.add_job_metadata = original


def test_migrated_columns_match_declared_ones(declared_and_migrated):
    """The point of routing the migration through `compile_attribute`."""
    legacy, fresh = declared_and_migrated

    assert _column_types(legacy) == {}, "declared with metadata off, so nothing to compare yet"
    assert set(_column_types(fresh)) == set(JOB_COLUMNS)

    preview = add_job_metadata_columns(legacy, dry_run=True)
    assert preview["columns_added"] == 3
    assert preview["details"][0]["status"] == "pending"

    applied = add_job_metadata_columns(legacy, dry_run=False)
    assert applied["columns_added"] == 3

    # Identical type *and* `:type:` marker -- the marker is what carries
    # `original_type`, and on PostgreSQL it needs its own COMMENT ON statement.
    assert _column_types(legacy) == _column_types(fresh)


def test_migration_is_idempotent(declared_and_migrated):
    legacy, _ = declared_and_migrated
    add_job_metadata_columns(legacy, dry_run=False)
    again = add_job_metadata_columns(legacy, dry_run=False)
    assert again["columns_added"] == 0
    assert again["details"][0]["status"] == "already_migrated"


def test_populate_fills_the_migrated_columns(declared_and_migrated):
    """A migrated table must be writable by the same code that writes a declared one."""
    legacy, _ = declared_and_migrated
    add_job_metadata_columns(legacy, dry_run=False)
    legacy().heading._init_from_database()

    legacy.populate()
    rows = (
        legacy()
        .connection.query(
            f"SELECT {', '.join(legacy().connection.adapter.quote_identifier(c) for c in JOB_COLUMNS)} "
            f"FROM {legacy().full_table_name}"
        )
        .fetchall()
    )
    assert rows, "populate() inserted nothing; the test would pass vacuously"
    start_time, duration, _version = rows[0]
    assert start_time is not None and duration is not None
