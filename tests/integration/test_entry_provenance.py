"""Integration tests for the hidden `_prov` attribute on Entry tables.

Covers what only a real database shows: which tiers receive the column, that the
value is written without any author involvement, that it stays out of the
heading and out of query composition, and that a fan-out write from inside a
`make()` records the ingesting table and key.
"""

import json

import pytest

import datajoint as dj
from datajoint import provenance
from datajoint.deploy import add_prov_column


@pytest.fixture
def schema_prov(connection_test, prefix):
    """A schema declared with provenance capture on and a source configured."""
    original_capture = dj.config.provenance.capture
    original_source = dj.config.provenance.source
    dj.config.provenance.capture = True
    dj.config.provenance.source = {"system": "PyRat", "endpoint": "https://pyrat.example.org"}

    schema = dj.Schema(prefix + "_entry_prov", connection=connection_test)

    class Subject(dj.Manual):
        definition = """
        subject_id : int16
        ---
        species : varchar(32)
        """

    class Param(dj.Lookup):
        definition = """
        param_id : int16
        ---
        value : float32
        """
        contents = [(1, 1.0)]

    class RecordingFile(dj.Manual):
        definition = """
        file_id : int16
        ---
        path : varchar(255)
        """

    # The fan-out shape: one make() parsing a source into an Entry table that
    # carries no foreign key back to the ingesting table.
    class Ingest(dj.Imported):
        definition = """
        -> RecordingFile
        ---
        n_rows : int16
        """

        def make(self, key):
            Session.insert1({"session_id": 100 + key["file_id"], "note": "fanned out"})
            self.insert1({**key, "n_rows": 1})

    class Session(dj.Manual):
        definition = """
        session_id : int16
        ---
        note : varchar(64)
        """

    class Analysis(dj.Computed):
        definition = """
        -> Subject
        ---
        score : float32
        """

        def make(self, key):
            self.insert1({**key, "score": 1.0})

    schema(Subject)
    schema(Param)
    schema(RecordingFile)
    schema(Session)
    schema(Ingest)
    schema(Analysis)

    yield (
        schema,
        dict(
            Subject=Subject,
            Param=Param,
            RecordingFile=RecordingFile,
            Session=Session,
            Ingest=Ingest,
            Analysis=Analysis,
        ),
    )

    schema.drop()
    dj.config.provenance.capture = original_capture
    dj.config.provenance.source = original_source


def _raw_prov(table):
    """Read the hidden attribute directly; it is not reachable through a heading."""
    rows = table.connection.query(
        f"SELECT {table.adapter.quote_identifier(provenance.PROV_ATTRIBUTE)} FROM {table.full_table_name}"
    ).fetchall()
    return [json.loads(r[0]) if isinstance(r[0], (str, bytes)) else r[0] for r in rows]


def test_only_entry_tables_get_the_column(schema_prov):
    """Computed and Imported provenance is entailed; Lookup and Part need none."""
    _, t = schema_prov

    def has_prov(table):
        table.heading.attributes  # force load
        return provenance.PROV_ATTRIBUTE in table.heading._attributes

    assert has_prov(t["Subject"]())
    assert has_prov(t["Session"]())
    assert has_prov(t["RecordingFile"]())
    assert not has_prov(t["Param"]())
    assert not has_prov(t["Ingest"]())
    assert not has_prov(t["Analysis"]())


def test_insert_records_provenance_without_the_author(schema_prov):
    """No `prov=` argument exists; the row carries a record regardless."""
    _, t = schema_prov
    t["Subject"].insert1({"subject_id": 1, "species": "mouse"})

    (record,) = _raw_prov(t["Subject"]())
    assert record["source"] == {"system": "PyRat", "endpoint": "https://pyrat.example.org"}
    assert "time" in record
    assert record["agent"]["user"]
    assert "context" not in record  # not inside a make()


def test_prov_stays_out_of_the_heading_and_of_queries(schema_prov):
    """A hidden attribute must not leak into fetches or joins."""
    _, t = schema_prov
    t["Subject"].insert1({"subject_id": 2, "species": "rat"})

    assert provenance.PROV_ATTRIBUTE not in t["Subject"]().heading.names
    row = t["Subject"]().to_dicts()[0]
    assert provenance.PROV_ATTRIBUTE not in row
    # a join must not trip over the hidden column
    joined = (t["Subject"] * t["Param"]).to_dicts()
    assert len(joined) == 1
    assert provenance.PROV_ATTRIBUTE not in joined[0]


def test_author_cannot_write_prov(schema_prov):
    """The slot is framework-owned: passing it is an unknown attribute."""
    _, t = schema_prov
    with pytest.raises(Exception):
        t["Subject"].insert1({"subject_id": 3, "species": "mouse", "_prov": {"forged": True}})


def test_fan_out_write_records_the_ingesting_make(schema_prov):
    """The Session row has no foreign key to Ingest, yet records what wrote it."""
    _, t = schema_prov
    t["RecordingFile"].insert1({"file_id": 7, "path": "/data/a.tif"})
    t["Ingest"].populate()

    (record,) = _raw_prov(t["Session"]())
    assert record["context"]["key"] == {"file_id": 7}
    assert "_ingest" in record["context"]["table"]
    # and the file row itself, inserted outside any make(), carries no context
    (file_record,) = _raw_prov(t["RecordingFile"]())
    assert "context" not in file_record


def test_capture_off_declares_no_column_and_migration_adds_it(connection_test, prefix):
    """The retrofit path for a table declared before capture was on."""
    original = dj.config.provenance.capture
    dj.config.provenance.capture = False
    schema = dj.Schema(prefix + "_entry_prov_off", connection=connection_test)

    class Legacy(dj.Manual):
        definition = """
        legacy_id : int16
        ---
        note : varchar(32)
        """

    schema(Legacy)
    try:
        Legacy().heading.attributes
        assert provenance.PROV_ATTRIBUTE not in Legacy().heading._attributes

        # An insert while the column is absent must still succeed, silently.
        dj.config.provenance.capture = True
        Legacy.insert1({"legacy_id": 1, "note": "before"})

        preview = add_prov_column(Legacy, dry_run=True)
        # A dry run reports through `ddl`; the counters record work actually done.
        assert preview["ddl"] and preview["columns_added"] == 0
        assert preview["details"][0]["status"] == "pending"

        applied = add_prov_column(Legacy, dry_run=False)
        assert applied["columns_added"] == 1

        # idempotent
        assert add_prov_column(Legacy, dry_run=False)["columns_added"] == 0

        # No manual _init_from_database(): add_prov_column invalidates the cached
        # heading, and the next access reloads it -- the same way the insert path
        # does. Reaching for _init_from_database() here is what previously hid
        # that inserts kept recording nothing until the process reconnected.
        heading = Legacy().heading
        heading.attributes  # force the lazy reload, as _has_prov_attribute does
        assert provenance.PROV_ATTRIBUTE in heading._attributes

        # the pre-existing row keeps NULL; a new row carries a record
        Legacy.insert1({"legacy_id": 2, "note": "after"})
        values = [
            r[0]
            for r in Legacy()
            .connection.query(f"SELECT `{provenance.PROV_ATTRIBUTE}` FROM {Legacy().full_table_name} ORDER BY legacy_id")
            .fetchall()
        ]
        assert values[0] is None
        assert values[1] is not None
    finally:
        schema.drop()
        dj.config.provenance.capture = original


def test_job_tables_do_not_get_prov(schema_prov):
    """Regression for the #1555 review: `~` passed the old prefix test.

    The job table is only materialised by a refresh, so a plain populate() does
    not surface this -- which is how it survived the first round of tests.
    """
    schema, t = schema_prov
    t["RecordingFile"].insert1({"file_id": 11, "path": "/data/b.tif"})
    t["Ingest"].jobs.refresh()
    t["Ingest"].populate(reserve_jobs=True)

    conn = t["Ingest"]().connection
    tables = [r[0] for r in conn.query(f"SHOW TABLES IN `{schema.database}`").fetchall()]
    job_tables = [name for name in tables if name.startswith("~")]
    assert job_tables, "no job table was created; the test would pass vacuously"

    for name in job_tables:
        columns = {
            r[0]
            for r in conn.query(
                "SELECT COLUMN_NAME FROM information_schema.COLUMNS " "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                args=(schema.database, name),
            ).fetchall()
        }
        assert provenance.PROV_ATTRIBUTE not in columns, f"{name} carries {provenance.PROV_ATTRIBUTE}"


def test_insert_from_query_carries_provenance_across(schema_prov):
    """`insert(QueryExpression)` builds INSERT ... SELECT and used to leave NULL.

    A copied row did not originate in the destination, so the source's record is
    the true one; re-stamping it here would claim an origin that is not where
    the data came from.
    """
    schema, t = schema_prov
    t["Subject"].insert1({"subject_id": 40, "species": "mouse"})
    (original,) = _raw_prov(t["Subject"]() & "subject_id = 40")
    assert original is not None

    class SubjectCopy(dj.Manual):
        definition = t["Subject"].definition

    schema(SubjectCopy)
    SubjectCopy.insert(t["Subject"]() & "subject_id = 40")

    (copied,) = _raw_prov(SubjectCopy())
    assert copied is not None, "copied row lost its provenance"
    assert copied == original, "copied row was re-stamped instead of carrying its origin"
