"""Migrating a DataJoint 0.14 schema's in-store columns, then reading them in 2.x.

The fixture lays a schema out as 0.14.6 does (``declare.py``, ``external.py`` and
``hash.py`` at that tag): a ``~external_legacy`` tracking table, ``binary(16)``
columns with 0.14 comments and foreign keys to it, and files at 0.14's paths. The
blobs are serialized by 0.14.6's own ``blob.pack``. 0.14 ran on MySQL only, so
the fixture does too; the read of a migrated attachment runs on both backends.
"""

import hashlib
import json
from pathlib import Path

import numpy as np
import pytest

import datajoint as dj
from datajoint.errors import DataJointError
from datajoint.hash_registry import compute_hash, get_hash
from datajoint import migrate as dj_migrate
from datajoint.gc import GarbageCollector
from datajoint.migrate import (
    _legacy_hash,
    _legacy_path,
    copy_table_data,
    create_parallel_schema,
    migrate_external,
    migrate_external_pointers_v2,
    migrate_filepath,
    verify_external_integrity,
    verify_schema_v20,
)

# np.array([[1.5, -2.0, 3.25], [4.0, 5.5, -6.75]]), packed by DataJoint 0.14.6
SIGNAL = np.array([[1.5, -2.0, 3.25], [4.0, 5.5, -6.75]])
SIGNAL_BLOB = bytes.fromhex(
    "6d596d00410200000000000000020000000000000003000000000000000600000000000000000000000000f83f"
    "000000000000104000000000000000c000000000000016400000000000000a400000000000001bc0"
)
# np.array([10, 20, 30, 40], dtype=np.int64), packed by DataJoint 0.14.6
COUNTS = np.array([10, 20, 30, 40], dtype=np.int64)
COUNTS_BLOB = bytes.fromhex(
    "6d596d0041010000000000000004000000000000000e000000000000000a0000000000000014000000000000001e000000000000002800000000000000"
)
REPORT_NAME = "report.txt"
REPORT = b"spike count: 12\n"
RAW_PATH = "acquisition/session1/raw.dat"
RAW = b"raw samples"

# DDL as 0.14.6 declares ExternalTable.definition and a table using blob@, attach@, filepath@
HASH_COMMENT = ":uuid:hash of contents (blob), of filename + contents (attach), or relative filepath (filepath)"
EXTERNAL_TABLE_DDL = """
CREATE TABLE `{db}`.`~external_legacy` (
`hash` binary(16) NOT NULL COMMENT "{hash_comment}",
`size` bigint unsigned NOT NULL COMMENT "size of object in bytes",
`attachment_name` varchar(255) DEFAULT NULL COMMENT "the filename of an attachment",
`filepath` varchar(1000) DEFAULT NULL COMMENT "relative filepath or attachment filename",
`contents_hash` binary(16) DEFAULT NULL COMMENT ":uuid:used for the filepath datatype",
`timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT "automatic timestamp",
PRIMARY KEY (`hash`)
) ENGINE=InnoDB, COMMENT "external storage tracking"
"""
RECORDING_DDL = """
CREATE TABLE `{db}`.`recording` (
`recording_id` int NOT NULL,
`signal` binary(16) NOT NULL COMMENT ":blob@legacy:raw signal",
`report` binary(16) DEFAULT NULL COMMENT ":attach@legacy:report file",
`raw_file` binary(16) DEFAULT NULL COMMENT ":filepath@legacy:acquired file",
PRIMARY KEY (`recording_id`),
FOREIGN KEY (`signal`) REFERENCES `{db}`.`~external_legacy` (`hash`) ON UPDATE RESTRICT ON DELETE RESTRICT,
FOREIGN KEY (`report`) REFERENCES `{db}`.`~external_legacy` (`hash`) ON UPDATE RESTRICT ON DELETE RESTRICT,
FOREIGN KEY (`raw_file`) REFERENCES `{db}`.`~external_legacy` (`hash`) ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE=InnoDB, COMMENT "recordings"
"""


def _md5(data: bytes) -> bytes:
    return hashlib.md5(data).digest()


@pytest.fixture
def legacy_store(tmp_path):
    """Configure store `legacy` (no subfolding set, so 0.14 used (2, 2)) and a download path."""
    location = tmp_path / "legacy_store"
    location.mkdir()
    saved_stores, saved_download = dict(dj.config.stores), dj.config.get("download_path")
    dj.config.stores["legacy"] = {"protocol": "file", "location": str(location)}
    dj.config["download_path"] = str(tmp_path / "downloads")
    yield location
    dj.config.stores.clear()
    dj.config.stores.update(saved_stores)
    dj.config["download_path"] = saved_download


@pytest.fixture
def schema014(connection_test, prefix, legacy_store):
    """A schema laid out as 0.14.6 lays it out, with its files in the store."""
    db = f"{prefix}_legacy014"
    connection_test.query(f"DROP DATABASE IF EXISTS `{db}`")
    connection_test.query(f"CREATE DATABASE `{db}`")
    connection_test.query(EXTERNAL_TABLE_DDL.format(db=db, hash_comment=HASH_COMMENT))
    connection_test.query(RECORDING_DDL.format(db=db))

    def put(path, data):
        file = legacy_store / path
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_bytes(data)

    def track(digest, size, attachment_name=None, filepath=None, contents_hash=None):
        connection_test.query(
            f"INSERT INTO `{db}`.`~external_legacy` (hash, size, attachment_name, filepath, contents_hash) "
            "VALUES (%s, %s, %s, %s, %s)",
            args=(digest, size, attachment_name, filepath, contents_hash),
        )

    # Blob: ExternalTable.put
    signal = _md5(SIGNAL_BLOB)
    put(f"{db}/{signal.hex()[:2]}/{signal.hex()[2:4]}/{signal.hex()}", SIGNAL_BLOB)
    track(signal, len(SIGNAL_BLOB))
    # Attachment: ExternalTable.upload_attachment
    report = _md5((REPORT_NAME + "\0").encode() + REPORT)
    put(f"{db}/{report.hex()[:2]}/{report.hex()[2:4]}/{report.hex()}.{REPORT_NAME}", REPORT)
    track(report, len(REPORT), attachment_name=REPORT_NAME)
    # Filepath: ExternalTable.upload_filepath
    raw = _md5(RAW_PATH.encode())
    put(RAW_PATH, RAW)
    track(raw, len(RAW), filepath=RAW_PATH, contents_hash=_md5(RAW))
    # Blob migrated from 0.11: kept at an explicit path, hash of the path, contents_hash of the bytes
    counts_path = f"{db}/0123456789abcdefABCDEF0123456789abcdefABCDE"
    counts = _md5(counts_path.encode())
    put(counts_path, COUNTS_BLOB)
    track(counts, len(COUNTS_BLOB), filepath=counts_path, contents_hash=_md5(COUNTS_BLOB))

    connection_test.query(
        f"INSERT INTO `{db}`.`recording` VALUES (1, %s, %s, %s), (2, %s, NULL, NULL)",
        args=(signal, report, raw, counts),
    )
    schema = dj.Schema(db, connection=connection_test)
    yield schema
    connection_test.query(f"DROP DATABASE IF EXISTS `{db}`")


def _recording(schema):
    return dj.FreeTable(schema.connection, f"`{schema.database}`.`recording`")


def _columns(schema):
    rows = schema.connection.query(
        "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'recording'",
        args=(schema.database,),
    ).fetchall()
    return {name for (name,) in rows}


def _migrate(schema):
    """The guide's in-place steps for in-store columns."""
    preview = migrate_external(schema, dry_run=True)
    assert preview["columns_found"] == 2
    assert all(detail["problems"] == [] for detail in preview["details"])
    assert migrate_external(schema, dry_run=False)["rows_migrated"] == 3
    assert migrate_filepath(schema, dry_run=False)["rows_migrated"] == 1
    assert migrate_external(schema, dry_run=False, finalize=True)["columns_migrated"] == 2
    assert migrate_filepath(schema, dry_run=False, finalize=True)["columns_migrated"] == 1


def test_v2_columns_read_before_finalize(schema014):
    """The guide's verification step: the new columns read in 2.x before finalizing."""
    migrate_external(schema014, dry_run=False)
    migrate_filepath(schema014, dry_run=False)
    recording = _recording(schema014) & {"recording_id": 1}
    np.testing.assert_array_equal(recording.fetch1("signal_v2"), SIGNAL)
    assert Path(recording.fetch1("report_v2")).read_bytes() == REPORT
    assert recording.fetch1("raw_file_v2").read() == RAW


def test_integrity_check_finds_014_files(schema014):
    result = verify_external_integrity(schema014)
    assert result["total_references"] == 4
    assert result["missing"] == []
    assert result["valid"] == 4


def test_migrated_columns_return_their_values(schema014):
    _migrate(schema014)

    recording = _recording(schema014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)

    report = Path((recording & {"recording_id": 1}).fetch1("report"))
    assert report.name == REPORT_NAME
    assert report.read_bytes() == REPORT
    assert (recording & {"recording_id": 2}).fetch1("report") is None

    assert (recording & {"recording_id": 1}).fetch1("raw_file").read() == RAW

    # The 0.14 references are kept beside the migrated columns.
    assert {"signal_v1", "report_v1", "raw_file_v1"} <= _columns(schema014)
    # A required 0.14 column stays required; optional ones stay optional.
    attributes = _recording(schema014).heading.attributes
    assert not attributes["signal"].nullable
    assert attributes["report"].nullable and attributes["raw_file"].nullable
    assert attributes["signal"].comment == "raw signal"
    assert verify_schema_v20(schema014.database, connection=schema014.connection)["compatible"]


def test_migrated_values_carry_2x_metadata(schema014):
    _migrate(schema014)
    db = schema014.database
    stored = schema014.connection.query(f"SELECT `signal` FROM `{db}`.`recording` WHERE recording_id = 1").fetchone()[0]
    stored = json.loads(stored)
    assert stored["hash"] == compute_hash(SIGNAL_BLOB)
    assert stored["store"] == "legacy"
    assert stored["path"] == _legacy_path(db, _md5(SIGNAL_BLOB), (2, 2))


def test_changed_files_fail_the_hash_check(schema014, legacy_store):
    _migrate(schema014)
    db = schema014.database
    signal, report = _md5(SIGNAL_BLOB), _md5((REPORT_NAME + "\0").encode() + REPORT)
    (legacy_store / _legacy_path(db, signal, (2, 2))).write_bytes(COUNTS_BLOB)
    (legacy_store / _legacy_path(db, report, (2, 2), REPORT_NAME)).write_bytes(b"spike count: 13\n")

    recording = _recording(schema014) & {"recording_id": 1}
    with pytest.raises(DataJointError, match="Hash mismatch"):
        recording.fetch1("signal")
    with pytest.raises(DataJointError, match="Hash mismatch"):
        recording.fetch1("report")


def test_missing_file_stops_migration_unchanged(schema014, legacy_store):
    report = _md5((REPORT_NAME + "\0").encode() + REPORT)
    (legacy_store / _legacy_path(schema014.database, report, (2, 2), REPORT_NAME)).unlink()

    preview = migrate_external(schema014, dry_run=True)
    problems = [p for detail in preview["details"] for p in detail["problems"]]
    assert len(problems) == 1 and REPORT_NAME in problems[0]

    before = _columns(schema014)
    with pytest.raises(DataJointError, match="Nothing was changed"):
        migrate_external(schema014, dry_run=False)
    assert _columns(schema014) == before  # the blob column was not converted either


def _drop_foreign_key(schema, column):
    db, connection = schema.database, schema.connection
    constraint = connection.query(
        "SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'recording' AND COLUMN_NAME = %s "
        "AND REFERENCED_TABLE_NAME IS NOT NULL",
        args=(db, column),
    ).fetchone()[0]
    connection.query(f"ALTER TABLE `{db}`.`recording` DROP FOREIGN KEY `{constraint}`")


def _pointers_v2(schema, attribute, dest_store="legacy"):
    return migrate_external_pointers_v2(
        schema=schema.database,
        table="recording",
        attribute=attribute,
        source_store="legacy",
        dest_store=dest_store,
        connection=schema.connection,
    )


@pytest.fixture
def raw_store(legacy_store):
    """A 2.x store named `raw` at the 0.14 location."""
    dj.config.stores["raw"] = {"protocol": "file", "location": str(legacy_store)}
    yield "raw"


def test_pointers_v2_converts_binary16_column(schema014, raw_store):
    """Option A on a table in the 0.14 layout: the column reads in 2.x, through the new store name."""
    assert _pointers_v2(schema014, "signal", dest_store=raw_store)["rows_migrated"] == 2
    assert _pointers_v2(schema014, "report", dest_store=raw_store)["rows_migrated"] == 1

    recording = _recording(schema014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT
    attributes = recording.heading.attributes
    assert not attributes["signal"].nullable and attributes["report"].nullable
    assert attributes["signal"].comment == "raw signal"
    assert {"signal_v1", "report_v1"} <= _columns(schema014)

    db = schema014.database
    stored = schema014.connection.query(f"SELECT `signal` FROM `{db}`.`recording` WHERE recording_id = 1").fetchone()[0]
    assert json.loads(stored)["store"] == raw_store


def test_pointers_v2_converts_widened_column(schema014):
    """A column widened by hand that keeps its 0.14 comment is converted to a column 2.x reads."""
    db, connection = schema014.database, schema014.connection
    _drop_foreign_key(schema014, "signal")
    connection.query(
        f"ALTER TABLE `{db}`.`recording` MODIFY `signal` varbinary(1024) NOT NULL COMMENT ':blob@legacy:raw signal'"
    )

    assert _pointers_v2(schema014, "signal")["rows_migrated"] == 2
    recording = _recording(schema014)
    for recording_id, expected in ((1, SIGNAL), (2, COUNTS)):
        np.testing.assert_array_equal((recording & {"recording_id": recording_id}).fetch1("signal"), expected)
    rows = connection.query(f"SELECT recording_id, `signal` FROM `{db}`.`recording` ORDER BY recording_id").fetchall()
    for (_, stored), blob in zip(rows, (SIGNAL_BLOB, COUNTS_BLOB)):
        metadata = json.loads(stored)
        assert metadata["store"] == "legacy"
        assert metadata["hash"] == compute_hash(blob)
        assert get_hash(metadata) == blob


def test_pointers_v2_refuses_2x_declared_column(schema014, raw_store):
    """A column declared by 2.x holds no 0.14 references; the error points to copy_table_data."""
    v2 = _schema_v2(schema014)
    try:
        with pytest.raises(DataJointError, match="copy_table_data"):
            migrate_external_pointers_v2(
                schema=v2.database,
                table="recording",
                attribute="signal",
                source_store="legacy",
                dest_store=raw_store,
                connection=v2.connection,
            )
    finally:
        v2.drop(prompt=False)


def test_pointers_v2_missing_file_changes_nothing(schema014, legacy_store):
    (legacy_store / _legacy_path(schema014.database, _md5(SIGNAL_BLOB), (2, 2))).unlink()
    before = _columns(schema014)
    with pytest.raises(DataJointError, match="Nothing was changed"):
        _pointers_v2(schema014, "signal")
    assert _columns(schema014) == before


def _schema_v2(schema014, store="raw"):
    """The 2.x declaration of `recording`, as Option A's Phase I code declares it."""
    v2 = dj.Schema(f"{schema014.database}_v2", connection=schema014.connection)

    @v2
    class Recording(dj.Manual):
        definition = f"""
        recording_id : int32
        ---
        signal : <blob@{store}>        # raw signal
        report = null : <attach@{store}>      # report file
        raw_file = null : <filepath@{store}>  # acquired file
        """

    return v2


def test_copy_table_data_converts_014_columns(schema014, raw_store):
    """Option A end to end: copy a 0.14 table into its 2.x declaration and read it back."""
    v2 = _schema_v2(schema014)
    try:
        result = copy_table_data(schema014.database, v2.database, "recording", connection=schema014.connection)
        assert result["rows_copied"] == 2

        recording = dj.FreeTable(v2.connection, f"`{v2.database}`.`recording`")
        np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
        np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
        report = Path((recording & {"recording_id": 1}).fetch1("report"))
        assert report.name == REPORT_NAME and report.read_bytes() == REPORT
        assert (recording & {"recording_id": 2}).fetch1("report") is None
        assert (recording & {"recording_id": 1}).fetch1("raw_file").read() == RAW
    finally:
        v2.drop(prompt=False)


def test_copy_table_data_with_where_and_limit(schema014, raw_store, legacy_store):
    """Only the rows copied are looked up; a missing file elsewhere does not block the copy."""
    (legacy_store / _legacy_path(schema014.database, _md5(SIGNAL_BLOB), (2, 2))).unlink()
    v2 = _schema_v2(schema014)
    try:
        result = copy_table_data(
            schema014.database,
            v2.database,
            "recording",
            where_clause="recording_id = 2",
            limit=1,
            connection=schema014.connection,
        )
        assert result["rows_copied"] == 1
        recording = dj.FreeTable(v2.connection, f"`{v2.database}`.`recording`")
        np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    finally:
        v2.drop(prompt=False)


def test_copy_table_data_missing_file_copies_nothing(schema014, raw_store, legacy_store):
    (legacy_store / _legacy_path(schema014.database, _md5(SIGNAL_BLOB), (2, 2))).unlink()
    v2 = _schema_v2(schema014)
    try:
        with pytest.raises(DataJointError, match="Nothing was copied"):
            copy_table_data(schema014.database, v2.database, "recording", connection=schema014.connection)
        recording = dj.FreeTable(v2.connection, f"`{v2.database}`.`recording`")
        assert len(recording) == 0
    finally:
        v2.drop(prompt=False)


def test_copy_table_data_refuses_other_codec(schema014, raw_store):
    """A 0.14 blob@ column copied into a column that is not <blob@store> is refused."""
    v2 = dj.Schema(f"{schema014.database}_v2", connection=schema014.connection)

    @v2
    class Recording(dj.Manual):
        definition = """
        recording_id : int32
        ---
        signal : <blob>
        report = null : <attach@raw>
        raw_file = null : <filepath@raw>
        """

    try:
        with pytest.raises(DataJointError, match="not declared as <blob@store>"):
            copy_table_data(schema014.database, v2.database, "recording", connection=schema014.connection)
        assert len(Recording()) == 0
    finally:
        v2.drop(prompt=False)


def test_comments_with_quotes_and_percent(schema014):
    """0.14 comments with an apostrophe, a percent sign, or a backslash are kept through migration."""
    db, connection = schema014.database, schema014.connection
    comment = "subject's report, 100% done \\ v2"
    connection.query(
        f"ALTER TABLE `{db}`.`recording` MODIFY `report` binary(16) DEFAULT NULL COMMENT %s",
        args=(":attach@legacy:" + comment,),
    )
    _migrate(schema014)
    recording = _recording(schema014)
    assert recording.heading.attributes["report"].comment == comment
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT


def test_failure_drops_the_columns_this_run_added(schema014, monkeypatch):
    """If a later column fails, the _v2 columns already added are dropped again."""
    added = dj_migrate._add_converted_column
    calls = []

    def fail_second(*args, **kwargs):
        calls.append(args[3])
        if len(calls) == 2:
            raise DataJointError("simulated failure")
        return added(*args, **kwargs)

    monkeypatch.setattr(dj_migrate, "_add_converted_column", fail_second)
    before = _columns(schema014)
    with pytest.raises(DataJointError, match="simulated failure"):
        migrate_external(schema014, dry_run=False)
    assert len(calls) == 2
    assert _columns(schema014) == before


# Values that DataJoint 2.x releases before this fix wrote: no path, no store, a hex hash
def _old_value(digest, size):
    return json.dumps({"url": None, "hash": digest.hex().upper(), "size": size})


def _write_old_v2_columns(schema):
    db, connection = schema.database, schema.connection
    for column, comment in (("signal", ":blob@legacy: raw signal"), ("report", ":attach@legacy: report file")):
        connection.query(f"ALTER TABLE `{db}`.`recording` ADD COLUMN `{column}_v2` JSON COMMENT '{comment}'")
        connection.query(
            f"UPDATE `{db}`.`recording` t JOIN `{db}`.`~external_legacy` e ON t.`{column}` = e.hash "
            f"SET t.`{column}_v2` = JSON_OBJECT('url', NULL, 'hash', HEX(e.hash), 'size', e.size)"
        )


def test_rerun_rewrites_v2_columns_from_earlier_release(schema014):
    """_v2 columns written by an earlier release are rewritten, not skipped."""
    _write_old_v2_columns(schema014)

    preview = migrate_external(schema014, dry_run=True)
    assert {detail["action"] for detail in preview["details"]} == {"rewrite"}
    with pytest.raises(DataJointError, match="cannot be finalized"):
        migrate_external(schema014, dry_run=False, finalize=True)

    assert migrate_external(schema014, dry_run=False)["rows_migrated"] == 3
    recording = _recording(schema014) & {"recording_id": 1}
    np.testing.assert_array_equal(recording.fetch1("signal_v2"), SIGNAL)
    assert Path(recording.fetch1("report_v2")).read_bytes() == REPORT

    # Now readable, so a further rerun skips them.
    statuses = {detail["status"] for detail in migrate_external(schema014, dry_run=False)["details"]}
    assert statuses == {"already_migrated"}
    assert migrate_external(schema014, dry_run=False, finalize=True)["columns_migrated"] == 2
    np.testing.assert_array_equal((_recording(schema014) & {"recording_id": 1}).fetch1("signal"), SIGNAL)


def test_rerun_repairs_columns_finalized_by_earlier_release(schema014):
    """Columns that an earlier release finalized are rewritten from the references kept in _v1."""
    db, connection = schema014.database, schema014.connection
    _write_old_v2_columns(schema014)
    # What finalize did in an earlier release
    for column, comment in (("signal", ":blob@legacy: raw signal"), ("report", ":attach@legacy: report file")):
        connection.query(
            f"ALTER TABLE `{db}`.`recording` CHANGE COLUMN `{column}` `{column}_v1` binary(16) COMMENT 'legacy 0.x'"
        )
        connection.query(f"ALTER TABLE `{db}`.`recording` CHANGE COLUMN `{column}_v2` `{column}` JSON COMMENT '{comment}'")

    preview = migrate_external(schema014, dry_run=True)
    assert all(detail["nullable"] for detail in preview["details"])
    result = migrate_external(schema014, dry_run=False)
    assert {detail["action"] for detail in result["details"]} == {"repair"}
    assert result["rows_migrated"] == 3
    recording = _recording(schema014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT
    assert recording.heading.attributes["signal"].comment == "raw signal"

    # The earlier finalize dropped NOT NULL, and repair cannot know it was there.
    assert recording.heading.attributes["signal"].nullable
    # The guide's statement restores it.
    connection.query(
        f"ALTER TABLE `{db}`.`recording` MODIFY COLUMN `signal` JSON NOT NULL COMMENT ':<blob@legacy>:raw signal'"
    )
    recording = _recording(schema014)
    assert not recording.heading.attributes["signal"].nullable
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)


def test_finalize_refuses_without_v2_columns(schema014):
    before = _columns(schema014)
    preview = migrate_filepath(schema014, dry_run=True, finalize=True)
    assert "does not exist" in preview["details"][0]["problems"][0]
    with pytest.raises(DataJointError, match="Nothing was changed"):
        migrate_filepath(schema014, dry_run=False, finalize=True)
    assert _columns(schema014) == before


def test_filepath_without_contents_hash(schema014):
    """0.14.7+ leaves contents_hash NULL for large files; the file still migrates and reads."""
    db = schema014.database
    schema014.connection.query(
        f"UPDATE `{db}`.`~external_legacy` SET contents_hash = NULL WHERE hash = %s",
        args=(_md5(RAW_PATH.encode()),),
    )
    preview = migrate_filepath(schema014, dry_run=True)
    assert preview["details"][0]["problems"] == []
    _migrate(schema014)
    assert (_recording(schema014) & {"recording_id": 1}).fetch1("raw_file").read() == RAW


def test_null_subfolding_means_the_014_default(schema014):
    """The 2.x configuration template writes "subfolding": null; 0.14's default (2, 2) applies."""
    dj.config.stores["legacy"]["subfolding"] = None
    _migrate(schema014)
    np.testing.assert_array_equal((_recording(schema014) & {"recording_id": 1}).fetch1("signal"), SIGNAL)


@pytest.fixture
def copy014(schema014):
    """A copy of the 0.14 schema made with create_parallel_schema; its files stay under the original name."""
    db, connection = schema014.database, schema014.connection
    copy_db = f"{db}_copy"
    connection.query(f"DROP DATABASE IF EXISTS `{copy_db}`")
    result = create_parallel_schema(db, copy_db, copy_data=True, connection=connection)
    assert set(result["tables"]) == {"recording", "~external_legacy"}
    yield dj.Schema(copy_db, connection=connection)
    connection.query(f"DROP DATABASE IF EXISTS `{copy_db}`")


def test_parallel_schema_copies_014_schema(schema014, copy014):
    """The tracking table sorts last; the copy's foreign keys point to the copy's own tracking table."""
    connection = copy014.connection
    referenced = connection.query(
        "SELECT DISTINCT REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'recording' AND REFERENCED_TABLE_NAME IS NOT NULL",
        args=(copy014.database,),
    ).fetchall()
    assert referenced == ((copy014.database, "~external_legacy"),)
    assert len(_recording(copy014)) == 2
    assert connection.query("SELECT @@SESSION.foreign_key_checks").fetchone()[0] == 1


def test_parallel_schema_keeps_percent_in_comments(schema014, connection_test):
    db = schema014.database
    connection_test.query(
        f"ALTER TABLE `{db}`.`recording` MODIFY `report` binary(16) DEFAULT NULL COMMENT %s",
        args=(":attach@legacy:report, 100% done",),
    )
    try:
        create_parallel_schema(db, f"{db}_copy", connection=connection_test)
        comment = connection_test.query(
            "SELECT COLUMN_COMMENT FROM information_schema.COLUMNS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'recording' AND COLUMN_NAME = 'report'",
            args=(f"{db}_copy",),
        ).fetchone()[0]
        assert comment == ":attach@legacy:report, 100% done"
    finally:
        connection_test.query(f"DROP DATABASE IF EXISTS `{db}_copy`")


def test_copy_needs_the_original_database_name(schema014, copy014):
    """On a copy, blob and attachment files are found under the original name, given as legacy_database."""
    with pytest.raises(DataJointError, match="legacy_database"):
        migrate_external(copy014, dry_run=False)

    assert verify_external_integrity(copy014, legacy_database=schema014.database)["missing"] == []
    preview = migrate_external(copy014, dry_run=True, legacy_database=schema014.database)
    assert all(detail["problems"] == [] for detail in preview["details"])
    assert migrate_external(copy014, dry_run=False, legacy_database=schema014.database)["rows_migrated"] == 3
    assert migrate_filepath(copy014, dry_run=False)["rows_migrated"] == 1
    assert migrate_external(copy014, dry_run=False, finalize=True)["columns_migrated"] == 2
    assert migrate_filepath(copy014, dry_run=False, finalize=True)["columns_migrated"] == 1

    recording = _recording(copy014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT
    assert (recording & {"recording_id": 1}).fetch1("raw_file").read() == RAW
    stored = copy014.connection.query(
        f"SELECT `signal` FROM `{copy014.database}`.`recording` WHERE recording_id = 1"
    ).fetchone()[0]
    assert json.loads(stored)["path"].startswith(f"{schema014.database}/")


def test_pointers_v2_on_a_copy(schema014, copy014, raw_store):
    for attribute in ("signal", "report"):
        with pytest.raises(DataJointError, match="legacy_database"):
            _pointers_v2(copy014, attribute, dest_store=raw_store)
        migrate_external_pointers_v2(
            schema=copy014.database,
            table="recording",
            attribute=attribute,
            source_store="legacy",
            dest_store=raw_store,
            connection=copy014.connection,
            legacy_database=schema014.database,
        )
    recording = _recording(copy014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT


def test_copy_table_data_from_a_copy(schema014, copy014, raw_store):
    """copy_table_data uses the source's name unless legacy_database names the original."""
    v2 = _schema_v2(copy014)
    try:
        with pytest.raises(DataJointError, match="legacy_database"):
            copy_table_data(copy014.database, v2.database, "recording", connection=copy014.connection)
        result = copy_table_data(
            copy014.database,
            v2.database,
            "recording",
            connection=copy014.connection,
            legacy_database=schema014.database,
        )
        assert result["rows_copied"] == 2
        recording = dj.FreeTable(v2.connection, f"`{v2.database}`.`recording`")
        np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
        assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT
    finally:
        v2.drop(prompt=False)


def _014_files(legacy_store, db):
    return {str(path.relative_to(legacy_store)) for path in (legacy_store / db).rglob("*") if path.is_file()}


def test_gc_root_level_layout_skips_schema_with_tracking_tables(schema014, legacy_store):
    """With schema_prefix="", {database}/ holds the 0.14 files; GC does not list it while 0.14 tables remain."""
    _migrate(schema014)
    db = schema014.database
    files = _014_files(legacy_store, db)
    dj.config.stores["legacy"]["schema_prefix"] = ""

    collector = GarbageCollector(schema014, store="legacy")
    stats = collector.collect(dry_run=False)
    assert _014_files(legacy_store, db) == files
    assert stats["orphaned_schema_paths"] == [] and stats["deleted"] == 0
    assert stats["skipped_schemas"] == [db]
    np.testing.assert_array_equal((_recording(schema014) & {"recording_id": 1}).fetch1("signal"), SIGNAL)


def test_gc_root_level_layout_keeps_migrated_files(schema014, legacy_store):
    """Without the tracking tables, migrated files count as referenced and 0.14 names are never orphans."""
    _migrate(schema014)
    db, connection = schema014.database, schema014.connection
    constraints = connection.query(
        "SELECT TABLE_NAME, CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = %s AND REFERENCED_TABLE_NAME = '~external_legacy'",
        args=(db,),
    ).fetchall()
    for table_name, constraint in constraints:
        connection.query(f"ALTER TABLE `{db}`.`{table_name}` DROP FOREIGN KEY `{constraint}`")
    connection.query(f"DROP TABLE `{db}`.`~external_legacy`")

    # A 0.14 blob that no row references, and a file in the root-level layout that nothing references
    unreferenced = _legacy_path(db, _md5(b"unreferenced"), (2, 2))
    stray = f"{db}/recording/rid=9/data_x1y2z3w4.npy"
    for path in (unreferenced, stray):
        (legacy_store / path).parent.mkdir(parents=True, exist_ok=True)
        (legacy_store / path).write_bytes(b"x")
    files = _014_files(legacy_store, db)
    dj.config.stores["legacy"]["schema_prefix"] = ""

    collector = GarbageCollector(schema014, store="legacy")
    assert collector.collect()["orphaned_schema_paths"] == [stray]
    collector.collect(dry_run=False)
    assert _014_files(legacy_store, db) == files - {stray}
    recording = _recording(schema014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT


def test_gc_root_level_layout_keeps_files_a_copy_references(schema014, copy014, legacy_store):
    """Files under the original's name that only the migrated copy references stay when the original is collected."""
    db, connection = schema014.database, schema014.connection
    assert migrate_external(copy014, dry_run=False, legacy_database=db)["rows_migrated"] == 3
    assert migrate_external(copy014, dry_run=False, finalize=True)["columns_migrated"] == 2
    # The original no longer has 0.14 tables, so GC lists its section
    connection.query(f"DROP TABLE `{db}`.`recording`")
    connection.query(f"DROP TABLE `{db}`.`~external_legacy`")
    stray = f"{db}/recording/rid=9/data_x1y2z3w4.npy"
    (legacy_store / stray).parent.mkdir(parents=True)
    (legacy_store / stray).write_bytes(b"x")
    files = _014_files(legacy_store, db)
    dj.config.stores["legacy"]["schema_prefix"] = ""

    collector = GarbageCollector(schema014, store="legacy")
    assert collector.collect()["orphaned_schema_paths"] == [stray]
    collector.collect(dry_run=False)
    assert _014_files(legacy_store, db) == files - {stray}
    recording = _recording(copy014)
    np.testing.assert_array_equal((recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
    np.testing.assert_array_equal((recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
    assert Path((recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT


def test_gc_root_level_layout_copy_on_both_backends(connection_by_backend, backend, prefix, legacy_store):
    """
    A copy's migrated values point under the original's name. Collecting the
    original alone keeps the 0.14-named and top-level files and the original's
    own <filepath@> files; a file in a subfolder that only the copy references
    is kept when the copy is collected too.
    """
    dj.config.stores["legacy"]["schema_prefix"] = ""
    original = dj.Schema(f"{prefix}_gc_original_{backend}", connection=connection_by_backend)
    copy = dj.Schema(f"{prefix}_gc_copy_{backend}", connection=connection_by_backend)
    db = original.database

    @original
    class Note(dj.Manual):
        definition = """
        note_id : int32
        ---
        note : <filepath@legacy>
        """

    @copy
    class Recording(dj.Manual):
        definition = """
        recording_id : int32
        ---
        signal = null : <blob@legacy>
        report = null : <attach@legacy>
        raw_file = null : <filepath@legacy>
        """

    def put(path, data):
        (legacy_store / path).parent.mkdir(parents=True, exist_ok=True)
        (legacy_store / path).write_bytes(data)

    try:
        signal = _md5(SIGNAL_BLOB)
        signal_path = _legacy_path(db, signal, (2, 2))
        report = _md5((REPORT_NAME + "\0").encode() + REPORT)
        report_path = _legacy_path(db, report, (2, 2), REPORT_NAME)
        counts_path = f"{db}/0123456789abcdefABCDEF0123456789abcdefABCDE"  # a blob 0.14 migrated from 0.11
        note_path, raw_path = f"{db}/notes/readme.txt", f"{db}/acquisition/raw.dat"
        stray = f"{db}/recording/rid=9/data_x1y2z3w4.npy"
        for path, data in (
            (signal_path, SIGNAL_BLOB),
            (report_path, REPORT),
            (counts_path, COUNTS_BLOB),
            (note_path, b"notes"),
            (raw_path, RAW),
            (stray, b"x"),
        ):
            put(path, data)

        Note.insert1({"note_id": 1, "note": note_path})
        Recording.insert([{"recording_id": 1, "raw_file": raw_path}, {"recording_id": 2, "raw_file": None}])
        quote = connection_by_backend.adapter.quote_identifier

        def set_metadata(recording_id, column, metadata):
            connection_by_backend.query(
                f"UPDATE {Recording.full_table_name} SET {quote(column)} = %s WHERE {quote('recording_id')} = %s",
                args=(json.dumps({"schema": db, "store": "legacy", **metadata}), recording_id),
            )

        set_metadata(1, "signal", {"hash": _legacy_hash(signal), "path": signal_path, "size": len(SIGNAL_BLOB)})
        set_metadata(
            1,
            "report",
            {"hash": _legacy_hash(report), "path": report_path, "size": len(REPORT), "attachment_name": REPORT_NAME},
        )
        set_metadata(2, "signal", {"hash": _legacy_hash(_md5(COUNTS_BLOB)), "path": counts_path, "size": len(COUNTS_BLOB)})
        files = _014_files(legacy_store, db)

        # The original alone: the file in a subfolder that only the copy references is listed
        assert GarbageCollector(original, store="legacy").collect()["orphaned_schema_paths"] == [raw_path, stray]

        collector = GarbageCollector(original, copy, store="legacy")
        assert collector.collect()["orphaned_schema_paths"] == [stray]
        collector.collect(dry_run=False)
        assert _014_files(legacy_store, db) == files - {stray}
        np.testing.assert_array_equal((Recording & {"recording_id": 1}).fetch1("signal"), SIGNAL)
        np.testing.assert_array_equal((Recording & {"recording_id": 2}).fetch1("signal"), COUNTS)
        assert Path((Recording & {"recording_id": 1}).fetch1("report")).read_bytes() == REPORT
        assert (Recording & {"recording_id": 1}).fetch1("raw_file").read() == RAW
        assert (Note & {"note_id": 1}).fetch1("note").read() == b"notes"
    finally:
        copy.drop(prompt=False)
        original.drop(prompt=False)


def test_gc_root_level_layout_normalizes_filepath_references(connection_by_backend, backend, prefix, legacy_store):
    """<filepath@> values stored with ./ or // still count as references to the files they read."""
    dj.config.stores["legacy"]["schema_prefix"] = ""
    schema = dj.Schema(f"{prefix}_gc_filepath_{backend}", connection=connection_by_backend)
    db = schema.database

    @schema
    class Note(dj.Manual):
        definition = """
        note_id : int32
        ---
        note : <filepath@legacy>
        """

    try:
        files = {f"{db}/sub/a.txt": b"a", f"{db}/sub/b.txt": b"b"}
        for path, data in files.items():
            (legacy_store / path).parent.mkdir(parents=True, exist_ok=True)
            (legacy_store / path).write_bytes(data)
        Note.insert([{"note_id": 1, "note": f"./{db}/sub/a.txt"}, {"note_id": 2, "note": f"{db}//sub/b.txt"}])

        collector = GarbageCollector(schema, store="legacy")
        assert collector.collect()["orphaned_schema_paths"] == []
        stats = collector.collect(dry_run=False)
        assert stats["deleted"] == 0
        assert _014_files(legacy_store, db) == set(files)
        assert (Note & {"note_id": 1}).fetch1("note").read() == b"a"
        assert (Note & {"note_id": 2}).fetch1("note").read() == b"b"
    finally:
        schema.drop(prompt=False)


def test_dry_run_needs_the_store(schema014):
    """A dry run looks files up, so the store must be configured."""
    del dj.config.stores["legacy"]
    with pytest.raises(DataJointError, match="legacy"):
        migrate_external(schema014, dry_run=True)


def test_migrated_attachment_reads(connection_by_backend, backend, prefix, legacy_store):
    """A migrated attachment's metadata reads through <attach@> on either backend."""
    schema = dj.Schema(f"{prefix}_legacy_attach_{backend}", connection=connection_by_backend)

    @schema
    class Document(dj.Manual):
        definition = """
        doc_id : int32
        ---
        report = null : <attach@legacy>
        """

    try:
        Document.insert1({"doc_id": 1})
        digest = _md5((REPORT_NAME + "\0").encode() + REPORT)
        path = _legacy_path(schema.database, digest, (2, 2), REPORT_NAME)
        (legacy_store / path).parent.mkdir(parents=True)
        (legacy_store / path).write_bytes(REPORT)
        metadata = {
            "hash": _legacy_hash(digest),
            "path": path,
            "schema": schema.database,
            "store": "legacy",
            "size": len(REPORT),
            "attachment_name": REPORT_NAME,
        }
        quote = connection_by_backend.adapter.quote_identifier
        connection_by_backend.query(
            f"UPDATE {Document.full_table_name} SET {quote('report')} = %s WHERE {quote('doc_id')} = 1",
            args=(json.dumps(metadata),),
        )

        report = Path((Document & {"doc_id": 1}).fetch1("report"))
        assert report.name == REPORT_NAME
        assert report.read_bytes() == REPORT

        (legacy_store / path).write_bytes(b"spike count: 13\n")
        with pytest.raises(DataJointError, match="Hash mismatch"):
            (Document & {"doc_id": 1}).fetch1("report")
    finally:
        schema.drop(prompt=False)
