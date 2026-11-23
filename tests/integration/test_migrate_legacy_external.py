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
from datajoint.migrate import (
    _legacy_hash,
    _legacy_path,
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


def test_pointers_v2_refuses_binary16_column(schema014):
    before = _columns(schema014)
    with pytest.raises(DataJointError, match="BINARY\\(16\\)"):
        migrate_external_pointers_v2(
            schema=schema014.database,
            table="recording",
            attribute="signal",
            source_store="legacy",
            dest_store="legacy",
            connection=schema014.connection,
        )
    assert _columns(schema014) == before


def test_pointers_v2_writes_2x_metadata(schema014):
    db, connection = schema014.database, schema014.connection
    # Widen the column so that it can hold the JSON metadata.
    constraint = connection.query(
        "SELECT CONSTRAINT_NAME FROM information_schema.KEY_COLUMN_USAGE "
        "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'recording' AND COLUMN_NAME = 'signal' "
        "AND REFERENCED_TABLE_NAME IS NOT NULL",
        args=(db,),
    ).fetchone()[0]
    connection.query(f"ALTER TABLE `{db}`.`recording` DROP FOREIGN KEY `{constraint}`")
    connection.query(
        f"ALTER TABLE `{db}`.`recording` MODIFY `signal` varbinary(1024) NOT NULL COMMENT ':blob@legacy:raw signal'"
    )

    result = migrate_external_pointers_v2(
        schema=db,
        table="recording",
        attribute="signal",
        source_store="legacy",
        dest_store="legacy",
        connection=connection,
    )
    assert result["rows_migrated"] == 2
    rows = connection.query(f"SELECT recording_id, `signal` FROM `{db}`.`recording` ORDER BY recording_id").fetchall()
    for (_, stored), blob in zip(rows, (SIGNAL_BLOB, COUNTS_BLOB)):
        metadata = json.loads(stored)
        assert metadata["store"] == "legacy"
        assert metadata["hash"] == compute_hash(blob)
        assert get_hash(metadata) == blob


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
