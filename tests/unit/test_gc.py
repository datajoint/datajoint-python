"""Tests for garbage collection module."""

from pathlib import PurePosixPath, PureWindowsPath
from unittest.mock import MagicMock, patch

import pytest

from datajoint import storage
from datajoint.errors import DataJointError
from datajoint.gc import GarbageCollector
from datajoint.storage import StorageBackend


@pytest.mark.parametrize(
    ("path_cls", "location"),
    [
        pytest.param(PureWindowsPath, "data\\blobs", id="windows"),
        pytest.param(PurePosixPath, "data/blobs", id="posix"),
    ],
)
def test_delete_schema_path_prunes_parent_dir(monkeypatch, path_cls, location):
    """Pruning's `rsplit("/", 1)` needs forward slashes; `windows` param catches
    a regression to `str(Path(...))`, `posix` pins the already-correct case."""
    monkeypatch.setattr(storage, "Path", path_cls)
    backend = StorageBackend.__new__(StorageBackend)
    backend.spec = {"protocol": "file", "location": location}
    backend.protocol = "file"
    backend._fs = MagicMock()
    backend.fs.exists.return_value = True
    # Ensure rmdir called only once: non-empty dir stops the walk one level up
    backend.fs.ls.side_effect = lambda p: [] if p == "data/blobs/schema/ab/cd" else ["sibling"]
    collector = GarbageCollector.__new__(GarbageCollector)
    collector.backend = backend
    assert collector.delete_schema_path("schema/ab/cd/hash123") is True
    backend.fs.rm.assert_called_once_with("data/blobs/schema/ab/cd/hash123")
    backend.fs.rmdir.assert_called_once_with("data/blobs/schema/ab/cd")


def _ref_collector(rows=None, get_table_error=None):
    """A collector over one mock schema ``s`` holding one table ``t`` whose
    ``<hash@>``-style column ``a`` yields ``rows`` (an iterable, which may raise
    partway through) — or whose ``get_table`` raises ``get_table_error``."""
    table = MagicMock()
    table.heading.hash_objects = ["a"]
    table.heading.schema_objects = []
    # Each row's metadata is its path; the codec reports it in store "st".
    table.heading.attributes["a"].codec.referenced_paths.side_effect = lambda v: [(v, "st")]
    table.proj.return_value.cursor.return_value = rows if rows is not None else iter([])
    schema = MagicMock()
    schema.database = "s"
    schema.list_tables.return_value = ["t"]
    if get_table_error is not None:
        schema.get_table.side_effect = get_table_error
    else:
        schema.get_table.return_value = table
    collector = GarbageCollector.__new__(GarbageCollector)
    collector.schemas = (schema,)
    collector.store = "st"
    collector.config = MagicMock()
    collector._default_store = None
    collector._scan_errors = []
    return collector


def _rows_then_error(*paths, error):
    for p in paths:
        yield {"a": p}
    raise error


def _list_tables_failing(collector, error):
    """Prepend a schema ``bad`` whose ``list_tables`` raises ``error``."""
    bad = MagicMock()
    bad.database = "bad"
    bad.list_tables.side_effect = error
    collector.schemas = (bad, *collector.schemas)
    return collector


class TestReferenceScanErrors:
    """A schema, table or attribute whose references cannot be read leaves
    the reference set partial; the error is recorded so collect() fails
    closed (#1478), as list_*_paths walk failures already do."""

    def test_table_error_recorded(self):
        collector = _ref_collector(get_table_error=RuntimeError("table gone"))

        assert collector.hash_references() == set()
        assert collector._scan_errors == ["hash_references(s.t): table gone"]

    def test_attribute_error_recorded_after_partial_rows(self):
        """A cursor that fails partway keeps the rows read so far and records
        the error, naming the table and attribute."""
        rows = _rows_then_error("_hash/s/live1", error=ConnectionError("connection lost"))
        collector = _ref_collector(rows=rows)

        assert collector.hash_references() == {"_hash/s/live1"}
        assert collector._scan_errors == ["hash_references(s.t.a): connection lost"]

    def test_codec_error_recorded(self):
        collector = _ref_collector(rows=iter([{"a": "_hash/s/live1"}]))
        attr = collector.schemas[0].get_table.return_value.heading.attributes["a"]
        attr.codec.referenced_paths.side_effect = ValueError("bad metadata")

        assert collector.hash_references() == set()
        assert collector._scan_errors == ["hash_references(s.t.a): bad metadata"]

    def test_schema_references_label(self):
        collector = _ref_collector(get_table_error=RuntimeError("table gone"))

        assert collector.schema_references() == set()
        assert collector._scan_errors == ["schema_references(s.t): table gone"]

    def test_list_tables_error_recorded_and_scan_continues(self):
        """A schema whose tables cannot be listed is recorded; the other
        schemas are still scanned."""
        collector = _list_tables_failing(_ref_collector(rows=iter([{"a": "_hash/s/live1"}])), RuntimeError("no privileges"))

        assert collector.hash_references() == {"_hash/s/live1"}
        assert collector._scan_errors == ["hash_references(bad): no privileges"]

    def _collect(self, collector, dry_run):
        # _hash/s/live2 is referenced only by the row the failed scan never read.
        stored = {"_hash/s/live1": 10, "_hash/s/live2": 20}
        with (
            patch.object(GarbageCollector, "list_hash_paths", return_value=stored),
            patch.object(GarbageCollector, "list_schema_paths", return_value={}),
            patch("datajoint.gc.delete_path", return_value=True) as mock_delete,
        ):
            if dry_run:
                return collector.collect(dry_run=True), mock_delete
            with pytest.raises(DataJointError, match="Refusing to delete") as exc:
                collector.collect(dry_run=False)
            return exc, mock_delete

    def test_collect_refuses_to_delete_on_reference_scan_error(self):
        rows = _rows_then_error("_hash/s/live1", error=ConnectionError("connection lost"))
        exc, mock_delete = self._collect(_ref_collector(rows=rows), dry_run=False)

        mock_delete.assert_not_called()
        assert "hash_references(s.t.a): connection lost" in str(exc.value)

    def test_dry_run_reports_reference_scan_error(self):
        rows = _rows_then_error("_hash/s/live1", error=ConnectionError("connection lost"))
        stats, mock_delete = self._collect(_ref_collector(rows=rows), dry_run=True)

        mock_delete.assert_not_called()
        assert stats["scan_errors"] == ["hash_references(s.t.a): connection lost"]
        # The orphan list is computed from the partial set and is not trusted.
        assert stats["orphaned_hash_paths"] == ["_hash/s/live2"]

    def test_list_tables_error_reported_by_dry_run_and_blocks_deletion(self):
        """A list_tables failure no longer escapes collect(): the dry run
        returns its report and the destructive run refuses."""
        stats, mock_delete = self._collect(_list_tables_failing(_ref_collector(), RuntimeError("no privileges")), dry_run=True)
        assert stats["scan_errors"] == [
            "hash_references(bad): no privileges",
            "schema_references(bad): no privileges",
        ]

        exc, mock_delete = self._collect(_list_tables_failing(_ref_collector(), RuntimeError("no privileges")), dry_run=False)
        mock_delete.assert_not_called()
        assert "hash_references(bad): no privileges" in str(exc.value)

    def test_clean_scan_deletes_orphans(self):
        collector = _ref_collector(rows=iter([{"a": "_hash/s/live1"}]))
        with (
            patch.object(GarbageCollector, "list_hash_paths", return_value={"_hash/s/live1": 10, "_hash/s/dead": 20}),
            patch.object(GarbageCollector, "list_schema_paths", return_value={}),
            patch("datajoint.gc.delete_path", return_value=True) as mock_delete,
        ):
            stats = collector.collect(dry_run=False)

        assert stats["scan_errors"] == []
        assert stats["hash_paths_deleted"] == 1 and stats["bytes_freed"] == 20
        mock_delete.assert_called_once_with("_hash/s/dead", "st", config=collector.config)

    def test_reference_scan_errors_reset_between_collect_calls(self):
        """An error recorded by a direct hash_references() call does not
        survive into the next collect()."""
        collector = _ref_collector(get_table_error=RuntimeError("table gone"))
        collector.hash_references()
        assert collector._scan_errors

        collector.schemas = _ref_collector().schemas  # the table is readable again
        stats, _ = self._collect(collector, dry_run=True)

        assert stats["scan_errors"] == []
