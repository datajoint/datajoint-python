"""
Tests for garbage collection (gc.py).
"""

from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import datajoint as dj
from datajoint import gc
from datajoint.builtin_codecs.object import ObjectCodec
from datajoint.codecs import get_codec
from datajoint.errors import DataJointError


class GcCustomObjectCodec(ObjectCodec):
    """A custom schema-addressed codec — a ``SchemaCodec`` subclass with a
    non-built-in name, mirroring the user's NetCDF codec in #1469. Registered
    at import via ``Codec.__init_subclass__``. GC must recognize it by type
    (not by hardcoded name) and treat its files as referenced, not orphaned.
    """

    name = "gc_custom_object"


# Tables used by TestScanWithLiveData. Defined at module scope so dj.Schema's
# context resolution can find them by class name; bound to a schema inside
# each fixture (see schema(...) calls below).


class GcBlobTest(dj.Manual):
    definition = """
    rid : int
    ---
    payload : <blob@local>
    """


class GcNpyTest(dj.Manual):
    definition = """
    rid : int
    ---
    waveform : <npy@local>
    """


class GcObjectTest(dj.Manual):
    definition = """
    rid : int
    ---
    results : <object@local>
    """


class GcCustomCodecTest(dj.Manual):
    definition = """
    rid : int
    ---
    payload : <gc_custom_object@local>
    """


class TestUsesHashStorage:
    """Tests for _uses_hash_storage helper function."""

    def test_returns_false_for_no_adapter(self):
        """Test that False is returned when attribute has no codec."""
        attr = MagicMock()
        attr.codec = None

        assert gc._uses_hash_storage(attr) is False

    def test_returns_true_for_hash_type(self):
        """Test that True is returned for <hash@> type."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "hash"
        attr.store = "mystore"

        assert gc._uses_hash_storage(attr) is True

    def test_returns_true_for_blob_external(self):
        """Test that True is returned for <blob@> type (external)."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "blob"
        attr.store = "mystore"

        assert gc._uses_hash_storage(attr) is True

    def test_returns_true_for_attach_external(self):
        """Test that True is returned for <attach@> type (external)."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "attach"
        attr.store = "mystore"

        assert gc._uses_hash_storage(attr) is True

    def test_returns_false_for_blob_internal(self):
        """Test that False is returned for <blob> internal storage."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "blob"
        attr.store = None

        assert gc._uses_hash_storage(attr) is False


class TestHashReferencedPaths:
    """Reference extraction for hash-addressed metadata via the codec API
    (Codec.referenced_paths — the production path used by gc.scan)."""

    def test_returns_empty_for_none(self):
        assert get_codec("hash").referenced_paths(None) == []

    def test_parses_json_string(self):
        value = '{"path": "_hash/schema/abc123", "hash": "abc123", "store": "mystore"}'
        refs = get_codec("hash").referenced_paths(value)

        assert refs == [("_hash/schema/abc123", "mystore")]

    def test_parses_dict_directly(self):
        value = {"path": "_hash/schema/def456", "hash": "def456", "store": None}
        refs = get_codec("hash").referenced_paths(value)

        assert refs == [("_hash/schema/def456", None)]

    def test_returns_empty_for_invalid_json(self):
        assert get_codec("hash").referenced_paths("not json") == []

    def test_returns_empty_for_dict_without_path(self):
        assert get_codec("hash").referenced_paths({"hash": "abc123"}) == []


class TestUsesSchemaStorage:
    """Tests for _uses_schema_storage helper function."""

    def test_returns_false_for_no_adapter(self):
        """Test that False is returned when attribute has no codec."""
        attr = MagicMock()
        attr.codec = None

        assert gc._uses_schema_storage(attr) is False

    def test_returns_true_for_object_type(self):
        """Test that True is returned for <object@> type."""
        attr = MagicMock()
        attr.codec = get_codec("object")

        assert gc._uses_schema_storage(attr) is True

    def test_returns_true_for_npy_type(self):
        """Test that True is returned for <npy@> type."""
        attr = MagicMock()
        attr.codec = get_codec("npy")

        assert gc._uses_schema_storage(attr) is True

    def test_returns_true_for_custom_schema_subclass(self):
        """Recognition is by type, not name: a custom SchemaCodec subclass
        (here, a subclass of ObjectCodec) must be seen as schema-addressed so
        GC does not misclassify its live files as orphans (#1469)."""
        attr = MagicMock()
        attr.codec = get_codec("gc_custom_object")

        assert gc._uses_schema_storage(attr) is True

    def test_returns_false_for_other_types(self):
        """Test that False is returned for non-schema-addressed types."""
        attr = MagicMock()
        attr.codec = get_codec("blob")

        assert gc._uses_schema_storage(attr) is False


class TestSchemaReferencedPaths:
    """Reference extraction for schema-addressed metadata via the codec API
    (inherited by every SchemaCodec subclass, including custom codecs)."""

    def test_returns_empty_for_none(self):
        assert get_codec("object").referenced_paths(None) == []

    def test_parses_json_string(self):
        value = '{"path": "schema/table/pk/field", "store": "mystore"}'
        refs = get_codec("object").referenced_paths(value)

        assert refs == [("schema/table/pk/field", "mystore")]

    def test_parses_dict_directly(self):
        refs = get_codec("object").referenced_paths({"path": "test/path", "store": None})

        assert refs == [("test/path", None)]

    def test_custom_subclass_inherits_extraction(self):
        refs = get_codec("gc_custom_object").referenced_paths({"path": "a/b/c", "store": "local"})

        assert refs == [("a/b/c", "local")]

    def test_returns_empty_for_dict_without_path(self):
        assert get_codec("object").referenced_paths({"other": "data"}) == []


class TestScan:
    """Tests for scan function."""

    def test_requires_at_least_one_schema(self):
        """Test that at least one schema is required."""
        with pytest.raises(DataJointError, match="At least one schema must be provided"):
            gc.scan()

    @patch("datajoint.gc.scan_schema_references")
    @patch("datajoint.gc.list_schema_paths")
    @patch("datajoint.gc.scan_hash_references")
    @patch("datajoint.gc.list_stored_hashes")
    def test_returns_stats(self, mock_list_hashes, mock_scan_hash, mock_list_schemas, mock_scan_schema):
        """Test that scan returns proper statistics."""
        # Mock hash-addressed storage (now uses paths)
        mock_scan_hash.return_value = {"_hash/schema/path1", "_hash/schema/path2"}
        mock_list_hashes.return_value = {
            "_hash/schema/path1": 100,
            "_hash/schema/path3": 200,  # orphaned
        }

        # Mock schema-addressed storage
        mock_scan_schema.return_value = {"schema/table/pk1/field"}
        mock_list_schemas.return_value = {
            "schema/table/pk1/field": 500,
            "schema/table/pk2/field": 300,  # orphaned
        }

        mock_schema = MagicMock()
        stats = gc.scan(mock_schema, store_name="test_store")

        # Hash stats
        assert stats["hash_referenced"] == 2
        assert stats["hash_stored"] == 2
        assert stats["hash_orphaned"] == 1
        assert "_hash/schema/path3" in stats["orphaned_hashes"]

        # Schema stats
        assert stats["schema_paths_referenced"] == 1
        assert stats["schema_paths_stored"] == 2
        assert stats["schema_paths_orphaned"] == 1
        assert "schema/table/pk2/field" in stats["orphaned_paths"]

        # Combined totals
        assert stats["referenced"] == 3
        assert stats["stored"] == 4
        assert stats["orphaned"] == 2
        assert stats["orphaned_bytes"] == 500  # 200 hash + 300 schema


class TestCollect:
    """Tests for collect function."""

    @patch("datajoint.gc.scan")
    def test_dry_run_does_not_delete(self, mock_scan):
        """Test that dry_run=True doesn't delete anything."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 100,
            "orphaned_hashes": ["_hash/schema/orphan_path"],
            "orphaned_paths": [],
            "hash_orphaned": 1,
            "schema_paths_orphaned": 0,
        }

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=True)

        assert stats["deleted"] == 0
        assert stats["bytes_freed"] == 0
        assert stats["dry_run"] is True

    @patch("datajoint.gc.delete_path")
    @patch("datajoint.gc.list_stored_hashes")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_hashes(self, mock_scan, mock_list_stored, mock_delete):
        """Test that orphaned hashes are deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 100,
            "orphaned_hashes": ["_hash/schema/orphan_path"],
            "orphaned_paths": [],
            "hash_orphaned": 1,
            "schema_paths_orphaned": 0,
        }
        mock_list_stored.return_value = {"_hash/schema/orphan_path": 100}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["hash_deleted"] == 1
        assert stats["bytes_freed"] == 100
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("_hash/schema/orphan_path", "test_store", config=mock_schema.connection._config)

    @patch("datajoint.gc.delete_schema_path")
    @patch("datajoint.gc.list_schema_paths")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_schemas(self, mock_scan, mock_list_schemas, mock_delete):
        """Test that orphaned schema paths are deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 500,
            "orphaned_hashes": [],
            "orphaned_paths": ["schema/table/pk/field"],
            "hash_orphaned": 0,
            "schema_paths_orphaned": 1,
        }
        mock_list_schemas.return_value = {"schema/table/pk/field": 500}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["schema_paths_deleted"] == 1
        assert stats["bytes_freed"] == 500
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("schema/table/pk/field", "test_store", config=mock_schema.connection._config)


class TestFormatStats:
    """Tests for format_stats function."""

    def test_formats_scan_stats(self):
        """Test formatting scan statistics."""
        stats = {
            "referenced": 10,
            "stored": 15,
            "orphaned": 5,
            "orphaned_bytes": 1024 * 1024,  # 1 MB
            "hash_referenced": 6,
            "hash_stored": 8,
            "hash_orphaned": 2,
            "hash_orphaned_bytes": 512 * 1024,
            "schema_paths_referenced": 4,
            "schema_paths_stored": 7,
            "schema_paths_orphaned": 3,
            "schema_paths_orphaned_bytes": 512 * 1024,
        }

        result = gc.format_stats(stats)

        assert "Referenced in database: 10" in result
        assert "Stored in backend:      15" in result
        assert "Orphaned (unreferenced): 5" in result
        assert "1.00 MB" in result
        # Check for detailed sections
        assert "Hash-Addressed Storage" in result
        assert "Schema-Addressed Storage" in result

    def test_formats_collect_stats_dry_run(self):
        """Test formatting collect statistics with dry_run."""
        stats = {
            "referenced": 10,
            "stored": 15,
            "orphaned": 5,
            "deleted": 0,
            "bytes_freed": 0,
            "dry_run": True,
        }

        result = gc.format_stats(stats)

        assert "DRY RUN" in result

    def test_formats_collect_stats_actual(self):
        """Test formatting collect statistics after actual deletion."""
        stats = {
            "referenced": 10,
            "stored": 15,
            "orphaned": 5,
            "deleted": 3,
            "hash_deleted": 2,
            "schema_paths_deleted": 1,
            "bytes_freed": 2 * 1024 * 1024,  # 2 MB
            "errors": 2,
            "dry_run": False,
        }

        result = gc.format_stats(stats)

        assert "Deleted:     3" in result
        assert "Hash items:   2" in result
        assert "Schema paths: 1" in result
        assert "2.00 MB" in result
        assert "Errors:      2" in result


class TestScanWithLiveData:
    """End-to-end tests for gc.scan() against real schemas with external storage.

    Exercises the full production path:
        scan_*_references → table.proj(attr).cursor() → raw JSON metadata.

    These are the regression tests that would have caught issue #1442
    (silent type mismatch when scan helpers iterated decoded codec outputs
    instead of raw stored metadata).
    """

    @pytest.fixture
    def schema_blob(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_blob"
        schema = dj.Schema(
            schema_name,
            context={"GcBlobTest": GcBlobTest},
            connection=connection_test,
        )
        schema(GcBlobTest)
        yield schema
        schema.drop()

    @pytest.fixture
    def schema_npy(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_npy"
        schema = dj.Schema(
            schema_name,
            context={"GcNpyTest": GcNpyTest},
            connection=connection_test,
        )
        schema(GcNpyTest)
        yield schema
        schema.drop()

    @pytest.fixture
    def schema_object(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_object"
        schema = dj.Schema(
            schema_name,
            context={"GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcObjectTest)
        yield schema
        schema.drop()

    def test_scan_finds_active_blob_reference(self, schema_blob):
        """scan() must report hash_referenced >= 1 for a populated <blob@> column.

        Decoded value type returned by BlobCodec.decode is numpy.ndarray, which
        does not satisfy the raw-metadata dict/JSON-string check in referenced_paths — this
        test fails before the cursor-based fix in scan_hash_references.
        """
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(64, dtype="uint8")})

        stats = gc.scan(schema_blob, store_name="local")

        assert stats["hash_referenced"] >= 1, f"scan should find the active <blob@> reference; got {stats}"

    def test_scan_finds_active_npy_reference(self, schema_npy):
        """scan() must report schema_paths_referenced >= 1 for a populated <npy@> column.

        Decoded value type returned by NpyCodec.decode is NpyRef (lazy handle),
        which does not satisfy the raw-metadata dict check in referenced_paths — this test
        fails before the cursor-based fix in scan_schema_references.
        """
        GcNpyTest.insert1({"rid": 1, "waveform": np.arange(64, dtype="float32")})

        stats = gc.scan(schema_npy, store_name="local")

        assert stats["schema_paths_referenced"] >= 1, f"scan should find the active <npy@> reference; got {stats}"

    def test_scan_finds_active_object_reference(self, schema_object):
        """scan() must report schema_paths_referenced >= 1 for a populated <object@> column.

        Decoded value type returned by ObjectCodec.decode is ObjectRef (lazy
        handle), which does not satisfy the raw-metadata dict check in referenced_paths —
        this test fails before the cursor-based fix in scan_schema_references.
        """
        GcObjectTest.insert1({"rid": 1, "results": b"hello-gc-test"})

        stats = gc.scan(schema_object, store_name="local")

        assert stats["schema_paths_referenced"] >= 1, f"scan should find the active <object@> reference; got {stats}"

    @pytest.fixture
    def schema_custom(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_custom"
        schema = dj.Schema(
            schema_name,
            context={"GcCustomCodecTest": GcCustomCodecTest},
            connection=connection_test,
        )
        schema(GcCustomCodecTest)
        yield schema
        schema.drop()

    def test_custom_codec_reference_not_orphaned(self, schema_custom):
        """#1469: a live custom SchemaCodec value must be recognized as
        referenced and its file path must NOT be flagged as an orphan. Before
        the fix, _uses_schema_storage keyed on the hardcoded names object/npy,
        so this codec was never scanned and its live file was reported orphaned.

        Asserts on the specific live path (not global counts) so it is robust to
        other tests sharing the same ``local`` store.
        """
        GcCustomCodecTest.insert1({"rid": 1, "payload": b"live-payload"})

        refs = gc.scan_schema_references(schema_custom, store_name="local")
        assert refs, "custom codec's live reference must be discovered (#1469)"
        live_path = next(iter(refs))

        stats = gc.scan(schema_custom, store_name="local")
        assert live_path not in stats["orphaned_paths"], f"live custom-codec file wrongly flagged orphan: {live_path}"

    def test_custom_codec_survives_collect(self, schema_custom):
        """#1469 end-to-end data-loss guard: collect() must delete only the
        deleted row's file and keep the surviving row's file (checked by exact
        path, robust to a shared store)."""
        GcCustomCodecTest.insert1({"rid": 1, "payload": b"row-one"})
        GcCustomCodecTest.insert1({"rid": 2, "payload": b"row-two"})

        refs_all = gc.scan_schema_references(schema_custom, store_name="local")
        assert len(refs_all) == 2, f"both live rows should be referenced; got {refs_all}"

        # Delete one row — its file becomes a genuine orphan (delete-then-GC).
        (GcCustomCodecTest & {"rid": 1}).delete(prompt=False)

        refs_live = gc.scan_schema_references(schema_custom, store_name="local")
        assert len(refs_live) == 1, f"one live row should remain referenced; got {refs_live}"
        live_path = next(iter(refs_live))
        deleted_paths = refs_all - refs_live

        gc.collect(schema_custom, store_name="local", dry_run=False)

        stored_after = set(gc.list_schema_paths("local", config=schema_custom.connection._config))
        assert live_path in stored_after, f"collect() deleted the live custom-codec file {live_path} (#1469)"
        assert not (deleted_paths & stored_after), f"deleted row's orphan not reclaimed: {deleted_paths & stored_after}"


class TestDirectoryObjects:
    """Directory-valued objects (e.g. Zarr stores) — the referenced metadata
    path is a directory PREFIX whose stored form is many chunk files plus a
    `{path}.manifest.json` sidecar. Orphan matching must be coverage-based
    (exact / ancestor-prefix / manifest), not exact set difference: with exact
    matching, every chunk of a LIVE store is misclassified as an orphan and
    collect() deletes live data."""

    @pytest.fixture
    def schema_dirobj(self, connection_test, prefix, mock_stores):
        schema = dj.Schema(
            f"{prefix}_test_gc_dirobj",
            context={"GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcObjectTest)
        yield schema
        schema.drop()

    @staticmethod
    def _make_store_dir(tmp_path, name, n_chunks=3):
        d = tmp_path / name
        d.mkdir()
        (d / ".zarray").write_bytes(b"{}")
        for i in range(n_chunks):
            (d / f"0.{i}").write_bytes(f"chunk{i}".encode())
        return d

    def test_live_directory_object_not_orphaned(self, schema_dirobj, tmp_path):
        """A live folder object's chunk files and manifest must be covered by
        its referenced prefix — none may appear in orphaned_paths."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "live.zarr"))})

        refs = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs) == 1
        ref = next(iter(refs))

        stored = gc.list_schema_paths("local", config=schema_dirobj.connection._config)
        chunk_files = [p for p in stored if p.startswith(ref + "/")]
        assert len(chunk_files) >= 4, f"expected the folder's files under {ref}; got {sorted(stored)}"
        assert f"{ref}.manifest.json" in stored, "manifest sidecar must be listed (it is reclaimable state)"

        stats = gc.scan(schema_dirobj, store_name="local")
        flagged = [p for p in stats["orphaned_paths"] if p == ref or p.startswith(ref + "/") or p == f"{ref}.manifest.json"]
        assert not flagged, f"live directory object misclassified as orphaned: {flagged}"

    def test_orphaned_directory_object_fully_reclaimed(self, schema_dirobj, tmp_path):
        """After deleting one row, collect() must reclaim that folder object's
        chunks AND manifest while leaving the surviving row's folder intact."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "gone.zarr"))})
        GcObjectTest.insert1({"rid": 2, "results": str(self._make_store_dir(tmp_path, "kept.zarr"))})

        refs_all = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs_all) == 2

        (GcObjectTest & {"rid": 1}).delete(prompt=False)
        refs_live = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs_live) == 1
        live_ref = next(iter(refs_live))
        dead_ref = next(iter(refs_all - refs_live))

        gc.collect(schema_dirobj, store_name="local", dry_run=False)

        stored_after = set(gc.list_schema_paths("local", config=schema_dirobj.connection._config))
        live_files = {p for p in stored_after if p.startswith(live_ref + "/")}
        assert len(live_files) >= 4, f"live folder object lost files: {sorted(stored_after)}"
        assert f"{live_ref}.manifest.json" in stored_after, "live object's manifest must survive collect()"

        dead_files = {p for p in stored_after if p == dead_ref or p.startswith(dead_ref + "/")}
        assert not dead_files, f"orphaned folder object not reclaimed: {sorted(dead_files)}"
        assert f"{dead_ref}.manifest.json" not in stored_after, "orphaned object's manifest must be reclaimed"
