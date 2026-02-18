"""
Tests for garbage collection (gc.py).
"""

from unittest.mock import MagicMock, patch

import pytest

from datajoint import gc
from datajoint.errors import DataJointError


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


class TestExtractHashRefs:
    """Tests for _extract_hash_refs helper function."""

    def test_returns_empty_for_none(self):
        """Test that empty list is returned for None value."""
        assert gc._extract_hash_refs(None) == []

    def test_parses_json_string(self):
        """Test parsing JSON string with path."""
        value = '{"path": "_hash/schema/abc123", "hash": "abc123", "store": "mystore"}'
        refs = gc._extract_hash_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("_hash/schema/abc123", "mystore")

    def test_parses_dict_directly(self):
        """Test parsing dict with path."""
        value = {"path": "_hash/schema/def456", "hash": "def456", "store": None}
        refs = gc._extract_hash_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("_hash/schema/def456", None)

    def test_returns_empty_for_invalid_json(self):
        """Test that empty list is returned for invalid JSON."""
        assert gc._extract_hash_refs("not json") == []

    def test_returns_empty_for_dict_without_path(self):
        """Test that empty list is returned for dict without path key."""
        assert gc._extract_hash_refs({"hash": "abc123"}) == []


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
        attr.codec = MagicMock()
        attr.codec.name = "object"

        assert gc._uses_schema_storage(attr) is True

    def test_returns_true_for_npy_type(self):
        """Test that True is returned for <npy@> type."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "npy"

        assert gc._uses_schema_storage(attr) is True

    def test_returns_false_for_other_types(self):
        """Test that False is returned for non-schema-addressed types."""
        attr = MagicMock()
        attr.codec = MagicMock()
        attr.codec.name = "blob"

        assert gc._uses_schema_storage(attr) is False


class TestExtractSchemaRefs:
    """Tests for _extract_schema_refs helper function."""

    def test_returns_empty_for_none(self):
        """Test that empty list is returned for None value."""
        assert gc._extract_schema_refs(None) == []

    def test_parses_json_string(self):
        """Test parsing JSON string with path."""
        value = '{"path": "schema/table/pk/field", "store": "mystore"}'
        refs = gc._extract_schema_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("schema/table/pk/field", "mystore")

    def test_parses_dict_directly(self):
        """Test parsing dict with path."""
        value = {"path": "test/path", "store": None}
        refs = gc._extract_schema_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("test/path", None)

    def test_returns_empty_for_dict_without_path(self):
        """Test that empty list is returned for dict without path key."""
        assert gc._extract_schema_refs({"other": "data"}) == []


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
        mock_delete.assert_called_once_with(
            "_hash/schema/orphan_path", "test_store", config=mock_schema.connection._config
        )

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
        mock_delete.assert_called_once_with(
            "schema/table/pk/field", "test_store", config=mock_schema.connection._config
        )


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
