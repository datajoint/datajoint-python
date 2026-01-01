"""
Tests for garbage collection (gc.py).
"""

from unittest.mock import MagicMock, patch

import pytest

from datajoint import gc
from datajoint.errors import DataJointError


class TestUsesContentStorage:
    """Tests for _uses_content_storage helper function."""

    def test_returns_false_for_no_adapter(self):
        """Test that False is returned when attribute has no adapter."""
        attr = MagicMock()
        attr.adapter = None

        assert gc._uses_content_storage(attr) is False

    def test_returns_true_for_hash_type(self):
        """Test that True is returned for <hash@> type."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "hash"
        attr.store = "mystore"

        assert gc._uses_content_storage(attr) is True

    def test_returns_true_for_blob_external(self):
        """Test that True is returned for <blob@> type (external)."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "blob"
        attr.store = "mystore"

        assert gc._uses_content_storage(attr) is True

    def test_returns_true_for_attach_external(self):
        """Test that True is returned for <attach@> type (external)."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "attach"
        attr.store = "mystore"

        assert gc._uses_content_storage(attr) is True

    def test_returns_false_for_blob_internal(self):
        """Test that False is returned for <blob> internal storage."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "blob"
        attr.store = None

        assert gc._uses_content_storage(attr) is False


class TestExtractContentRefs:
    """Tests for _extract_content_refs helper function."""

    def test_returns_empty_for_none(self):
        """Test that empty list is returned for None value."""
        assert gc._extract_content_refs(None) == []

    def test_parses_json_string(self):
        """Test parsing JSON string with hash."""
        value = '{"hash": "abc123", "store": "mystore"}'
        refs = gc._extract_content_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("abc123", "mystore")

    def test_parses_dict_directly(self):
        """Test parsing dict with hash."""
        value = {"hash": "def456", "store": None}
        refs = gc._extract_content_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("def456", None)

    def test_returns_empty_for_invalid_json(self):
        """Test that empty list is returned for invalid JSON."""
        assert gc._extract_content_refs("not json") == []

    def test_returns_empty_for_dict_without_hash(self):
        """Test that empty list is returned for dict without hash key."""
        assert gc._extract_content_refs({"other": "data"}) == []


class TestUsesObjectStorage:
    """Tests for _uses_object_storage helper function."""

    def test_returns_false_for_no_adapter(self):
        """Test that False is returned when attribute has no adapter."""
        attr = MagicMock()
        attr.adapter = None

        assert gc._uses_object_storage(attr) is False

    def test_returns_true_for_object_type(self):
        """Test that True is returned for <object> type."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "object"

        assert gc._uses_object_storage(attr) is True

    def test_returns_false_for_other_types(self):
        """Test that False is returned for non-object types."""
        attr = MagicMock()
        attr.adapter = MagicMock()
        attr.adapter.type_name = "blob"

        assert gc._uses_object_storage(attr) is False


class TestExtractObjectRefs:
    """Tests for _extract_object_refs helper function."""

    def test_returns_empty_for_none(self):
        """Test that empty list is returned for None value."""
        assert gc._extract_object_refs(None) == []

    def test_parses_json_string(self):
        """Test parsing JSON string with path."""
        value = '{"path": "schema/table/objects/pk/field_abc123", "store": "mystore"}'
        refs = gc._extract_object_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("schema/table/objects/pk/field_abc123", "mystore")

    def test_parses_dict_directly(self):
        """Test parsing dict with path."""
        value = {"path": "test/path", "store": None}
        refs = gc._extract_object_refs(value)

        assert len(refs) == 1
        assert refs[0] == ("test/path", None)

    def test_returns_empty_for_dict_without_path(self):
        """Test that empty list is returned for dict without path key."""
        assert gc._extract_object_refs({"other": "data"}) == []


class TestScan:
    """Tests for scan function."""

    def test_requires_at_least_one_schema(self):
        """Test that at least one schema is required."""
        with pytest.raises(DataJointError, match="At least one schema must be provided"):
            gc.scan()

    @patch("datajoint.gc.scan_object_references")
    @patch("datajoint.gc.list_stored_objects")
    @patch("datajoint.gc.scan_references")
    @patch("datajoint.gc.list_stored_content")
    def test_returns_stats(self, mock_list_content, mock_scan_refs, mock_list_objects, mock_scan_objects):
        """Test that scan returns proper statistics."""
        # Mock content-addressed storage
        mock_scan_refs.return_value = {"hash1", "hash2"}
        mock_list_content.return_value = {
            "hash1": 100,
            "hash3": 200,  # orphaned
        }

        # Mock path-addressed storage
        mock_scan_objects.return_value = {"path/to/obj1"}
        mock_list_objects.return_value = {
            "path/to/obj1": 500,
            "path/to/obj2": 300,  # orphaned
        }

        mock_schema = MagicMock()
        stats = gc.scan(mock_schema, store_name="test_store")

        # Content stats
        assert stats["content_referenced"] == 2
        assert stats["content_stored"] == 2
        assert stats["content_orphaned"] == 1
        assert "hash3" in stats["orphaned_hashes"]

        # Object stats
        assert stats["object_referenced"] == 1
        assert stats["object_stored"] == 2
        assert stats["object_orphaned"] == 1
        assert "path/to/obj2" in stats["orphaned_paths"]

        # Combined totals
        assert stats["referenced"] == 3
        assert stats["stored"] == 4
        assert stats["orphaned"] == 2
        assert stats["orphaned_bytes"] == 500  # 200 content + 300 object


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
            "orphaned_hashes": ["orphan_hash"],
            "orphaned_paths": [],
            "content_orphaned": 1,
            "object_orphaned": 0,
        }

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=True)

        assert stats["deleted"] == 0
        assert stats["bytes_freed"] == 0
        assert stats["dry_run"] is True

    @patch("datajoint.gc.delete_content")
    @patch("datajoint.gc.list_stored_content")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_content(self, mock_scan, mock_list_stored, mock_delete):
        """Test that orphaned content is deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 100,
            "orphaned_hashes": ["orphan_hash"],
            "orphaned_paths": [],
            "content_orphaned": 1,
            "object_orphaned": 0,
        }
        mock_list_stored.return_value = {"orphan_hash": 100}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["content_deleted"] == 1
        assert stats["bytes_freed"] == 100
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("orphan_hash", "test_store")

    @patch("datajoint.gc.delete_object")
    @patch("datajoint.gc.list_stored_objects")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_objects(self, mock_scan, mock_list_objects, mock_delete):
        """Test that orphaned objects are deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 500,
            "orphaned_hashes": [],
            "orphaned_paths": ["path/to/orphan"],
            "content_orphaned": 0,
            "object_orphaned": 1,
        }
        mock_list_objects.return_value = {"path/to/orphan": 500}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["object_deleted"] == 1
        assert stats["bytes_freed"] == 500
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("path/to/orphan", "test_store")


class TestFormatStats:
    """Tests for format_stats function."""

    def test_formats_scan_stats(self):
        """Test formatting scan statistics."""
        stats = {
            "referenced": 10,
            "stored": 15,
            "orphaned": 5,
            "orphaned_bytes": 1024 * 1024,  # 1 MB
            "content_referenced": 6,
            "content_stored": 8,
            "content_orphaned": 2,
            "content_orphaned_bytes": 512 * 1024,
            "object_referenced": 4,
            "object_stored": 7,
            "object_orphaned": 3,
            "object_orphaned_bytes": 512 * 1024,
        }

        result = gc.format_stats(stats)

        assert "Referenced in database: 10" in result
        assert "Stored in backend:      15" in result
        assert "Orphaned (unreferenced): 5" in result
        assert "1.00 MB" in result
        # Check for detailed sections
        assert "Content-Addressed Storage" in result
        assert "Path-Addressed Storage" in result

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
            "content_deleted": 2,
            "object_deleted": 1,
            "bytes_freed": 2 * 1024 * 1024,  # 2 MB
            "errors": 2,
            "dry_run": False,
        }

        result = gc.format_stats(stats)

        assert "Deleted:     3" in result
        assert "Content: 2" in result
        assert "Objects: 1" in result
        assert "2.00 MB" in result
        assert "Errors:      2" in result
