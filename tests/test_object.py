"""
Tests for the object column type.

Tests cover:
- Storage path generation
- Insert with file, folder, and stream
- Fetch returning ObjectRef
- ObjectRef methods (read, open, download, listdir, walk, verify)
- Staged insert
- Error cases
"""

import io
import json
import os
from pathlib import Path

import pytest

import datajoint as dj
from datajoint.objectref import ObjectRef
from datajoint.storage import build_object_path, generate_token, encode_pk_value

from .schema_object import ObjectFile, ObjectFolder, ObjectMultiple, ObjectWithOther


class TestStoragePathGeneration:
    """Tests for storage path generation utilities."""

    def test_generate_token_default_length(self):
        """Test token generation with default length."""
        token = generate_token()
        assert len(token) == 8
        # All characters should be URL-safe
        safe_chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
        assert all(c in safe_chars for c in token)

    def test_generate_token_custom_length(self):
        """Test token generation with custom length."""
        token = generate_token(12)
        assert len(token) == 12

    def test_generate_token_minimum_length(self):
        """Test token generation respects minimum length."""
        token = generate_token(2)  # Below minimum
        assert len(token) == 4  # Should be clamped to minimum

    def test_generate_token_maximum_length(self):
        """Test token generation respects maximum length."""
        token = generate_token(20)  # Above maximum
        assert len(token) == 16  # Should be clamped to maximum

    def test_generate_token_uniqueness(self):
        """Test that generated tokens are unique."""
        tokens = [generate_token() for _ in range(100)]
        assert len(set(tokens)) == 100

    def test_encode_pk_value_integer(self):
        """Test encoding integer primary key values."""
        assert encode_pk_value(123) == "123"
        assert encode_pk_value(0) == "0"
        assert encode_pk_value(-5) == "-5"

    def test_encode_pk_value_string(self):
        """Test encoding string primary key values."""
        assert encode_pk_value("simple") == "simple"
        assert encode_pk_value("test_value") == "test_value"

    def test_encode_pk_value_unsafe_chars(self):
        """Test encoding strings with unsafe characters."""
        # Slash should be URL-encoded
        result = encode_pk_value("path/to/file")
        assert "/" not in result or result == "path%2Fto%2Ffile"

    def test_build_object_path_basic(self):
        """Test basic object path building."""
        path, token = build_object_path(
            schema="myschema",
            table="MyTable",
            field="data_file",
            primary_key={"id": 123},
            ext=".dat",
        )
        assert "myschema" in path
        assert "MyTable" in path
        assert "objects" in path
        assert "id=123" in path
        assert "data_file_" in path
        assert path.endswith(".dat")
        assert len(token) == 8

    def test_build_object_path_no_extension(self):
        """Test object path building without extension."""
        path, token = build_object_path(
            schema="myschema",
            table="MyTable",
            field="data_folder",
            primary_key={"id": 456},
            ext=None,
        )
        assert not path.endswith(".")
        assert "data_folder_" in path

    def test_build_object_path_multiple_pk(self):
        """Test object path with multiple primary key attributes."""
        path, token = build_object_path(
            schema="myschema",
            table="MyTable",
            field="raw_data",
            primary_key={"subject_id": 1, "session_id": 2},
            ext=".zarr",
        )
        assert "subject_id=1" in path
        assert "session_id=2" in path

    def test_build_object_path_with_partition(self):
        """Test object path with partition pattern."""
        path, token = build_object_path(
            schema="myschema",
            table="MyTable",
            field="data",
            primary_key={"subject_id": 1, "session_id": 2},
            ext=".dat",
            partition_pattern="{subject_id}",
        )
        # subject_id should be at the beginning due to partition
        assert path.startswith("subject_id=1")


class TestObjectRef:
    """Tests for ObjectRef class."""

    def test_from_json_string(self):
        """Test creating ObjectRef from JSON string."""
        json_str = json.dumps(
            {
                "path": "schema/Table/objects/id=1/data_abc123.dat",
                "size": 1024,
                "hash": None,
                "ext": ".dat",
                "is_dir": False,
                "timestamp": "2025-01-15T10:30:00+00:00",
            }
        )
        obj = ObjectRef.from_json(json_str)
        assert obj.path == "schema/Table/objects/id=1/data_abc123.dat"
        assert obj.size == 1024
        assert obj.hash is None
        assert obj.ext == ".dat"
        assert obj.is_dir is False

    def test_from_json_dict(self):
        """Test creating ObjectRef from dict."""
        data = {
            "path": "schema/Table/objects/id=1/data_abc123.zarr",
            "size": 5678,
            "hash": None,
            "ext": ".zarr",
            "is_dir": True,
            "timestamp": "2025-01-15T10:30:00+00:00",
            "item_count": 42,
        }
        obj = ObjectRef.from_json(data)
        assert obj.path == "schema/Table/objects/id=1/data_abc123.zarr"
        assert obj.size == 5678
        assert obj.is_dir is True
        assert obj.item_count == 42

    def test_from_json_zarr_style(self):
        """Test creating ObjectRef from Zarr-style JSON with null size."""
        data = {
            "path": "schema/Recording/objects/id=1/neural_data_abc123.zarr",
            "size": None,
            "hash": None,
            "ext": ".zarr",
            "is_dir": True,
            "timestamp": "2025-01-15T10:30:00+00:00",
        }
        obj = ObjectRef.from_json(data)
        assert obj.path == "schema/Recording/objects/id=1/neural_data_abc123.zarr"
        assert obj.size is None
        assert obj.hash is None
        assert obj.ext == ".zarr"
        assert obj.is_dir is True
        assert obj.item_count is None

    def test_to_json(self):
        """Test converting ObjectRef to JSON dict."""
        from datetime import datetime, timezone

        obj = ObjectRef(
            path="schema/Table/objects/id=1/data.dat",
            size=1024,
            hash=None,
            ext=".dat",
            is_dir=False,
            timestamp=datetime(2025, 1, 15, 10, 30, tzinfo=timezone.utc),
        )
        data = obj.to_json()
        assert data["path"] == "schema/Table/objects/id=1/data.dat"
        assert data["size"] == 1024
        assert data["is_dir"] is False

    def test_repr_file(self):
        """Test string representation for file."""
        from datetime import datetime, timezone

        obj = ObjectRef(
            path="test/path.dat",
            size=1024,
            hash=None,
            ext=".dat",
            is_dir=False,
            timestamp=datetime.now(timezone.utc),
        )
        assert "file" in repr(obj)
        assert "test/path.dat" in repr(obj)

    def test_repr_folder(self):
        """Test string representation for folder."""
        from datetime import datetime, timezone

        obj = ObjectRef(
            path="test/folder.zarr",
            size=5678,
            hash=None,
            ext=".zarr",
            is_dir=True,
            timestamp=datetime.now(timezone.utc),
        )
        assert "folder" in repr(obj)

    def test_str(self):
        """Test str() returns path."""
        from datetime import datetime, timezone

        obj = ObjectRef(
            path="my/path/to/data.dat",
            size=100,
            hash=None,
            ext=".dat",
            is_dir=False,
            timestamp=datetime.now(timezone.utc),
        )
        assert str(obj) == "my/path/to/data.dat"


class TestObjectInsertFile:
    """Tests for inserting files with object type."""

    def test_insert_file(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test inserting a file."""
        table = ObjectFile()

        # Create a test file
        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "test_data.dat")
        data = os.urandom(1024)
        with test_file.open("wb") as f:
            f.write(data)

        # Insert the file
        table.insert1({"file_id": 1, "data_file": str(test_file)})

        # Verify record was inserted
        assert len(table) == 1

        # Cleanup
        table.delete()

    def test_insert_file_with_extension(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test that file extension is preserved."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "data.csv")
        test_file.write_text("a,b,c\n1,2,3\n")

        table.insert1({"file_id": 2, "data_file": str(test_file)})

        # Fetch and check extension in metadata
        record = table.fetch1()
        obj = record["data_file"]
        assert obj.ext == ".csv"

        table.delete()

    def test_insert_file_nonexistent(self, schema_obj, mock_object_storage):
        """Test that inserting nonexistent file raises error."""
        table = ObjectFile()

        with pytest.raises(dj.DataJointError, match="not found"):
            table.insert1({"file_id": 3, "data_file": "/nonexistent/path/file.dat"})


class TestObjectInsertFolder:
    """Tests for inserting folders with object type."""

    def test_insert_folder(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test inserting a folder."""
        table = ObjectFolder()

        # Create a test folder with files
        source_folder = tmpdir_factory.mktemp("source")
        data_folder = Path(source_folder, "data_folder")
        data_folder.mkdir()

        # Add some files
        (data_folder / "file1.txt").write_text("content1")
        (data_folder / "file2.txt").write_text("content2")
        subdir = data_folder / "subdir"
        subdir.mkdir()
        (subdir / "file3.txt").write_text("content3")

        # Insert the folder
        table.insert1({"folder_id": 1, "data_folder": str(data_folder)})

        assert len(table) == 1

        # Fetch and verify
        record = table.fetch1()
        obj = record["data_folder"]
        assert obj.is_dir is True
        assert obj.item_count == 3  # 3 files

        table.delete()


class TestObjectInsertStream:
    """Tests for inserting from streams with object type."""

    def test_insert_stream(self, schema_obj, mock_object_storage):
        """Test inserting from a stream."""
        table = ObjectFile()

        # Create a BytesIO stream
        data = b"This is test data from a stream"
        stream = io.BytesIO(data)

        # Insert with extension and stream tuple
        table.insert1({"file_id": 10, "data_file": (".txt", stream)})

        assert len(table) == 1

        # Fetch and verify extension
        record = table.fetch1()
        obj = record["data_file"]
        assert obj.ext == ".txt"
        assert obj.size == len(data)

        table.delete()


class TestObjectFetch:
    """Tests for fetching object type attributes."""

    def test_fetch_returns_objectref(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test that fetch returns ObjectRef."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "test.dat")
        test_file.write_bytes(os.urandom(512))

        table.insert1({"file_id": 20, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        assert isinstance(obj, ObjectRef)
        assert obj.size == 512
        assert obj.is_dir is False

        table.delete()

    def test_fetch_metadata_no_io(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test that accessing metadata does not perform I/O."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "test.dat")
        test_file.write_bytes(os.urandom(256))

        table.insert1({"file_id": 21, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        # These should all work without I/O
        assert obj.path is not None
        assert obj.size == 256
        assert obj.ext == ".dat"
        assert obj.is_dir is False
        assert obj.timestamp is not None

        table.delete()


class TestObjectRefOperations:
    """Tests for ObjectRef file operations."""

    def test_read_file(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test reading file content via ObjectRef."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "readable.dat")
        original_data = os.urandom(128)
        test_file.write_bytes(original_data)

        table.insert1({"file_id": 30, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        # Read content
        content = obj.read()
        assert content == original_data

        table.delete()

    def test_open_file(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test opening file via ObjectRef."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "openable.txt")
        test_file.write_text("Hello, World!")

        table.insert1({"file_id": 31, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        # Open and read
        with obj.open(mode="rb") as f:
            content = f.read()
        assert content == b"Hello, World!"

        table.delete()

    def test_download_file(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test downloading file via ObjectRef."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "downloadable.dat")
        original_data = os.urandom(256)
        test_file.write_bytes(original_data)

        table.insert1({"file_id": 32, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        # Download to new location
        download_folder = tmpdir_factory.mktemp("download")
        local_path = obj.download(download_folder)

        assert Path(local_path).exists()
        assert Path(local_path).read_bytes() == original_data

        table.delete()

    def test_exists(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test exists() method."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "exists.dat")
        test_file.write_bytes(b"data")

        table.insert1({"file_id": 33, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        assert obj.exists() is True

        table.delete()


class TestObjectRefFolderOperations:
    """Tests for ObjectRef folder operations."""

    def test_listdir(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test listing folder contents."""
        table = ObjectFolder()

        source_folder = tmpdir_factory.mktemp("source")
        data_folder = Path(source_folder, "listable")
        data_folder.mkdir()
        (data_folder / "a.txt").write_text("a")
        (data_folder / "b.txt").write_text("b")
        (data_folder / "c.txt").write_text("c")

        table.insert1({"folder_id": 40, "data_folder": str(data_folder)})

        record = table.fetch1()
        obj = record["data_folder"]

        contents = obj.listdir()
        assert len(contents) == 3
        assert "a.txt" in contents
        assert "b.txt" in contents
        assert "c.txt" in contents

        table.delete()

    def test_walk(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test walking folder tree."""
        table = ObjectFolder()

        source_folder = tmpdir_factory.mktemp("source")
        data_folder = Path(source_folder, "walkable")
        data_folder.mkdir()
        (data_folder / "root.txt").write_text("root")
        subdir = data_folder / "subdir"
        subdir.mkdir()
        (subdir / "nested.txt").write_text("nested")

        table.insert1({"folder_id": 41, "data_folder": str(data_folder)})

        record = table.fetch1()
        obj = record["data_folder"]

        # Collect walk results
        walk_results = list(obj.walk())
        assert len(walk_results) >= 1

        table.delete()

    def test_open_subpath(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test opening file within folder using subpath."""
        table = ObjectFolder()

        source_folder = tmpdir_factory.mktemp("source")
        data_folder = Path(source_folder, "subpathable")
        data_folder.mkdir()
        (data_folder / "inner.txt").write_text("inner content")

        table.insert1({"folder_id": 42, "data_folder": str(data_folder)})

        record = table.fetch1()
        obj = record["data_folder"]

        with obj.open("inner.txt", mode="rb") as f:
            content = f.read()
        assert content == b"inner content"

        table.delete()

    def test_read_on_folder_raises(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test that read() on folder raises error."""
        table = ObjectFolder()

        source_folder = tmpdir_factory.mktemp("source")
        data_folder = Path(source_folder, "folder")
        data_folder.mkdir()
        (data_folder / "file.txt").write_text("content")

        table.insert1({"folder_id": 43, "data_folder": str(data_folder)})

        record = table.fetch1()
        obj = record["data_folder"]

        with pytest.raises(dj.DataJointError, match="Cannot read"):
            obj.read()

        table.delete()

    def test_listdir_on_file_raises(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test that listdir() on file raises error."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "file.dat")
        test_file.write_bytes(b"data")

        table.insert1({"file_id": 44, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        with pytest.raises(dj.DataJointError, match="Cannot listdir"):
            obj.listdir()

        table.delete()


class TestObjectMultiple:
    """Tests for tables with multiple object attributes."""

    def test_multiple_objects(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test inserting multiple object attributes."""
        table = ObjectMultiple()

        source_folder = tmpdir_factory.mktemp("source")
        raw_file = Path(source_folder, "raw.dat")
        raw_file.write_bytes(os.urandom(100))
        processed_file = Path(source_folder, "processed.dat")
        processed_file.write_bytes(os.urandom(200))

        table.insert1(
            {
                "record_id": 1,
                "raw_data": str(raw_file),
                "processed": str(processed_file),
            }
        )

        record = table.fetch1()
        raw_obj = record["raw_data"]
        processed_obj = record["processed"]

        assert raw_obj.size == 100
        assert processed_obj.size == 200
        assert raw_obj.path != processed_obj.path

        table.delete()


class TestObjectWithOtherAttributes:
    """Tests for object type mixed with other attributes."""

    def test_object_with_other(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test table with object and other attribute types."""
        table = ObjectWithOther()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "data.bin")
        test_file.write_bytes(os.urandom(64))

        table.insert1(
            {
                "subject_id": 1,
                "session_id": 1,
                "name": "Test Session",
                "data_file": str(test_file),
                "notes": "Some notes here",
            }
        )

        record = table.fetch1()
        assert record["name"] == "Test Session"
        assert record["notes"] == "Some notes here"
        assert isinstance(record["data_file"], ObjectRef)
        assert record["data_file"].size == 64

        table.delete()


class TestObjectVerify:
    """Tests for ObjectRef verification."""

    def test_verify_file(self, schema_obj, mock_object_storage, tmpdir_factory):
        """Test verifying file integrity."""
        table = ObjectFile()

        source_folder = tmpdir_factory.mktemp("source")
        test_file = Path(source_folder, "verifiable.dat")
        test_file.write_bytes(os.urandom(128))

        table.insert1({"file_id": 50, "data_file": str(test_file)})

        record = table.fetch1()
        obj = record["data_file"]

        # Should not raise
        assert obj.verify() is True

        table.delete()


class TestStagedInsert:
    """Tests for staged insert operations."""

    def test_staged_insert_basic(self, schema_obj, mock_object_storage):
        """Test basic staged insert."""
        table = ObjectFile()

        with table.staged_insert1 as staged:
            staged.rec["file_id"] = 60

            # Write directly to storage
            with staged.open("data_file", ".dat") as f:
                f.write(b"staged data content")

            # No need to assign - metadata computed on exit

        # Verify record was inserted
        assert len(table) == 1
        record = table.fetch1()
        obj = record["data_file"]
        assert obj.ext == ".dat"

        table.delete()

    def test_staged_insert_exception_cleanup(self, schema_obj, mock_object_storage):
        """Test that staged insert cleans up on exception."""
        table = ObjectFile()

        try:
            with table.staged_insert1 as staged:
                staged.rec["file_id"] = 61

                with staged.open("data_file", ".dat") as f:
                    f.write(b"will be cleaned up")

                raise ValueError("Simulated error")
        except ValueError:
            pass

        # No record should be inserted
        assert len(table) == 0

    def test_staged_insert_store_method(self, schema_obj, mock_object_storage):
        """Test staged insert store() method returns FSMap."""
        import fsspec

        table = ObjectFile()

        with table.staged_insert1 as staged:
            staged.rec["file_id"] = 62

            store = staged.store("data_file", ".zarr")
            assert isinstance(store, fsspec.FSMap)

            # Write some data
            store["test_key"] = b"test_value"

        assert len(table) == 1

        table.delete()

    def test_staged_insert_fs_property(self, schema_obj, mock_object_storage):
        """Test staged insert fs property returns filesystem."""
        import fsspec

        table = ObjectFile()

        with table.staged_insert1 as staged:
            staged.rec["file_id"] = 63

            fs = staged.fs
            assert isinstance(fs, fsspec.AbstractFileSystem)

            # Just open and write to test fs works
            with staged.open("data_file", ".txt") as f:
                f.write(b"test")

        table.delete()

    def test_staged_insert_missing_pk_raises(self, schema_obj, mock_object_storage):
        """Test that staged insert raises if PK not set before store()."""
        table = ObjectFile()

        with pytest.raises(dj.DataJointError, match="Primary key"):
            with table.staged_insert1 as staged:
                # Don't set primary key
                staged.store("data_file", ".dat")


class TestRemoteURLSupport:
    """Tests for remote URL detection and parsing."""

    def test_is_remote_url_s3(self):
        """Test S3 URL detection."""
        from datajoint.storage import is_remote_url

        assert is_remote_url("s3://bucket/path/file.dat") is True
        assert is_remote_url("S3://bucket/path/file.dat") is True

    def test_is_remote_url_gcs(self):
        """Test GCS URL detection."""
        from datajoint.storage import is_remote_url

        assert is_remote_url("gs://bucket/path/file.dat") is True
        assert is_remote_url("gcs://bucket/path/file.dat") is True

    def test_is_remote_url_azure(self):
        """Test Azure URL detection."""
        from datajoint.storage import is_remote_url

        assert is_remote_url("az://container/path/file.dat") is True
        assert is_remote_url("abfs://container/path/file.dat") is True

    def test_is_remote_url_http(self):
        """Test HTTP/HTTPS URL detection."""
        from datajoint.storage import is_remote_url

        assert is_remote_url("http://example.com/path/file.dat") is True
        assert is_remote_url("https://example.com/path/file.dat") is True

    def test_is_remote_url_local_path(self):
        """Test local paths are not detected as remote."""
        from datajoint.storage import is_remote_url

        assert is_remote_url("/local/path/file.dat") is False
        assert is_remote_url("relative/path/file.dat") is False
        assert is_remote_url("C:\\Windows\\path\\file.dat") is False

    def test_is_remote_url_non_string(self):
        """Test non-string inputs return False."""
        from datajoint.storage import is_remote_url

        assert is_remote_url(None) is False
        assert is_remote_url(123) is False
        assert is_remote_url(Path("/local/path")) is False

    def test_parse_remote_url_s3(self):
        """Test S3 URL parsing."""
        from datajoint.storage import parse_remote_url

        protocol, path = parse_remote_url("s3://bucket/path/file.dat")
        assert protocol == "s3"
        assert path == "bucket/path/file.dat"

    def test_parse_remote_url_gcs(self):
        """Test GCS URL parsing."""
        from datajoint.storage import parse_remote_url

        protocol, path = parse_remote_url("gs://bucket/path/file.dat")
        assert protocol == "gcs"
        assert path == "bucket/path/file.dat"

        protocol, path = parse_remote_url("gcs://bucket/path/file.dat")
        assert protocol == "gcs"
        assert path == "bucket/path/file.dat"

    def test_parse_remote_url_azure(self):
        """Test Azure URL parsing."""
        from datajoint.storage import parse_remote_url

        protocol, path = parse_remote_url("az://container/path/file.dat")
        assert protocol == "abfs"
        assert path == "container/path/file.dat"

    def test_parse_remote_url_http(self):
        """Test HTTP URL parsing."""
        from datajoint.storage import parse_remote_url

        protocol, path = parse_remote_url("https://example.com/path/file.dat")
        assert protocol == "https"
        assert path == "example.com/path/file.dat"

    def test_parse_remote_url_unsupported(self):
        """Test unsupported protocol raises error."""
        from datajoint.storage import parse_remote_url

        with pytest.raises(dj.DataJointError, match="Unsupported remote URL"):
            parse_remote_url("ftp://server/path/file.dat")
