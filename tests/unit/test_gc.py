"""Tests for garbage collection module."""

from pathlib import PurePosixPath, PureWindowsPath
from unittest.mock import MagicMock

import pytest

from datajoint import storage
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
