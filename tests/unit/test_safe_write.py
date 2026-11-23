"""Tests for ``safe_write`` and ``safe_copy`` and the file-store ``put_file`` built on them."""

import importlib
import threading

import pytest


def _utils():
    # Some unit tests purge ``datajoint`` from ``sys.modules``; bind the live module.
    return importlib.import_module("datajoint.utils")


def _leftovers(folder):
    """Entries other than the files a test created on purpose."""
    return sorted(p.name for p in folder.iterdir() if p.name.endswith((".tmp", ".copying", ".saving")))


class TestSafeCopy:
    def test_copies_to_a_new_destination(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "sub" / "dest.bin"

        _utils().safe_copy(src, dest)

        assert dest.read_bytes() == b"new"
        assert _leftovers(dest.parent) == []

    def test_overwrite_replaces_an_existing_destination(self, tmp_path):
        # On Windows Path.rename raises FileExistsError when the target exists.
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"old")

        _utils().safe_copy(src, dest, overwrite=True)

        assert dest.read_bytes() == b"new"
        assert _leftovers(tmp_path) == []

    def test_without_overwrite_an_existing_destination_is_kept(self, tmp_path):
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"old")

        _utils().safe_copy(src, dest)

        assert dest.read_bytes() == b"old"

    def test_a_failed_copy_leaves_no_temporary_file(self, tmp_path):
        dest = tmp_path / "dest.bin"

        with pytest.raises(FileNotFoundError):
            _utils().safe_copy(tmp_path / "missing.bin", dest, overwrite=True)

        assert not dest.exists()
        assert _leftovers(tmp_path) == []

    def test_concurrent_writers_to_one_destination_all_succeed(self, tmp_path):
        # A fixed temporary name made the writers share one file: one could
        # truncate it while another renamed it into place.
        dest = tmp_path / "dest.bin"
        sources = []
        for i in range(8):
            src = tmp_path / f"src{i}.bin"
            src.write_bytes(bytes([i]) * 200_000)
            sources.append(src)
        errors = []
        barrier = threading.Barrier(len(sources))

        def write(src):
            try:
                barrier.wait()
                _utils().safe_copy(src, dest, overwrite=True)
            except BaseException as exc:  # noqa: BLE001 - reported by the test
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(src,)) for src in sources]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        data = dest.read_bytes()
        assert len(data) == 200_000
        assert data in {bytes([i]) * 200_000 for i in range(8)}
        assert _leftovers(tmp_path) == []


class TestSafeWrite:
    def test_writes_a_new_file(self, tmp_path):
        dest = tmp_path / "sub" / "blob.bin"

        _utils().safe_write(dest, b"data")

        assert dest.read_bytes() == b"data"
        assert _leftovers(dest.parent) == []

    def test_an_existing_file_is_kept(self, tmp_path):
        dest = tmp_path / "blob.bin"
        dest.write_bytes(b"old")

        _utils().safe_write(dest, b"new")

        assert dest.read_bytes() == b"old"

    def test_concurrent_writers_to_one_destination_all_succeed(self, tmp_path):
        dest = tmp_path / "blob.bin"
        errors = []
        barrier = threading.Barrier(8)

        def write(i):
            try:
                barrier.wait()
                _utils().safe_write(dest, bytes([i]) * 200_000)
            except BaseException as exc:  # noqa: BLE001 - reported by the test
                errors.append(exc)

        threads = [threading.Thread(target=write, args=(i,)) for i in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == []
        assert len(dest.read_bytes()) == 200_000
        assert _leftovers(tmp_path) == []


def test_put_file_overwrites_an_existing_file_in_a_file_store(tmp_path):
    StorageBackend = importlib.import_module("datajoint.storage").StorageBackend
    root = tmp_path / "store"
    root.mkdir()
    local = tmp_path / "a.bin"
    local.write_bytes(b"first")
    backend = StorageBackend({"protocol": "file", "location": str(root)})

    backend.put_file(local, "x/a.bin")
    local.write_bytes(b"second")
    backend.put_file(local, "x/a.bin")

    assert (root / "x" / "a.bin").read_bytes() == b"second"
