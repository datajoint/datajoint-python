"""Tests for ``safe_write`` and ``safe_copy`` and the file-store ``put_file`` built on them."""

import importlib
import threading
from pathlib import Path

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

    def test_a_copy_that_fails_midway_leaves_no_temporary_file(self, tmp_path, monkeypatch):
        utils = _utils()
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"
        dest.write_bytes(b"old")

        def fail_after_writing(src, dst):
            Path(dst).write_bytes(b"partial")
            raise OSError("disk full")

        monkeypatch.setattr(utils.shutil, "copyfile", fail_after_writing)
        with pytest.raises(OSError, match="disk full"):
            utils.safe_copy(src, dest, overwrite=True)

        assert dest.read_bytes() == b"old"
        assert _leftovers(tmp_path) == []

    def test_the_temporary_name_does_not_depend_on_the_destination_name(self, tmp_path, monkeypatch):
        # File names are limited to 255 bytes, so a target whose name is close to
        # the limit must not get a longer temporary name.
        utils = _utils()
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / ("a" * 100 + ".bin")
        seen = []
        real_copyfile = utils.shutil.copyfile

        def record(src, dst):
            seen.append(Path(dst).name)
            return real_copyfile(src, dst)

        monkeypatch.setattr(utils.shutil, "copyfile", record)
        utils.safe_copy(src, dest, overwrite=True)

        assert dest.read_bytes() == b"new"
        assert len(seen) == 1 and len(seen[0]) <= 24, seen
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


class _FakeClock:
    """Stands in for ``time`` and ``random`` inside ``datajoint.utils``, so a retry deadline needs no waiting."""

    def __init__(self):
        self.now = 0.0
        self.slept = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.slept.append(seconds)
        self.now += seconds

    @staticmethod
    def uniform(a, b):
        return 1.0


class TestReplaceRetry:
    @pytest.fixture
    def clock(self, monkeypatch):
        utils = _utils()
        clock = _FakeClock()
        monkeypatch.setattr(utils, "time", clock)
        monkeypatch.setattr(utils, "random", clock)
        return clock

    @staticmethod
    def _fail_replace(monkeypatch, failures):
        """Make ``os.replace`` raise ``PermissionError`` ``failures`` times (None: always), then work."""
        utils = _utils()
        real_replace = utils.os.replace
        calls = []

        def replace(src, dst):
            calls.append((src, dst))
            if failures is None or len(calls) <= failures:
                raise PermissionError(13, "Access is denied")
            real_replace(src, dst)

        monkeypatch.setattr(utils.os, "replace", replace)
        return calls

    def test_other_systems_do_not_retry(self, tmp_path, monkeypatch, clock):
        monkeypatch.setattr(_utils(), "_IS_WINDOWS", False)
        calls = self._fail_replace(monkeypatch, failures=None)
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")

        with pytest.raises(PermissionError):
            _utils().safe_copy(src, tmp_path / "dest.bin", overwrite=True)

        assert len(calls) == 1
        assert clock.slept == []
        assert _leftovers(tmp_path) == []

    def test_windows_retries_until_the_replace_works(self, tmp_path, monkeypatch, clock):
        monkeypatch.setattr(_utils(), "_IS_WINDOWS", True)
        calls = self._fail_replace(monkeypatch, failures=3)
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")
        dest = tmp_path / "dest.bin"

        _utils().safe_copy(src, dest, overwrite=True)

        assert len(calls) == 4
        assert len(clock.slept) == 3
        assert dest.read_bytes() == b"new"
        assert _leftovers(tmp_path) == []

    def test_windows_gives_up_at_the_deadline_with_the_original_error(self, tmp_path, monkeypatch, clock):
        monkeypatch.setattr(_utils(), "_IS_WINDOWS", True)
        self._fail_replace(monkeypatch, failures=None)
        src = tmp_path / "src.bin"
        src.write_bytes(b"new")

        with pytest.raises(PermissionError, match="Access is denied"):
            _utils().safe_copy(src, tmp_path / "dest.bin", overwrite=True)

        assert 1.0 < sum(clock.slept) <= 2.0
        assert all(b >= a for a, b in zip(clock.slept, clock.slept[1:]))  # the pause grows
        assert _leftovers(tmp_path) == []

    @pytest.mark.parametrize("is_windows", [False, True])
    def test_safe_write_accepts_a_file_another_writer_stored_first(self, tmp_path, monkeypatch, clock, is_windows):
        utils = _utils()
        monkeypatch.setattr(utils, "_IS_WINDOWS", is_windows)
        dest = tmp_path / "blob.bin"
        calls = []

        def replace(src, dst):
            calls.append(src)
            Path(dst).write_bytes(b"same content")  # the other writer finished first
            raise PermissionError(13, "Access is denied")

        monkeypatch.setattr(utils.os, "replace", replace)

        utils.safe_write(dest, b"same content")

        assert dest.read_bytes() == b"same content"
        assert len(calls) == 1  # no retry once the file is there
        assert clock.slept == []
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
