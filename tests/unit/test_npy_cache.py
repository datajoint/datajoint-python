"""Tests for the cache directory used by NpyRef.load(mmap_mode=...) on remote stores."""

import os
import stat
import tempfile

import numpy as np
import pytest

from datajoint.builtin_codecs import npy
from datajoint.builtin_codecs.npy import NpyRef

posix_only = pytest.mark.skipif(not hasattr(os, "getuid"), reason="requires POSIX file ownership")


@pytest.fixture
def tempdir(monkeypatch, tmp_path):
    """Point tempfile.gettempdir() at tmp_path and reset the per-process fallback."""
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    monkeypatch.setattr(npy, "_fallback_mmap_cache_dir", None)
    return tmp_path


@posix_only
def test_cache_dir_is_per_user_and_private(tempdir):
    cache_dir = npy._mmap_cache_dir()

    assert cache_dir == tempdir / f"datajoint_mmap_{os.getuid()}"
    st = os.lstat(cache_dir)
    assert stat.S_ISDIR(st.st_mode)
    assert stat.S_IMODE(st.st_mode) == 0o700
    assert st.st_uid == os.getuid()


@posix_only
def test_existing_cache_dir_permissions_tightened(tempdir):
    cache_dir = tempdir / f"datajoint_mmap_{os.getuid()}"
    cache_dir.mkdir()
    os.chmod(cache_dir, 0o777)

    assert npy._mmap_cache_dir() == cache_dir
    assert stat.S_IMODE(os.lstat(cache_dir).st_mode) == 0o700


@posix_only
def test_foreign_cache_dir_not_used(tempdir, monkeypatch, caplog):
    """A directory at the per-user path owned by someone else is replaced by a new private one."""
    cache_dir = tempdir / f"datajoint_mmap_{os.getuid()}"
    cache_dir.mkdir(mode=0o777)
    real_lstat = os.lstat

    def fake_lstat(path, *args, **kwargs):
        st = real_lstat(path, *args, **kwargs)
        if os.fspath(path) == os.fspath(cache_dir):
            fields = list(st)
            fields[stat.ST_UID] = os.getuid() + 54321
            return os.stat_result(fields)
        return st

    monkeypatch.setattr(os, "lstat", fake_lstat)

    with caplog.at_level("WARNING", logger="datajoint"):
        used = npy._mmap_cache_dir()

    assert used != cache_dir
    assert used.parent == tempdir
    st = real_lstat(used)
    assert stat.S_IMODE(st.st_mode) == 0o700
    assert st.st_uid == os.getuid()
    assert any("not a directory owned by the current user" in r.message for r in caplog.records)
    # The replacement is reused for the rest of the session
    assert npy._mmap_cache_dir() == used


@posix_only
def test_symlink_at_cache_path_not_followed(tempdir):
    elsewhere = tempdir / "elsewhere"
    elsewhere.mkdir()
    cache_dir = tempdir / f"datajoint_mmap_{os.getuid()}"
    cache_dir.symlink_to(elsewhere)

    used = npy._mmap_cache_dir()

    assert used not in (cache_dir, elsewhere)
    assert stat.S_IMODE(os.lstat(used).st_mode) == 0o700


def test_no_posix_ownership_uses_shared_name(tempdir, monkeypatch):
    """Without os.getuid (Windows) the per-user temp directory holds datajoint_mmap."""
    monkeypatch.delattr(os, "getuid", raising=False)

    assert npy._mmap_cache_dir() == tempdir / "datajoint_mmap"
    assert (tempdir / "datajoint_mmap").is_dir()


def test_remote_mmap_load_uses_cache_dir(tempdir, tmp_path):
    arr = np.arange(5, dtype=np.int32)
    np.save(tmp_path / "src.npy", arr)
    payload = (tmp_path / "src.npy").read_bytes()

    class RemoteBackend:
        protocol = "s3"

        def get_buffer(self, path):
            return payload

    meta = {"path": "schema/table/key/arr.npy", "store": "s3-store", "dtype": "int32", "shape": [5]}
    loaded = NpyRef(meta, RemoteBackend()).load(mmap_mode="r")

    np.testing.assert_array_equal(loaded, arr)
    cached = list(npy._mmap_cache_dir().glob("*.npy"))
    assert len(cached) == 1
