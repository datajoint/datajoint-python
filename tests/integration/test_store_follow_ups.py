"""
Store follow-ups (#1713).

- ``<filepath@>`` with no store named checks, and records, ``stores.filepath_default``
  even when ``stores.default`` names a different store.
- A folder object in an S3 store downloads with every file under its own name,
  including keys that contain glob characters, on any supported fsspec version.
"""

import time

import fsspec
import pytest

import datajoint as dj
from datajoint.objectref import ObjectRef
from datajoint.storage import StorageBackend


class FilepathDefault(dj.Manual):
    definition = """
    ref_id : int32
    ---
    file_ref : <filepath@>
    """


@pytest.fixture
def two_default_stores(tmp_path):
    """stores.default and stores.filepath_default name different file stores."""
    integrated, raw = tmp_path / "integrated", tmp_path / "raw"
    integrated.mkdir()
    (raw / "session").mkdir(parents=True)
    (raw / "session" / "only-in-raw.txt").write_text("raw")
    (integrated / "only-in-integrated.txt").write_text("integrated")
    saved = dict(dj.config.stores)
    dj.config.stores.clear()
    dj.config.stores.update(
        {
            "integrated": {"protocol": "file", "location": str(integrated)},
            "raw": {"protocol": "file", "location": str(raw)},
            "default": "integrated",
            "filepath_default": "raw",
        }
    )
    yield raw
    dj.config.stores.clear()
    dj.config.stores.update(saved)


@pytest.fixture
def schema_filepath_default(connection_by_backend, db_creds_by_backend, two_default_stores):
    name = f"djtest_fp_default_{db_creds_by_backend['backend']}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(name, connection=connection_by_backend)
    schema(FilepathDefault)
    yield schema
    schema.drop(prompt=False)


def test_filepath_insert_uses_filepath_default_store(schema_filepath_default):
    FilepathDefault.insert1({"ref_id": 1, "file_ref": "session/only-in-raw.txt"})
    ref = (FilepathDefault & {"ref_id": 1}).fetch1("file_ref")
    assert ref.read() == b"raw"
    assert ref.store == "raw"


def test_filepath_insert_does_not_check_default_store(schema_filepath_default):
    with pytest.raises(FileNotFoundError, match="store 'raw'"):
        FilepathDefault.insert1({"ref_id": 2, "file_ref": "only-in-integrated.txt"})
    assert len(FilepathDefault()) == 0


NAMES = ["a.txt", "b.txt", "[ab].txt", "star*.txt", "what?.txt", "sub/c.txt"]


@pytest.fixture
def s3_folder(s3_client, stores_config):
    """A folder object in the S3 store, written directly to the bucket."""
    spec = dict(stores_config["repo-s3"])
    backend = StorageBackend(spec)
    folder = f"folder_download_{str(int(time.time() * 1000))[-8:]}"
    for name in NAMES:
        backend.fs.pipe(backend._full_path(f"{folder}/{name}"), name.encode())
    yield backend, folder
    backend.fs.rm(backend._full_path(folder), recursive=True)


@pytest.mark.parametrize("fsspec_version", [None, "2023.1.0"], ids=["installed", "old"])
def test_download_folder_from_s3(s3_folder, tmp_path, monkeypatch, fsspec_version):
    if fsspec_version:
        monkeypatch.setattr(fsspec, "__version__", fsspec_version)
    backend, folder = s3_folder
    ref = ObjectRef(path=folder, size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=backend)
    dest = tmp_path / "dest"
    assert ref.download(dest) == dest
    files = sorted(p.relative_to(dest / folder).as_posix() for p in (dest / folder).rglob("*") if p.is_file())
    assert files == sorted(NAMES)
    for name in NAMES:
        assert (dest / folder / name).read_bytes() == name.encode()
