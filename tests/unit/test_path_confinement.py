"""Stored values cannot direct file access outside the store or the download path.

Rows written by one user are fetched by others, so a path or file name read back
from the database must not reach files the store or ``download_path`` does not
cover: ``<attach>`` names must be plain file names, and store paths used by
``<filepath@>``, ``<object@>``, and hash-addressed storage must stay inside the
store location. ``allow_filepath_outside_store`` lifts the store check for
``<filepath@>`` only.

No test reads a device or opens a pipe: a regression must fail, not hang.
"""

import importlib
import os
import sys

import pytest

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX paths and symlinks")


@pytest.fixture(autouse=True)
def _live_modules():
    """
    Bind the names below from the currently imported ``datajoint`` package.

    Some unit tests purge ``datajoint`` from ``sys.modules`` and import it again;
    names bound at collection time would then refer to stale classes, and
    ``pytest.raises(DataJointError)`` would miss the error actually raised.
    """
    global AttachCodec, FilepathCodec, ObjectCodec, ObjectRef, DataJointError, Config, StorageBackend
    AttachCodec = importlib.import_module("datajoint.builtin_codecs.attach").AttachCodec
    FilepathCodec = importlib.import_module("datajoint.builtin_codecs.filepath").FilepathCodec
    ObjectCodec = importlib.import_module("datajoint.builtin_codecs.object").ObjectCodec
    ObjectRef = importlib.import_module("datajoint.objectref").ObjectRef
    DataJointError = importlib.import_module("datajoint.errors").DataJointError
    Config = importlib.import_module("datajoint.settings").Config
    StorageBackend = importlib.import_module("datajoint.storage").StorageBackend


@pytest.fixture
def layout(tmp_path):
    """A file store, a sibling directory whose name extends the store's, a file outside both, and a download path."""
    store = tmp_path / "store"
    store.mkdir()
    (store / "data").mkdir()
    (store / "data" / "inside.txt").write_text("inside")
    sibling_prefix = tmp_path / "store-sibling"
    sibling_prefix.mkdir()
    (sibling_prefix / "sibling.txt").write_text("sibling")
    (tmp_path / "outside.txt").write_text("outside")
    downloads = tmp_path / "downloads"
    return tmp_path, store, downloads


@pytest.fixture
def cfg(layout):
    _, store, downloads = layout
    config = Config()
    config.stores["local"] = {"protocol": "file", "location": str(store)}
    config.stores["default"] = "local"
    config.download_path = str(downloads)
    return config


PATHS_WITH_PARENT_REF = [
    "../outside.txt",
    "data/../../outside.txt",
    "../store-sibling/sibling.txt",
    "..\\outside.txt",
    "data/..\\..\\outside.txt",
    "data/sub/../inside.txt",
    "data/..",
]


def _outside_paths(root, store):
    """Absolute paths outside the store: a plain outside file and a sibling directory sharing the store's name prefix."""
    return [str(root / "outside.txt"), str(root / "store-sibling" / "sibling.txt"), str(store) + "-sibling/sibling.txt"]


# --------------------------------------------------------------------------- #
# StorageBackend._full_path
# --------------------------------------------------------------------------- #
class TestStorePathConfinement:
    @pytest.mark.parametrize("path", PATHS_WITH_PARENT_REF + ["data/x\x00y"])
    def test_file_store_rejects_parent_ref_and_nul(self, layout, path):
        _, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        with pytest.raises(DataJointError, match="outside the store location"):
            backend._full_path(path)

    @posix_only
    def test_file_store_rejects_absolute_paths_outside_location(self, layout):
        root, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        for path in _outside_paths(root, store) + [str(store / "data" / ".." / ".." / "outside.txt")]:
            with pytest.raises(DataJointError, match="allow_filepath_outside_store"):
                backend._full_path(path)

    @posix_only
    def test_file_store_accepts_absolute_paths_inside_location(self, layout):
        _, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        inside = str(store / "data" / "inside.txt")
        assert backend._full_path(inside) == inside
        assert backend._full_path(str(store) + "/./data//inside.txt")
        assert backend.get_buffer(inside) == b"inside"
        assert backend._path_in_location(inside) == "data/inside.txt"
        assert backend._path_in_location(str(store) + "-sibling/sibling.txt") is None

    @pytest.mark.parametrize("path", ["data/inside.txt", "./data/inside.txt", "data//inside.txt", ""])
    def test_file_store_accepts_relative_paths_inside(self, layout, path):
        _, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        assert backend._full_path(path) == (store / path).as_posix()

    @posix_only
    @pytest.mark.parametrize("location", [".", ""])
    def test_file_store_in_working_directory_rejects_protocol_and_home_paths(self, layout, monkeypatch, location):
        from fsspec.implementations.local import LocalFileSystem

        root, store, _ = layout
        monkeypatch.chdir(store)
        monkeypatch.setenv("HOME", str(root))
        backend = StorageBackend({"protocol": "file", "location": location})
        outside = str(root / "outside.txt")
        cwd = os.path.realpath(os.getcwd())
        for path in ("file://" + outside, "file:" + outside, "local://" + outside, "local:" + outside, "~/outside.txt"):
            try:
                full = backend._full_path(path)
            except DataJointError as error:
                assert "outside the store location" in str(error)
                continue
            # fsspec before 2024 does not read some of these prefixes as a protocol. A path it
            # accepts then names a file inside the working directory, and I/O stays there
            assert os.path.realpath(LocalFileSystem._strip_protocol(full)).startswith(cwd + "/")
            backend.put_buffer(b"written", path)
            assert backend.get_buffer(path) == b"written"
        assert (root / "outside.txt").read_text() == "outside"
        assert backend.get_buffer("data/inside.txt") == b"inside"

    def test_io_never_reaches_outside(self, layout):
        root, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        with pytest.raises(DataJointError):
            backend.get_buffer(str(root / "outside.txt"))
        with pytest.raises(DataJointError):
            backend.get_buffer("../outside.txt")
        with pytest.raises(DataJointError):
            backend.put_buffer(b"x", "../written.txt")
        with pytest.raises(DataJointError):
            backend.remove("../outside.txt")
        assert not (root / "written.txt").exists()
        assert (root / "outside.txt").exists()

    @pytest.mark.parametrize("protocol", ["s3", "gcs", "azure"])
    def test_object_store_rejects_parent_components(self, protocol):
        spec = {"protocol": protocol, "endpoint": "localhost:9000", "bucket": "b", "container": "b", "location": "loc"}
        backend = StorageBackend(spec)
        for path in ("../x", "a/../../x", "../../other-bucket/x"):
            with pytest.raises(DataJointError, match="outside the store location"):
                backend._full_path(path)
        # Paths inside the location are unchanged, including a leading slash
        assert backend._full_path("a/b") == "b/loc/a/b"
        assert backend._full_path("/a/b") == "b/loc//a/b"

    def test_confinement_can_be_lifted_per_backend(self, layout):
        root, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        backend.confine_paths = False
        assert backend._full_path(str(root / "outside.txt")) == (root / "outside.txt").as_posix()
        assert backend.get_buffer("../outside.txt") == b"outside"
        # Other backends stay confined
        assert StorageBackend.confine_paths is True


# --------------------------------------------------------------------------- #
# <filepath@>
# --------------------------------------------------------------------------- #
class TestFilepathConfinement:
    @pytest.mark.parametrize("path", PATHS_WITH_PARENT_REF[:3])
    def test_encode_rejects_parent_ref(self, cfg, path):
        with pytest.raises(DataJointError, match="outside the store location"):
            FilepathCodec().encode(path, store_name="local", context={"config": cfg})

    @posix_only
    def test_encode_rejects_absolute_paths_outside_store(self, cfg, layout):
        root, store, _ = layout
        for path in _outside_paths(root, store):
            with pytest.raises(DataJointError, match="outside the store location"):
                FilepathCodec().encode(path, store_name="local", context={"config": cfg})

    @posix_only
    def test_encode_records_absolute_path_inside_store_as_relative(self, cfg, layout):
        _, store, _ = layout
        codec = FilepathCodec()
        meta = codec.encode(str(store / "data" / "inside.txt"), store_name="local", context={"config": cfg})
        assert meta["path"] == "data/inside.txt"
        assert codec.decode(meta, context={"config": cfg}).read() == b"inside"

    @posix_only
    def test_decode_of_existing_absolute_path_inside_store(self, cfg, layout):
        _, store, _ = layout
        stored = {"path": str(store / "data" / "inside.txt"), "store": "local", "size": 6}
        ref = FilepathCodec().decode(stored, context={"config": cfg})
        assert ref.read() == b"inside"
        assert ref.full_path == stored["path"]

    @posix_only
    def test_encode_absolute_path_into_reserved_section(self, cfg, layout):
        _, store, _ = layout
        (store / "_hash").mkdir()
        (store / "_hash" / "x.bin").write_bytes(b"x")
        with pytest.raises(ValueError, match="reserved section"):
            FilepathCodec().encode(str(store / "_hash" / "x.bin"), store_name="local", context={"config": cfg})

    @pytest.mark.parametrize(
        "path", ["./_hash/x.bin", "././_schema/x", ".//_hash/x", "_hash", "_HASH/x.bin", "_Schema/x", "./_Hash"]
    )
    def test_encode_reserved_sections_after_normalizing(self, cfg, path):
        with pytest.raises(ValueError, match="reserved section"):
            FilepathCodec().encode(path, store_name="local", context={"config": cfg})

    def test_encode_filepath_prefix_after_normalizing(self, cfg, layout):
        _, store, _ = layout
        cfg.stores["local"]["filepath_prefix"] = "data"
        (store / "other.txt").write_text("other")
        with pytest.raises(DataJointError, match="outside the store location"):
            FilepathCodec().encode("data/../other.txt", store_name="local", context={"config": cfg})
        with pytest.raises(ValueError, match="filepath_prefix"):
            FilepathCodec().encode("other.txt", store_name="local", context={"config": cfg})
        for path in ("data/inside.txt", "./data/inside.txt"):
            meta = FilepathCodec().encode(path, store_name="local", context={"config": cfg})
            assert meta["path"] == path

    def test_round_trip_inside_store(self, cfg):
        codec = FilepathCodec()
        meta = codec.encode("data/inside.txt", store_name="local", context={"config": cfg})
        assert codec.decode(meta, context={"config": cfg}).read() == b"inside"

    @pytest.mark.parametrize("kind", ["absolute", "parent_ref", "sibling_prefix"])
    def test_decode_of_row_outside_store_does_not_reach_it(self, cfg, layout, kind):
        root, store, _ = layout
        path = {
            "absolute": str(root / "outside.txt"),
            "parent_ref": "../outside.txt",
            "sibling_prefix": str(store) + "-sibling/sibling.txt",
        }[kind]
        ref = FilepathCodec().decode({"path": path, "store": "local", "size": None}, context={"config": cfg})
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.read()
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.full_path
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.download(root / "dl")

    def test_encode_without_store_uses_filepath_default(self, cfg, layout, tmp_path):
        """With no store named, every check runs against stores.filepath_default, the store the row records."""
        _, store, _ = layout
        raw = tmp_path / "raw"
        (raw / "session").mkdir(parents=True)
        (raw / "session" / "only-in-raw.txt").write_text("raw")
        (store / "only-in-default.txt").write_text("default")
        cfg.stores["raw"] = {"protocol": "file", "location": str(raw)}
        cfg.stores["filepath_default"] = "raw"
        codec = FilepathCodec()
        for store_name in (None, ""):
            meta = codec.encode("session/only-in-raw.txt", store_name=store_name, context={"config": cfg})
            assert meta["store"] == "raw"
            assert meta["path"] == "session/only-in-raw.txt"
            assert codec.decode(meta, context={"config": cfg}).read() == b"raw"
        with pytest.raises(FileNotFoundError, match="store 'raw'"):
            codec.encode("only-in-default.txt", context={"config": cfg})

    @posix_only
    def test_encode_without_store_confines_to_filepath_default(self, cfg, layout, tmp_path):
        """An absolute path is checked against, and recorded relative to, the filepath_default store."""
        _, store, _ = layout
        raw = tmp_path / "raw"
        raw.mkdir()
        (raw / "a.txt").write_text("a")
        cfg.stores["raw"] = {"protocol": "file", "location": str(raw)}
        cfg.stores["filepath_default"] = "raw"
        meta = FilepathCodec().encode(str(raw / "a.txt"), context={"config": cfg})
        assert (meta["store"], meta["path"]) == ("raw", "a.txt")
        with pytest.raises(DataJointError, match="outside the store location"):
            FilepathCodec().encode(str(store / "data" / "inside.txt"), context={"config": cfg})

    @posix_only
    def test_store_location_through_symbolic_link(self, layout, tmp_path):
        """A location that is a symbolic link works; paths are compared as written, without resolving links."""
        root, store, _ = layout
        link = tmp_path / "store-link"
        link.symlink_to(store, target_is_directory=True)
        config = Config()
        config.stores["linked"] = {"protocol": "file", "location": str(link)}
        codec = FilepathCodec()
        meta = codec.encode("data/inside.txt", store_name="linked", context={"config": config})
        assert codec.decode(meta, context={"config": config}).read() == b"inside"
        meta = codec.encode(str(link / "data" / "inside.txt"), store_name="linked", context={"config": config})
        assert meta["path"] == "data/inside.txt"
        # The same file named through the link's target is not under the location as written
        with pytest.raises(DataJointError, match="outside the store location"):
            codec.encode(str(store / "data" / "inside.txt"), store_name="linked", context={"config": config})
        for path in _outside_paths(root, store):
            with pytest.raises(DataJointError, match="outside the store location"):
                codec.encode(path, store_name="linked", context={"config": config})

    @posix_only
    def test_relative_store_location_resolves_at_call_time(self, layout, monkeypatch):
        """A relative location is resolved against the working directory when the path is checked."""
        root, store, _ = layout
        config = Config()
        config.stores["rel"] = {"protocol": "file", "location": "store"}
        codec = FilepathCodec()
        monkeypatch.chdir(root)
        meta = codec.encode(str(store / "data" / "inside.txt"), store_name="rel", context={"config": config})
        assert meta["path"] == "data/inside.txt"
        assert codec.decode(meta, context={"config": config}).read() == b"inside"
        with pytest.raises(DataJointError, match="outside the store location"):
            codec.encode(str(root / "outside.txt"), store_name="rel", context={"config": config})
        # From another working directory the same absolute path is outside the location
        (root / "elsewhere" / "store").mkdir(parents=True)
        monkeypatch.chdir(root / "elsewhere")
        with pytest.raises(DataJointError, match="outside the store location"):
            codec.encode(str(store / "data" / "inside.txt"), store_name="rel", context={"config": config})

    def test_escape_hatch_allows_outside_references(self, cfg, layout):
        root, _, _ = layout
        cfg.allow_filepath_outside_store = True
        codec = FilepathCodec()
        meta = codec.encode("../outside.txt", store_name="local", context={"config": cfg})
        assert codec.decode(meta, context={"config": cfg}).read() == b"outside"
        absolute = {"path": str(root / "outside.txt"), "store": "local", "size": None}
        assert codec.decode(absolute, context={"config": cfg}).read() == b"outside"

    def test_escape_hatch_does_not_apply_to_object(self, cfg, layout):
        root, _, _ = layout
        cfg.allow_filepath_outside_store = True
        ref = ObjectCodec().decode({"path": str(root / "outside.txt"), "store": "local", "size": 7}, context={"config": cfg})
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.read()


# --------------------------------------------------------------------------- #
# <object@> and ObjectRef
# --------------------------------------------------------------------------- #
class TestObjectConfinement:
    @pytest.mark.parametrize("path", ["../outside.txt", "/outside.txt", "_schema/../../outside.txt"])
    def test_encode_rejects_precomputed_metadata_outside_store(self, cfg, path):
        with pytest.raises(DataJointError, match="outside the store location"):
            ObjectCodec().encode({"path": path, "store": "local", "size": 1}, context={"config": cfg})

    def test_encode_passes_precomputed_metadata_inside_store(self, cfg):
        meta = {"path": "_schema/s/t/k/f_tok", "store": "local", "size": 1}
        assert ObjectCodec().encode(meta, context={"config": cfg}) is meta

    def test_decode_of_row_outside_store_does_not_reach_it(self, cfg, layout):
        root, _, _ = layout
        ref = ObjectCodec().decode({"path": str(root / "outside.txt"), "store": "local", "size": 7}, context={"config": cfg})
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.read()
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.full_path

    @pytest.mark.parametrize(
        "subpath",
        ["sub/C:\\x\\y.dll", "sub/\\\\server\\share\\y.dll", "sub\\..", "sub/x:y"],
        ids=["drive", "unc", "parent_ref", "colon"],
    )
    def test_download_into_directory_uses_last_component_only(self, cfg, layout, subpath):
        """The local file name is the last component, split on both separators, and must be plain."""
        _, store, downloads = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        backend.confine_paths = False  # isolate the local-name check
        downloads.mkdir()
        ref = ObjectRef(path="data", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=backend)
        name = subpath.replace("\\", "/").split("/")[-1]
        if name in ("", ".", "..") or (os.name == "nt" and ":" in name):
            with pytest.raises(DataJointError, match="not a plain file name"):
                ref.download(downloads, subpath=subpath)
        else:
            # Missing source file: the error comes from the store, after the name was accepted.
            # Windows reports a source path containing ":" as invalid rather than missing
            with pytest.raises(OSError):
                ref.download(downloads, subpath=subpath)
        assert not any(downloads.iterdir())

    @pytest.fixture
    def memory_backend(self):
        """A backend over fsspec's in-memory filesystem, whose keys may contain '..' as object stores' keys can."""
        from fsspec.implementations.memory import MemoryFileSystem

        class MemoryBackend:
            fs = MemoryFileSystem()

            def _full_path(self, path):
                return f"dj-test-bucket/loc/{path}"

        backend = MemoryBackend()
        yield backend
        backend.fs.rm("/dj-test-bucket", recursive=True)

    def test_download_folder_rejects_key_leading_outside(self, memory_backend, tmp_path):
        """Every key is checked before any file is written."""
        memory_backend.fs.pipe(
            {
                "/dj-test-bucket/loc/obj/a.txt": b"a",
                "/dj-test-bucket/loc/obj/sub/b.txt": b"b",
                "/dj-test-bucket/loc/obj/sub/../../../../x": b"x",
            }
        )
        dest = tmp_path / "work" / "dest"  # dest/obj/sub/../../../../x is tmp_path/x
        ref = ObjectRef(path="obj", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)
        with pytest.raises(DataJointError, match="leads outside the destination"):
            ref.download(dest)
        assert not (tmp_path / "x").exists()
        assert [p for p in tmp_path.rglob("*") if not p.is_dir()] == []
        assert not (dest / "obj").exists()

    def test_download_folder_round_trip(self, memory_backend, tmp_path):
        """Files land in destination/<object name>/..., as fsspec's recursive get placed them."""
        memory_backend.fs.pipe({"/dj-test-bucket/loc/obj/a.txt": b"a", "/dj-test-bucket/loc/obj/sub/b.txt": b"b"})
        dest = tmp_path / "dest"
        ref = ObjectRef(path="obj", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)
        assert ref.download(dest) == dest
        assert sorted(p.relative_to(dest).as_posix() for p in dest.rglob("*") if p.is_file()) == ["obj/a.txt", "obj/sub/b.txt"]
        assert (dest / "obj" / "a.txt").read_bytes() == b"a"
        assert (dest / "obj" / "sub" / "b.txt").read_bytes() == b"b"

    NAMES_WITH_GLOB_CHARACTERS = ["[ab].txt", "a.txt", "b.txt", "star*.txt", "what?.txt", "starX.txt"]

    def _glob_folder(self, memory_backend):
        memory_backend.fs.pipe({f"/dj-test-bucket/loc/obj/{n}": n.encode() for n in self.NAMES_WITH_GLOB_CHARACTERS})
        return ObjectRef(path="obj", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)

    def _check_glob_folder(self, dest):
        assert sorted(p.name for p in (dest / "obj").iterdir()) == sorted(self.NAMES_WITH_GLOB_CHARACTERS)
        for name in self.NAMES_WITH_GLOB_CHARACTERS:
            assert (dest / "obj" / name).read_bytes() == name.encode()

    def test_download_folder_keys_with_glob_characters(self, memory_backend, tmp_path):
        """Keys containing '[', '*' or '?' each land under their own name."""
        ref = self._glob_folder(memory_backend)
        ref.download(tmp_path / "dest")
        self._check_glob_folder(tmp_path / "dest")

    @pytest.mark.parametrize("version", ["2023.1.0", "2023.6.0", "not-a-version"])
    def test_download_folder_on_old_fsspec_fetches_files_one_by_one(self, memory_backend, tmp_path, monkeypatch, version):
        """Before fsspec 2023.9, get(list, list) expands the sources as globs, so it is not used."""
        import fsspec

        monkeypatch.setattr(fsspec, "__version__", version)

        def expanding_get(*args, **kwargs):
            raise AssertionError("fs.get(list, list) expands sources on this fsspec version")

        monkeypatch.setattr(memory_backend.fs, "get", expanding_get)
        ref = self._glob_folder(memory_backend)
        ref.download(tmp_path / "dest")
        self._check_glob_folder(tmp_path / "dest")

    def test_download_folder_directory_marker_key(self, memory_backend, tmp_path, monkeypatch):
        """A key ending in '/' (an object store's directory marker) becomes a folder, not a file."""
        memory_backend.fs.pipe({"/dj-test-bucket/loc/obj/sub/b.txt": b"b"})
        find = memory_backend.fs.find

        def find_with_marker(path, **kwargs):
            entries = find(path, **kwargs)
            entries["/dj-test-bucket/loc/obj/empty/"] = {"name": "/dj-test-bucket/loc/obj/empty/", "type": "file", "size": 0}
            return entries

        monkeypatch.setattr(memory_backend.fs, "find", find_with_marker)
        ref = ObjectRef(path="obj", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)
        dest = tmp_path / "dest"
        ref.download(dest)
        assert (dest / "obj" / "empty").is_dir()
        assert (dest / "obj" / "sub" / "b.txt").read_bytes() == b"b"

    def test_download_empty_folder_when_find_omits_it(self, memory_backend, tmp_path, monkeypatch):
        """Before fsspec 2024.3.1, find(withdirs=True) does not list the folder itself."""
        memory_backend.fs.mkdir("/dj-test-bucket/loc/empty")
        monkeypatch.setattr(memory_backend.fs, "find", lambda path, **kwargs: {})
        ref = ObjectRef(path="empty", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)
        dest = tmp_path / "dest"
        assert ref.download(dest) == dest
        assert (dest / "empty").is_dir()
        assert list((dest / "empty").iterdir()) == []

    def test_download_missing_folder_raises(self, memory_backend, tmp_path):
        memory_backend.fs.pipe({"/dj-test-bucket/loc/other.txt": b"x"})
        ref = ObjectRef(path="missing", size=None, hash=None, ext=None, is_dir=True, timestamp=None, _backend=memory_backend)
        with pytest.raises(FileNotFoundError):
            ref.download(tmp_path / "dest")
        assert not (tmp_path / "dest" / "missing").exists()


# --------------------------------------------------------------------------- #
# <attach>
# --------------------------------------------------------------------------- #
def _attach_blob(name: str, contents: bytes) -> bytes:
    return name.encode("utf-8") + b"\x00" + contents


class TestAttachConfinement:
    @pytest.mark.parametrize(
        "name",
        ["../outside.txt", "../../outside.txt", "sub/outside.txt", "..\\outside.txt", "back\\slash.txt", "..", ".", ""],
        ids=["parent_ref", "parent_ref_twice", "subdir", "backslash_parent_ref", "backslash", "dotdot", "dot", "empty"],
    )
    def test_decode_rejects_stored_name_that_is_not_plain(self, cfg, layout, name):
        root, _, downloads = layout
        (root / "outside.txt").unlink()
        with pytest.raises(DataJointError, match="not a plain file name"):
            AttachCodec().decode(_attach_blob(name, b"payload"), context={"config": cfg})
        assert not (root / "outside.txt").exists()
        assert not (downloads / "sub").exists()

    def test_decode_rejects_absolute_stored_name(self, cfg, layout):
        root, _, _ = layout
        target = root / "absolute.txt"
        with pytest.raises(DataJointError, match="not a plain file name"):
            AttachCodec().decode(_attach_blob(str(target), b"payload"), context={"config": cfg})
        assert not target.exists()

    def test_round_trip(self, cfg, layout, tmp_path):
        _, _, downloads = layout
        source = tmp_path / "notes.txt"
        source.write_bytes(b"hello")
        codec = AttachCodec()
        blob = codec.encode(source)
        assert blob == _attach_blob("notes.txt", b"hello")
        first = codec.decode(blob, context={"config": cfg})
        assert first == str(downloads / "notes.txt")
        # Identical content reuses the file; different content gets a new name
        assert codec.decode(blob, context={"config": cfg}) == first
        second = codec.decode(_attach_blob("notes.txt", b"changed"), context={"config": cfg})
        assert second == str(downloads / "notes_1.txt")
        assert (downloads / "notes.txt").read_bytes() == b"hello"
        assert (downloads / "notes_1.txt").read_bytes() == b"changed"

    def test_decode_skips_directory_with_the_same_name(self, cfg, layout):
        """A non-regular file in the way is never opened; the next free name is used."""
        _, _, downloads = layout
        (downloads / "notes.txt").mkdir(parents=True)
        result = AttachCodec().decode(_attach_blob("notes.txt", b"payload"), context={"config": cfg})
        assert result == str(downloads / "notes_1.txt")

    @posix_only
    def test_decode_does_not_reuse_or_write_through_link(self, cfg, layout):
        root, _, downloads = layout
        downloads.mkdir()
        (downloads / "linked.txt").symlink_to(root / "outside.txt")
        result = AttachCodec().decode(_attach_blob("linked.txt", b"changed"), context={"config": cfg})
        assert result == str(downloads / "linked_1.txt")
        assert (root / "outside.txt").read_text() == "outside"
        # A link is not reused even when its target holds the same content
        result = AttachCodec().decode(_attach_blob("linked.txt", b"outside"), context={"config": cfg})
        assert result == str(downloads / "linked_2.txt")

    @posix_only
    def test_decode_does_not_follow_dangling_link(self, cfg, layout):
        root, _, downloads = layout
        downloads.mkdir()
        (downloads / "new.txt").symlink_to(root / "created-through-link.txt")
        result = AttachCodec().decode(_attach_blob("new.txt", b"payload"), context={"config": cfg})
        assert result == str(downloads / "new_1.txt")
        assert not (root / "created-through-link.txt").exists()

    @pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="needs named pipes")
    def test_reuse_check_does_not_block_on_pipe_swapped_in(self, layout, monkeypatch):
        """A named pipe that replaces the file after the lstat check is opened without blocking, and not reused."""
        import threading

        attach = importlib.import_module("datajoint.builtin_codecs.attach")
        _, _, downloads = layout
        downloads.mkdir()
        fifo = downloads / "notes.txt"
        os.mkfifo(fifo)
        contents = b"payload"
        real_lstat = os.lstat
        regular = os.stat_result((0o100644, 0, 0, 1, 0, 0, len(contents), 0, 0, 0))

        def lstat(path, *args, **kwargs):
            # The check sees the regular file that was there before the swap
            return regular if os.fspath(path) == str(fifo) else real_lstat(path, *args, **kwargs)

        monkeypatch.setattr(os, "lstat", lstat)
        result = []
        thread = threading.Thread(target=lambda: result.append(attach._has_contents(fifo, contents)), daemon=True)
        thread.start()
        thread.join(timeout=5)
        if thread.is_alive():
            # Release the blocked open so the test fails instead of hanging
            os.close(os.open(fifo, os.O_WRONLY | os.O_NONBLOCK))
            thread.join(timeout=5)
            pytest.fail("_has_contents blocked opening a named pipe")
        assert result == [False]
