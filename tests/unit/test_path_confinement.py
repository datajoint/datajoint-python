"""Stored values cannot direct file access outside the store or the download path.

Rows written by one user are fetched by others, so a path or file name read back
from the database must not reach files the store or ``download_path`` does not
cover: ``<attach>`` names must be plain file names, and store paths used by
``<filepath@>``, ``<object@>``, and hash-addressed storage must stay inside the
store location. ``allow_filepath_outside_store`` lifts the store check for
``<filepath@>`` only.
"""

import importlib
import os
import sys

import pytest

posix_only = pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX symlinks, devices, or pipes")


@pytest.fixture(autouse=True)
def _live_modules():
    """
    Bind the names below from the currently imported ``datajoint`` package.

    Some unit tests purge ``datajoint`` from ``sys.modules`` and import it again;
    names bound at collection time would then refer to stale classes, and
    ``pytest.raises(DataJointError)`` would miss the error actually raised.
    """
    global AttachCodec, FilepathCodec, ObjectCodec, DataJointError, Config, StorageBackend
    AttachCodec = importlib.import_module("datajoint.builtin_codecs.attach").AttachCodec
    FilepathCodec = importlib.import_module("datajoint.builtin_codecs.filepath").FilepathCodec
    ObjectCodec = importlib.import_module("datajoint.builtin_codecs.object").ObjectCodec
    DataJointError = importlib.import_module("datajoint.errors").DataJointError
    Config = importlib.import_module("datajoint.settings").Config
    StorageBackend = importlib.import_module("datajoint.storage").StorageBackend


@pytest.fixture
def layout(tmp_path):
    """A file store, a sibling directory sharing its name as a prefix, and a download path."""
    store = tmp_path / "store"
    store.mkdir()
    (store / "data").mkdir()
    (store / "data" / "inside.txt").write_text("inside")
    sibling = tmp_path / "store-evil"
    sibling.mkdir()
    (sibling / "secret.txt").write_text("secret")
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


HOSTILE_RELATIVE = [
    "../outside.txt",
    "data/../../outside.txt",
    "../store-evil/secret.txt",
    "..\\outside.txt",
    "data/..\\..\\outside.txt",
    "data/x\x00y",
    "data/sub/../inside.txt",
    "data/..",
]


# --------------------------------------------------------------------------- #
# StorageBackend._full_path
# --------------------------------------------------------------------------- #
class TestStorePathConfinement:
    @pytest.mark.parametrize("path", HOSTILE_RELATIVE)
    def test_file_store_rejects_relative_escape(self, layout, path):
        _, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        with pytest.raises(DataJointError, match="outside the store location"):
            backend._full_path(path)

    def test_file_store_rejects_absolute_paths(self, layout):
        root, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        for path in (str(root / "outside.txt"), str(store / "data" / "inside.txt"), "/dev/zero"):
            with pytest.raises(DataJointError, match="outside the store location"):
                backend._full_path(path)

    @pytest.mark.parametrize("path", ["data/inside.txt", "./data/inside.txt", "data//inside.txt", ""])
    def test_file_store_accepts_paths_inside(self, layout, path):
        _, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        assert backend._full_path(path) == (store / path).as_posix()

    def test_io_never_reaches_outside(self, layout):
        root, store, _ = layout
        backend = StorageBackend({"protocol": "file", "location": str(store)})
        with pytest.raises(DataJointError):
            backend.get_buffer("/dev/zero")
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
    @pytest.mark.parametrize("path", HOSTILE_RELATIVE[:3])
    def test_encode_rejects_relative_escape(self, cfg, path):
        with pytest.raises(DataJointError, match="outside the store location"):
            FilepathCodec().encode(path, store_name="local", context={"config": cfg})

    def test_encode_rejects_absolute_paths(self, cfg, layout):
        root, store, _ = layout
        for path in (str(root / "outside.txt"), str(store / "data" / "inside.txt"), "/dev/zero"):
            with pytest.raises(DataJointError, match="outside the store location"):
                FilepathCodec().encode(path, store_name="local", context={"config": cfg})

    @pytest.mark.parametrize("path", ["./_hash/x.bin", "././_schema/x", ".//_hash/x", "_hash"])
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

    @pytest.mark.parametrize("kind", ["absolute", "relative", "sibling", "device"])
    def test_decode_of_hostile_row_does_not_read_outside(self, cfg, layout, kind):
        root, _, _ = layout
        path = {
            "absolute": str(root / "outside.txt"),
            "relative": "../outside.txt",
            "sibling": "../store-evil/secret.txt",
            "device": "/dev/zero",
        }[kind]
        ref = FilepathCodec().decode({"path": path, "store": "local", "size": None}, context={"config": cfg})
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.read()
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.download(root / "dl")

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
# <object@>
# --------------------------------------------------------------------------- #
class TestObjectConfinement:
    @pytest.mark.parametrize("path", ["../outside.txt", "/dev/zero", "_schema/../../outside.txt"])
    def test_encode_rejects_precomputed_metadata_outside_store(self, cfg, path):
        with pytest.raises(DataJointError, match="outside the store location"):
            ObjectCodec().encode({"path": path, "store": "local", "size": 1}, context={"config": cfg})

    def test_encode_passes_precomputed_metadata_inside_store(self, cfg):
        meta = {"path": "_schema/s/t/k/f_tok", "store": "local", "size": 1}
        assert ObjectCodec().encode(meta, context={"config": cfg}) is meta

    def test_decode_of_hostile_row_does_not_read_outside(self, cfg, layout):
        root, _, _ = layout
        ref = ObjectCodec().decode({"path": str(root / "outside.txt"), "store": "local", "size": 7}, context={"config": cfg})
        with pytest.raises(DataJointError, match="outside the store location"):
            ref.read()


# --------------------------------------------------------------------------- #
# <attach>
# --------------------------------------------------------------------------- #
def _attach_blob(name: str, contents: bytes) -> bytes:
    return name.encode("utf-8") + b"\x00" + contents


class TestAttachConfinement:
    @pytest.mark.parametrize(
        "name",
        ["../evil.txt", "../../evil.txt", "sub/evil.txt", "..\\evil.txt", "..", ".", "", "/evil.txt"],
    )
    def test_decode_rejects_names_that_are_not_plain(self, cfg, layout, name):
        root, _, downloads = layout
        with pytest.raises(DataJointError, match="not a plain file name"):
            AttachCodec().decode(_attach_blob(name, b"payload"), context={"config": cfg})
        assert not (root / "evil.txt").exists()
        assert not (downloads / "sub").exists()

    def test_decode_rejects_absolute_name(self, cfg, layout):
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

    @posix_only
    def test_decode_does_not_read_device_behind_link(self, cfg, layout):
        _, _, downloads = layout
        downloads.mkdir()
        (downloads / "zero").symlink_to("/dev/zero")
        result = AttachCodec().decode(_attach_blob("zero", b"payload"), context={"config": cfg})
        assert result == str(downloads / "zero_1")
        assert os.readlink(downloads / "zero") == "/dev/zero"

    @posix_only
    def test_decode_does_not_open_pipe(self, cfg, layout):
        _, _, downloads = layout
        downloads.mkdir()
        os.mkfifo(downloads / "pipe")
        result = AttachCodec().decode(_attach_blob("pipe", b"payload"), context={"config": cfg})
        assert result == str(downloads / "pipe_1")

    @posix_only
    def test_decode_does_not_write_through_link(self, cfg, layout):
        root, _, downloads = layout
        downloads.mkdir()
        (downloads / "outside.txt").symlink_to(root / "outside.txt")
        result = AttachCodec().decode(_attach_blob("outside.txt", b"overwritten"), context={"config": cfg})
        assert result == str(downloads / "outside_1.txt")
        assert (root / "outside.txt").read_text() == "outside"
        # A link is not reused even when its target holds the same content
        result = AttachCodec().decode(_attach_blob("outside.txt", b"outside"), context={"config": cfg})
        assert result == str(downloads / "outside_2.txt")

    @posix_only
    def test_decode_does_not_follow_dangling_link(self, cfg, layout):
        root, _, downloads = layout
        downloads.mkdir()
        (downloads / "new.txt").symlink_to(root / "created-through-link.txt")
        result = AttachCodec().decode(_attach_blob("new.txt", b"payload"), context={"config": cfg})
        assert result == str(downloads / "new_1.txt")
        assert not (root / "created-through-link.txt").exists()
