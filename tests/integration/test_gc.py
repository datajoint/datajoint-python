"""
Tests for garbage collection (gc.py).
"""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import datajoint as dj
from datajoint import gc
from datajoint.builtin_codecs.object import ObjectCodec
from datajoint.codecs import get_codec
from datajoint.errors import DataJointError


class GcCustomObjectCodec(ObjectCodec):
    """A custom schema-addressed codec — a ``SchemaCodec`` subclass with a
    non-built-in name, mirroring the user's NetCDF codec in #1469. Registered
    at import via ``Codec.__init_subclass__``. GC must recognize it by type
    (not by hardcoded name) and treat its files as referenced, not orphaned.
    """

    name = "gc_custom_object"


# Tables used by TestScanWithLiveData. Defined at module scope so dj.Schema's
# context resolution can find them by class name; bound to a schema inside
# each fixture (see schema(...) calls below).


class GcBlobTest(dj.Manual):
    definition = """
    rid : int
    ---
    payload : <blob@local>
    """


class GcNpyTest(dj.Manual):
    definition = """
    rid : int
    ---
    waveform : <npy@local>
    """


class GcObjectTest(dj.Manual):
    definition = """
    rid : int
    ---
    results : <object@local>
    """


class GcCustomCodecTest(dj.Manual):
    definition = """
    rid : int
    ---
    payload : <gc_custom_object@local>
    """


def _make_attr(**overrides):
    """Build a real heading.Attribute with defaults, overriding named fields."""
    from datajoint.heading import Attribute, default_attribute_properties

    return Attribute(**{**default_attribute_properties, **overrides})


class TestAttributeIsHashObject:
    """Attribute.is_hash_object classifies external hash-addressed storage
    (moved from gc._uses_hash_storage — the classification now lives on the
    heading so GC and other modules share one source of truth)."""

    def test_false_for_no_codec(self):
        assert _make_attr(codec=None).is_hash_object is False

    def test_true_for_hash_type(self):
        # <hash> is external-only; True regardless of store attribute value
        assert _make_attr(codec=get_codec("hash"), store="mystore").is_hash_object is True

    def test_true_for_blob_external(self):
        assert _make_attr(codec=get_codec("blob"), store="mystore").is_hash_object is True

    def test_true_for_attach_external(self):
        assert _make_attr(codec=get_codec("attach"), store="mystore").is_hash_object is True

    def test_false_for_blob_internal(self):
        # inline <blob> (no store) lives in the column — not hash-addressed
        assert _make_attr(codec=get_codec("blob"), store=None).is_hash_object is False

    def test_false_for_schema_codec(self):
        assert _make_attr(codec=get_codec("object"), store="mystore").is_hash_object is False


class TestHashReferencedPaths:
    """Reference extraction for hash-addressed metadata via the codec API
    (Codec.referenced_paths — the production path used by gc.scan)."""

    def test_returns_empty_for_none(self):
        assert get_codec("hash").referenced_paths(None) == []

    def test_parses_json_string(self):
        value = '{"path": "_hash/schema/abc123", "hash": "abc123", "store": "mystore"}'
        refs = get_codec("hash").referenced_paths(value)

        assert refs == [("_hash/schema/abc123", "mystore")]

    def test_parses_dict_directly(self):
        value = {"path": "_hash/schema/def456", "hash": "def456", "store": None}
        refs = get_codec("hash").referenced_paths(value)

        assert refs == [("_hash/schema/def456", None)]

    def test_returns_empty_for_invalid_json(self):
        assert get_codec("hash").referenced_paths("not json") == []

    def test_returns_empty_for_dict_without_path(self):
        assert get_codec("hash").referenced_paths({"hash": "abc123"}) == []


class TestAttributeIsSchemaObject:
    """Attribute.is_schema_object classifies schema-addressed storage by codec
    TYPE (so custom SchemaCodec subclasses are included, #1469)."""

    def test_false_for_no_codec(self):
        assert _make_attr(codec=None).is_schema_object is False

    def test_true_for_object_type(self):
        assert _make_attr(codec=get_codec("object")).is_schema_object is True

    def test_true_for_npy_type(self):
        assert _make_attr(codec=get_codec("npy")).is_schema_object is True

    def test_true_for_custom_schema_subclass(self):
        # by type, not name — a custom ObjectCodec subclass counts (#1469)
        assert _make_attr(codec=get_codec("gc_custom_object")).is_schema_object is True

    def test_false_for_hash_codec(self):
        assert _make_attr(codec=get_codec("blob"), store="mystore").is_schema_object is False


class TestSchemaReferencedPaths:
    """Reference extraction for schema-addressed metadata via the codec API
    (inherited by every SchemaCodec subclass, including custom codecs)."""

    def test_returns_empty_for_none(self):
        assert get_codec("object").referenced_paths(None) == []

    def test_parses_json_string(self):
        value = '{"path": "schema/table/pk/field", "store": "mystore"}'
        refs = get_codec("object").referenced_paths(value)

        assert refs == [("schema/table/pk/field", "mystore")]

    def test_parses_dict_directly(self):
        refs = get_codec("object").referenced_paths({"path": "test/path", "store": None})

        assert refs == [("test/path", None)]

    def test_custom_subclass_inherits_extraction(self):
        refs = get_codec("gc_custom_object").referenced_paths({"path": "a/b/c", "store": "local"})

        assert refs == [("a/b/c", "local")]

    def test_returns_empty_for_dict_without_path(self):
        assert get_codec("object").referenced_paths({"other": "data"}) == []


def _mock_collector(store="test_store"):
    """A GarbageCollector wired to mocks (no real store), for logic-only tests.
    The backend is resolved eagerly at construction, so patch it out."""
    cfg = MagicMock()
    cfg.get_store_spec.return_value = {"hash_prefix": "_hash", "schema_prefix": "_schema"}
    with patch("datajoint.gc.get_store_backend", return_value=MagicMock()):
        return gc.GarbageCollector(MagicMock(), store=store, config=cfg)


def _gc(schema, store="local"):
    """One store-bound collector per live-data test (config from the schema)."""
    return gc.GarbageCollector(schema, store=store)


class TestGarbageCollectorConstruction:
    """The collector is store-bound and resolves its store eagerly."""

    def test_resolves_store_at_construction(self):
        cfg = MagicMock()
        cfg.get_store_spec.return_value = {"hash_prefix": "_hash", "schema_prefix": "_schema"}
        with patch("datajoint.gc.get_store_backend", return_value="BACKEND") as mock_backend:
            collector = gc.GarbageCollector(MagicMock(), store="mystore", config=cfg)
        mock_backend.assert_called_once_with("mystore", config=cfg)  # eager
        assert collector.store == "mystore"
        assert collector.backend == "BACKEND"
        assert collector._hash_prefix == "_hash" and collector._schema_prefix == "_schema"


class TestCollect:
    """Tests for GarbageCollector.collect (dry_run=True is the read-only scan)."""

    # Fixtures patched onto the four data-gathering methods so the orphan set is
    # fixed: hashes path1,path2 referenced but path1,path3 stored → path3
    # orphaned; schema pk1 referenced but pk1,pk2 stored → pk2 orphaned.
    def _patch_data(self):
        return [
            patch.object(gc.GarbageCollector, "hash_references", return_value={"_hash/s/path1", "_hash/s/path2"}),
            patch.object(gc.GarbageCollector, "schema_references", return_value={"s/t/pk1/f"}),
            patch.object(gc.GarbageCollector, "list_hash_paths", return_value={"_hash/s/path1": 100, "_hash/s/path3": 200}),
            patch.object(gc.GarbageCollector, "list_schema_paths", return_value={"s/t/pk1/f": 500, "s/t/pk2/f": 300}),
        ]

    def test_requires_at_least_one_schema(self):
        """At least one schema is required, checked before the store is resolved."""
        with pytest.raises(DataJointError, match="At least one schema must be provided"):
            gc.GarbageCollector(store="test_store")

    def test_dry_run_reports_stats_and_deletes_nothing(self):
        """dry_run=True (default) returns the full orphan report without deleting."""
        with ExitStack() as es:
            for p in self._patch_data():
                es.enter_context(p)
            mock_delete = es.enter_context(patch("datajoint.gc.delete_path"))
            stats = _mock_collector().collect()  # dry_run defaults True

        assert stats["dry_run"] is True
        assert stats["deleted"] == 0 and stats["bytes_freed"] == 0
        mock_delete.assert_not_called()
        # full report is present
        assert stats["hash_paths_orphaned"] == 1 and stats["orphaned_hash_paths"] == ["_hash/s/path3"]
        assert stats["hash_paths_orphaned_bytes"] == 200
        assert stats["schema_paths_orphaned"] == 1 and stats["orphaned_schema_paths"] == ["s/t/pk2/f"]
        assert stats["schema_paths_orphaned_bytes"] == 300
        # no combined totals
        for k in ("referenced", "stored", "orphaned", "orphaned_bytes"):
            assert k not in stats

    def test_delete_removes_orphans_via_bound_store(self):
        """dry_run=False deletes each section's orphans, sized from the same scan."""
        with ExitStack() as es:
            for p in self._patch_data():
                es.enter_context(p)
            mock_delete_hash = es.enter_context(patch("datajoint.gc.delete_path", return_value=True))
            mock_delete_schema = es.enter_context(patch.object(gc.GarbageCollector, "delete_schema_path", return_value=True))
            collector = _mock_collector()
            stats = collector.collect(dry_run=False)

        assert stats["hash_paths_deleted"] == 1 and stats["schema_paths_deleted"] == 1
        assert stats["deleted"] == 2
        assert stats["bytes_freed"] == 500  # 200 (hash path3) + 300 (schema pk2)
        assert stats["dry_run"] is False
        # hash deleted via the collector's own store/config (not threaded params)
        mock_delete_hash.assert_called_once_with("_hash/s/path3", collector.store, config=collector.config)
        mock_delete_schema.assert_called_once_with("s/t/pk2/f")


class TestScanWithLiveData:
    """End-to-end tests for GarbageCollector.scan() against real schemas with external storage.

    Exercises the full production path:
        scan_*_references → table.proj(attr).cursor() → raw JSON metadata.

    These are the regression tests that would have caught issue #1442
    (silent type mismatch when scan helpers iterated decoded codec outputs
    instead of raw stored metadata).
    """

    @pytest.fixture
    def schema_blob(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_blob"
        schema = dj.Schema(
            schema_name,
            context={"GcBlobTest": GcBlobTest},
            connection=connection_test,
        )
        schema(GcBlobTest)
        yield schema
        schema.drop()

    @pytest.fixture
    def schema_npy(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_npy"
        schema = dj.Schema(
            schema_name,
            context={"GcNpyTest": GcNpyTest},
            connection=connection_test,
        )
        schema(GcNpyTest)
        yield schema
        schema.drop()

    @pytest.fixture
    def schema_object(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_object"
        schema = dj.Schema(
            schema_name,
            context={"GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcObjectTest)
        yield schema
        schema.drop()

    def test_scan_finds_active_blob_reference(self, schema_blob):
        """scan() must report hash_paths_referenced >= 1 for a populated <blob@> column.

        Decoded value type returned by BlobCodec.decode is numpy.ndarray, which
        does not satisfy the raw-metadata dict/JSON-string check in referenced_paths — this
        test fails before the cursor-based fix in scan_hash_references.
        """
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(64, dtype="uint8")})

        collector = _gc(schema_blob)
        stats = collector.collect()

        assert stats["hash_paths_referenced"] >= 1, f"scan should find the active <blob@> reference; got {stats}"

    def test_scan_finds_active_npy_reference(self, schema_npy):
        """scan() must report schema_paths_referenced >= 1 for a populated <npy@> column.

        Decoded value type returned by NpyCodec.decode is NpyRef (lazy handle),
        which does not satisfy the raw-metadata dict check in referenced_paths — this test
        fails before the cursor-based fix in scan_schema_references.
        """
        GcNpyTest.insert1({"rid": 1, "waveform": np.arange(64, dtype="float32")})

        collector = _gc(schema_npy)
        stats = collector.collect()

        assert stats["schema_paths_referenced"] >= 1, f"scan should find the active <npy@> reference; got {stats}"

    def test_scan_finds_active_object_reference(self, schema_object):
        """scan() must report schema_paths_referenced >= 1 for a populated <object@> column.

        Decoded value type returned by ObjectCodec.decode is ObjectRef (lazy
        handle), which does not satisfy the raw-metadata dict check in referenced_paths —
        this test fails before the cursor-based fix in scan_schema_references.
        """
        GcObjectTest.insert1({"rid": 1, "results": b"hello-gc-test"})

        collector = _gc(schema_object)
        stats = collector.collect()

        assert stats["schema_paths_referenced"] >= 1, f"scan should find the active <object@> reference; got {stats}"

    @pytest.fixture
    def schema_custom(self, connection_test, prefix, mock_stores):
        schema_name = f"{prefix}_test_gc_e2e_custom"
        schema = dj.Schema(
            schema_name,
            context={"GcCustomCodecTest": GcCustomCodecTest},
            connection=connection_test,
        )
        schema(GcCustomCodecTest)
        yield schema
        schema.drop()

    def test_custom_codec_reference_not_orphaned(self, schema_custom):
        """#1469: a live custom SchemaCodec value must be recognized as
        referenced and its file path must NOT be flagged as an orphan. Before
        the fix, schema-addressed classification keyed on the hardcoded names
        object/npy, so this codec was never scanned and its live file was
        reported orphaned. Now Attribute.is_schema_object dispatches on type.

        Asserts on the specific live path (not global counts) so it is robust to
        other tests sharing the same ``local`` store.
        """
        GcCustomCodecTest.insert1({"rid": 1, "payload": b"live-payload"})

        collector = _gc(schema_custom)
        refs = collector.schema_references()
        assert refs, "custom codec's live reference must be discovered (#1469)"
        live_path = next(iter(refs))

        stats = collector.collect()
        assert live_path not in stats["orphaned_schema_paths"], f"live custom-codec file wrongly flagged orphan: {live_path}"

    def test_custom_codec_survives_collect(self, schema_custom):
        """#1469 end-to-end data-loss guard: collect() must delete only the
        deleted row's file and keep the surviving row's file (checked by exact
        path, robust to a shared store)."""
        GcCustomCodecTest.insert1({"rid": 1, "payload": b"row-one"})
        GcCustomCodecTest.insert1({"rid": 2, "payload": b"row-two"})

        collector = _gc(schema_custom)
        refs_all = collector.schema_references()
        assert len(refs_all) == 2, f"both live rows should be referenced; got {refs_all}"

        # Delete one row — its file becomes a genuine orphan (delete-then-GC).
        (GcCustomCodecTest & {"rid": 1}).delete(prompt=False)

        refs_live = collector.schema_references()
        assert len(refs_live) == 1, f"one live row should remain referenced; got {refs_live}"
        live_path = next(iter(refs_live))
        deleted_paths = refs_all - refs_live

        collector.collect(dry_run=False)

        stored_after = set(collector.list_schema_paths(schema_custom.database))
        assert live_path in stored_after, f"collect() deleted the live custom-codec file {live_path} (#1469)"
        assert not (deleted_paths & stored_after), f"deleted row's orphan not reclaimed: {deleted_paths & stored_after}"


class TestDirectoryObjects:
    """Directory-valued objects (e.g. Zarr stores) — the referenced metadata
    path is a directory PREFIX whose stored form is many chunk files plus a
    `{path}.manifest.json` sidecar. Orphan matching must be coverage-based
    (exact / ancestor-prefix / manifest), not exact set difference: with exact
    matching, every chunk of a LIVE store is misclassified as an orphan and
    collect() deletes live data."""

    @pytest.fixture
    def schema_dirobj(self, connection_test, prefix, mock_stores):
        schema = dj.Schema(
            f"{prefix}_test_gc_dirobj",
            context={"GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcObjectTest)
        yield schema
        schema.drop()

    @staticmethod
    def _make_store_dir(tmp_path, name, n_chunks=3):
        d = tmp_path / name
        d.mkdir()
        (d / ".zarray").write_bytes(b"{}")
        for i in range(n_chunks):
            (d / f"0.{i}").write_bytes(f"chunk{i}".encode())
        return d

    def test_live_directory_object_not_orphaned(self, schema_dirobj, tmp_path):
        """A live folder object's chunk files and manifest must be covered by
        its referenced prefix — none may appear in orphaned_schema_paths."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "live.zarr"))})

        collector = _gc(schema_dirobj)
        refs = collector.schema_references()
        assert len(refs) == 1
        ref = next(iter(refs))

        stored = collector.list_schema_paths(schema_dirobj.database)
        chunk_files = [p for p in stored if p.startswith(ref + "/")]
        assert len(chunk_files) >= 4, f"expected the folder's files under {ref}; got {sorted(stored)}"
        assert f"{ref}.manifest.json" in stored, "manifest sidecar must be listed (it is reclaimable state)"

        stats = collector.collect()
        flagged = [
            p for p in stats["orphaned_schema_paths"] if p == ref or p.startswith(ref + "/") or p == f"{ref}.manifest.json"
        ]
        assert not flagged, f"live directory object misclassified as orphaned: {flagged}"

    def test_orphaned_directory_object_fully_reclaimed(self, schema_dirobj, tmp_path):
        """After deleting one row, collect() must reclaim that folder object's
        chunks AND manifest while leaving the surviving row's folder intact."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "gone.zarr"))})
        GcObjectTest.insert1({"rid": 2, "results": str(self._make_store_dir(tmp_path, "kept.zarr"))})

        collector = _gc(schema_dirobj)
        refs_all = collector.schema_references()
        assert len(refs_all) == 2

        (GcObjectTest & {"rid": 1}).delete(prompt=False)
        refs_live = collector.schema_references()
        assert len(refs_live) == 1
        live_ref = next(iter(refs_live))
        dead_ref = next(iter(refs_all - refs_live))

        collector.collect(dry_run=False)

        stored_after = set(collector.list_schema_paths(schema_dirobj.database))
        live_files = {p for p in stored_after if p.startswith(live_ref + "/")}
        assert len(live_files) >= 4, f"live folder object lost files: {sorted(stored_after)}"
        assert f"{live_ref}.manifest.json" in stored_after, "live object's manifest must survive collect()"

        dead_files = {p for p in stored_after if p == dead_ref or p.startswith(dead_ref + "/")}
        assert not dead_files, f"orphaned folder object not reclaimed: {sorted(dead_files)}"
        assert f"{dead_ref}.manifest.json" not in stored_after, "orphaned object's manifest must be reclaimed"


class GcProbeHash(dj.Manual):
    """Table whose name (`gc_probe_hash`) contains the substring `_hash` —
    regression guard for the schema-path walk's hash-subtree skip."""

    definition = """
    rid : int
    ---
    results : <object@local>
    """


class TestHashSubstringTableName:
    """A table whose name contains the substring `_hash` (e.g. `gc_probe_hash`)
    is listed and reclaimed normally. Per-schema scoping walks only the
    schema's own section (`{schema_prefix}/{schema}/`), so such a table's
    objects are ordinary schema-addressed files — there is no store-wide
    hash-subtree skip that a substring could trip over."""

    @pytest.fixture
    def schema_probe(self, connection_test, prefix, mock_stores):
        schema = dj.Schema(
            f"{prefix}_test_gc_hashname",
            context={"GcProbeHash": GcProbeHash},
            connection=connection_test,
        )
        schema(GcProbeHash)
        yield schema
        schema.drop()

    def test_hash_substring_table_listed_and_reclaimed(self, schema_probe):
        GcProbeHash.insert1({"rid": 1, "results": b"keep"})
        GcProbeHash.insert1({"rid": 2, "results": b"drop"})

        collector = _gc(schema_probe)
        refs = collector.schema_references()
        assert len(refs) == 2
        stored = set(collector.list_schema_paths(schema_probe.database))
        missing = refs - stored
        assert not missing, (
            f"files of a table whose name contains '_hash' must be listed; missing {missing} "
            "(substring-based skip would hide them from GC forever)"
        )

        (GcProbeHash & {"rid": 2}).delete(prompt=False)
        refs_live = collector.schema_references()
        dead_ref = next(iter(refs - refs_live))

        collector.collect(dry_run=False)
        stored_after = set(collector.list_schema_paths(schema_probe.database))
        assert dead_ref not in stored_after, "orphan under a '_hash'-substring table name must be reclaimed"
        assert next(iter(refs_live)) in stored_after, "live file must survive"


class TestUserFilesOutsideSchemaSectionUntouched:
    """Per-schema scoping walks only `{schema_prefix}/{schema}/`, so any file
    outside that section — including user-managed <filepath@> content anywhere
    else in the store — is structurally never a GC candidate. The old
    cohabitation hazard (user files misread as orphans) is gone by
    construction, with or without a declared filepath_prefix."""

    @pytest.fixture
    def schema_fp(self, connection_test, prefix, mock_stores):
        schema = dj.Schema(
            f"{prefix}_test_gc_userfiles",
            context={"GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcObjectTest)
        yield schema
        schema.drop()

    def test_user_file_outside_section_never_listed_or_collected(self, schema_fp):
        from pathlib import Path

        cfg = schema_fp.connection._config
        location = Path(cfg.get_store_spec("local")["location"])
        user_file = location / "userfiles" / "deep" / "important.bin"
        user_file.parent.mkdir(parents=True, exist_ok=True)
        user_file.write_bytes(b"user-managed")

        GcObjectTest.insert1({"rid": 1, "results": b"managed-object"})

        # The user file lives outside {schema_prefix}/{schema}/, so the scoped
        # walk never lists it — no filepath_prefix declaration needed.
        collector = _gc(schema_fp)
        stored = collector.list_schema_paths(schema_fp.database)
        assert "userfiles/deep/important.bin" not in stored
        assert stored, "the schema's own managed object must still be listed"

        collector.collect(dry_run=False)
        assert user_file.exists(), "collect() must never touch files outside the schema section"


class TestPrefixSettingsHonored:
    """The per-store hash_prefix/schema_prefix settings control the layout
    (docs: how-to/configure-storage): writers place objects under the
    configured sections and GC scans the same sections — relocation without
    drift between components."""

    @pytest.fixture
    def schema_prefixed(self, connection_test, prefix, mock_stores):
        cfg = connection_test._config
        cfg.stores["local"]["hash_prefix"] = "content"
        cfg.stores["local"]["schema_prefix"] = "objects"
        schema = dj.Schema(
            f"{prefix}_test_gc_prefixes",
            context={"GcBlobTest": GcBlobTest, "GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcBlobTest)
        schema(GcObjectTest)
        yield schema
        schema.drop()
        cfg.stores["local"].pop("hash_prefix", None)
        cfg.stores["local"].pop("schema_prefix", None)

    def test_custom_sections_written_scanned_and_collected(self, schema_prefixed):
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(16, dtype="uint8")})
        GcObjectTest.insert1({"rid": 1, "results": b"payload-one"})
        GcObjectTest.insert1({"rid": 2, "results": b"payload-two"})

        # Writers honored the configured sections (metadata records the real paths)
        collector = _gc(schema_prefixed)
        hash_refs = collector.hash_references()
        assert hash_refs and all(r.startswith("content/") for r in hash_refs), hash_refs
        obj_refs = collector.schema_references()
        assert len(obj_refs) == 2 and all(r.startswith("objects/") for r in obj_refs), obj_refs

        # GC scans the same sections: everything referenced, nothing falsely orphaned
        stats = collector.collect()
        assert stats["hash_paths_referenced"] >= 1
        assert not (hash_refs & set(stats["orphaned_hash_paths"]))
        assert not (obj_refs & set(stats["orphaned_schema_paths"]))

        # And reclamation works within the configured sections
        (GcObjectTest & {"rid": 2}).delete(prompt=False)
        collector.collect(dry_run=False)
        stored_after = set(collector.list_schema_paths(schema_prefixed.database))
        live = collector.schema_references()
        assert live <= stored_after, "live object under custom section must survive"
        assert not ((obj_refs - live) & stored_after), "orphan under custom section must be reclaimed"


class TestHashObjectsOutsideCurrentSection:
    """A live hash object is protected from GC even if it lives OUTSIDE the
    currently configured hash section — e.g. after `hash_prefix` is changed on
    a populated store. Reads always work (metadata records the full path); GC
    must not misclassify such objects as schema-addressed orphans and delete
    live data. Regression for the prefix-change corner."""

    @pytest.fixture
    def schema_pfx(self, connection_test, prefix, mock_stores):
        schema = dj.Schema(
            f"{prefix}_test_gc_hashmove",
            context={"GcBlobTest": GcBlobTest, "GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        schema(GcBlobTest)
        schema(GcObjectTest)
        yield schema
        schema.drop()

    def test_live_hash_object_survives_prefix_change(self, schema_pfx):
        cfg = schema_pfx.connection._config
        # Written under the default _hash/ section.
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(8, dtype="uint8")})
        GcObjectTest.insert1({"rid": 1, "results": b"schema-obj"})

        # Relocate the hash section: new writes would go to content/, but the
        # existing object stays at _hash/ (metadata keeps its full path).
        cfg.stores["local"]["hash_prefix"] = "content"
        try:
            # Construct AFTER the flip so the collector pins the current
            # (content) prefix. The reference is the metadata path, recorded
            # when the object was written under _hash — unaffected by the flip.
            collector = _gc(schema_pfx)
            hash_ref = next(iter(collector.hash_references()))
            assert hash_ref.startswith("_hash/")

            # Read still works via the metadata path.
            assert (GcBlobTest & {"rid": 1}).fetch1("payload") is not None

            stats = collector.collect()
            assert hash_ref not in set(
                stats["orphaned_schema_paths"]
            ), "live hash object outside the current hash section must not be a schema orphan"
            assert hash_ref not in set(stats["orphaned_hash_paths"])

            # End-to-end: collect must not destroy the live hash object.
            collector.collect(dry_run=False)
            from datajoint.hash_registry import get_store_backend

            backend = get_store_backend("local", config=cfg)
            assert backend.exists(hash_ref), "collect() deleted a live hash object after prefix change"
            assert (GcBlobTest & {"rid": 1}).fetch1("payload") is not None
        finally:
            cfg.stores["local"].pop("hash_prefix", None)


class TestPerSchemaScoping:
    """GC operates per schema: because every object path embeds the schema and
    hash dedup is per-schema, scanning/collecting one schema is confined to
    that schema's subfolder and can never see or delete another schema's
    objects — even when both share one store. Removes the old 'pass every
    schema or lose data' hazard."""

    @pytest.fixture
    def two_schemas(self, connection_test, prefix, mock_stores):
        # Unique names per test: the shared `local` store persists objects
        # across tests, but per-schema scoping means leftovers under other
        # schema names are invisible here — so unique names give clean counts.
        import time

        uid = str(int(time.time() * 1000))[-9:]
        a = dj.Schema(
            f"{prefix}_gcsc_a{uid}",
            context={"GcBlobTest": GcBlobTest, "GcObjectTest": GcObjectTest},
            connection=connection_test,
        )
        b = dj.Schema(
            f"{prefix}_gcsc_b{uid}",
            connection=connection_test,
        )
        a(GcBlobTest)
        a(GcObjectTest)
        # Distinct classes bound to schema b (same definitions, second schema).
        b_blob = type("GcBlobTest", (dj.Manual,), {"definition": GcBlobTest.definition})
        b_obj = type("GcObjectTest", (dj.Manual,), {"definition": GcObjectTest.definition})
        b(b_blob)
        b(b_obj)
        yield a, b, b_blob, b_obj
        b.drop()
        a.drop()

    def test_list_scoped_to_schema(self, two_schemas):
        a, b, b_blob, b_obj = two_schemas
        collector = _gc(a)
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        GcObjectTest.insert1({"rid": 1, "results": b"a-obj"})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-obj"})

        a_hashes = set(collector.list_hash_paths(a.database))
        a_paths = set(collector.list_schema_paths(a.database))
        assert a_hashes and all(f"/{a.database}/" in h for h in a_hashes), a_hashes
        assert a_paths and all(p.split("/")[0] == a.database or f"/{a.database}/" in p for p in a_paths), a_paths
        # nothing from schema b leaked into schema a's scoped listings
        assert not any(b.database in p for p in a_hashes | a_paths)

    def test_scan_one_schema_ignores_the_other(self, two_schemas):
        a, b, b_blob, b_obj = two_schemas
        collector = _gc(a)
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-obj"})

        stats = collector.collect()
        # b's live objects are outside a's subtree → not counted, not orphaned
        assert stats["hash_paths_orphaned"] == 0 and stats["schema_paths_orphaned"] == 0
        assert not any(b.database in p for p in stats["orphaned_schema_paths"] + stats["orphaned_hash_paths"])

    def test_collect_one_schema_never_touches_the_other(self, two_schemas):
        a, b, b_blob, b_obj = two_schemas
        collector = _gc(a)
        # a has an orphan (insert then delete); b has only live data
        GcObjectTest.insert1({"rid": 1, "results": b"a-doomed"})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-live"})

        a_orphan = next(iter(collector.schema_references()))
        (GcObjectTest & {"rid": 1}).delete(prompt=False)

        b_hashes_before = set(collector.list_hash_paths(b.database))
        b_paths_before = set(collector.list_schema_paths(b.database))
        assert b_hashes_before and b_paths_before

        collector.collect(dry_run=False)

        # a's orphan reclaimed; b entirely intact
        assert a_orphan not in set(collector.list_schema_paths(a.database))
        assert set(collector.list_hash_paths(b.database)) == b_hashes_before
        assert set(collector.list_schema_paths(b.database)) == b_paths_before
        assert len(b_obj()) == 1  # b's row (and its object) untouched


class TestScanErrorGuard:
    """A walk failure during list_*_paths (anything other than FileNotFoundError,
    which just means the section doesn't exist yet) leaves the stored listing
    partial. Comparing partial listings against complete reference sets would
    classify live files as orphaned — silent data loss under dry_run=False.
    The scan-error guard captures such errors and refuses to delete."""

    def test_scan_errors_reset_between_collect_calls(self):
        """_scan_errors is reset at the start of each collect() call, so an
        error from a prior collect() never carries over into the next report."""
        with ExitStack() as es:
            for p in TestCollect._patch_data(TestCollect):
                es.enter_context(p)
            es.enter_context(patch("datajoint.gc.delete_path"))
            collector = _mock_collector()
            # Simulate a leftover error from a previous run.
            collector._scan_errors = ["stale error from previous call"]

            stats = collector.collect()  # dry_run=True

        assert stats["scan_errors"] == [], "scan_errors must be reset at the start of collect()"
        assert collector._scan_errors == [], "the collector's _scan_errors must be cleared for the new run"

    def test_dry_run_false_raises_when_scan_errors_present(self):
        """collect(dry_run=False) must refuse to delete when list_*_paths hit a
        non-FileNotFoundError walk failure — partial listing means live files
        could be misclassified as orphans."""
        # Real walk failure: mock fs.walk on list_hash_paths to raise a
        # non-FileNotFoundError, exercising the outer except that records the
        # scan error.
        with ExitStack() as es:
            es.enter_context(patch.object(gc.GarbageCollector, "hash_references", return_value=set()))
            es.enter_context(patch.object(gc.GarbageCollector, "schema_references", return_value=set()))

            # Real list_hash_paths runs and hits fs.walk failure; list_schema_paths
            # is stubbed to a clean empty listing so only the hash walk errors.
            es.enter_context(patch.object(gc.GarbageCollector, "list_schema_paths", return_value={}))

            collector = _mock_collector()
            collector.backend.fs.walk.side_effect = PermissionError("s3 access denied")
            collector.backend._full_path.return_value = "/root"

            mock_delete = es.enter_context(patch("datajoint.gc.delete_path"))
            with pytest.raises(DataJointError, match="Refusing to delete"):
                collector.collect(dry_run=False)

            # And no deletion happened.
            mock_delete.assert_not_called()

    def test_scan_errors_in_stats_dict(self):
        """The returned stats dict always includes a 'scan_errors' key: empty
        list on a clean scan, list of "<method>(<schema>): <exception>" strings
        when a walk failed."""
        # Clean case — scan_errors is an empty list.
        with ExitStack() as es:
            for p in TestCollect._patch_data(TestCollect):
                es.enter_context(p)
            es.enter_context(patch("datajoint.gc.delete_path"))
            stats = _mock_collector().collect()

        assert "scan_errors" in stats
        assert stats["scan_errors"] == []

        # Error case — list of strings identifying which listing failed.
        with ExitStack() as es:
            es.enter_context(patch.object(gc.GarbageCollector, "hash_references", return_value=set()))
            es.enter_context(patch.object(gc.GarbageCollector, "schema_references", return_value=set()))
            es.enter_context(patch.object(gc.GarbageCollector, "list_schema_paths", return_value={}))

            collector = _mock_collector()
            collector.schemas[0].database = "s"
            collector.backend.fs.walk.side_effect = PermissionError("s3 access denied")
            collector.backend._full_path.return_value = "/root"

            stats = collector.collect()  # dry_run=True → returns rather than raising

        assert len(stats["scan_errors"]) == 1
        assert stats["scan_errors"][0].startswith("list_hash_paths(s):")
        assert "s3 access denied" in stats["scan_errors"][0]
