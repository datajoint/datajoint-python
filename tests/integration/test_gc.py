"""
Tests for garbage collection (gc.py).
"""

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


class TestScan:
    """Tests for scan function."""

    def test_requires_at_least_one_schema(self):
        """Test that at least one schema is required."""
        with pytest.raises(DataJointError, match="At least one schema must be provided"):
            gc.scan()

    @patch("datajoint.gc.scan_schema_references")
    @patch("datajoint.gc.list_schema_paths")
    @patch("datajoint.gc.scan_hash_references")
    @patch("datajoint.gc.list_hash_paths")
    def test_returns_stats(self, mock_list_hashes, mock_scan_hash, mock_list_schemas, mock_scan_schema):
        """Test that scan returns proper statistics."""
        # Mock hash-addressed storage (now uses paths)
        mock_scan_hash.return_value = {"_hash/schema/path1", "_hash/schema/path2"}
        mock_list_hashes.return_value = {
            "_hash/schema/path1": 100,
            "_hash/schema/path3": 200,  # orphaned
        }

        # Mock schema-addressed storage
        mock_scan_schema.return_value = {"schema/table/pk1/field"}
        mock_list_schemas.return_value = {
            "schema/table/pk1/field": 500,
            "schema/table/pk2/field": 300,  # orphaned
        }

        mock_schema = MagicMock()
        stats = gc.scan(mock_schema, store_name="test_store")

        # Hash stats
        assert stats["hash_referenced"] == 2
        assert stats["hash_stored"] == 2
        assert stats["hash_orphaned"] == 1
        assert "_hash/schema/path3" in stats["orphaned_hashes"]

        # Schema stats
        assert stats["schema_paths_referenced"] == 1
        assert stats["schema_paths_stored"] == 2
        assert stats["schema_paths_orphaned"] == 1
        assert "schema/table/pk2/field" in stats["orphaned_paths"]

        # Combined totals
        assert stats["referenced"] == 3
        assert stats["stored"] == 4
        assert stats["orphaned"] == 2
        assert stats["orphaned_bytes"] == 500  # 200 hash + 300 schema


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
            "orphaned_hashes": ["_hash/schema/orphan_path"],
            "orphaned_paths": [],
            "hash_orphaned": 1,
            "schema_paths_orphaned": 0,
        }

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=True)

        assert stats["deleted"] == 0
        assert stats["bytes_freed"] == 0
        assert stats["dry_run"] is True

    @patch("datajoint.gc.delete_path")
    @patch("datajoint.gc.list_hash_paths")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_hashes(self, mock_scan, mock_list_stored, mock_delete):
        """Test that orphaned hashes are deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 100,
            "orphaned_hashes": ["_hash/schema/orphan_path"],
            "orphaned_paths": [],
            "hash_orphaned": 1,
            "schema_paths_orphaned": 0,
        }
        mock_list_stored.return_value = {"_hash/schema/orphan_path": 100}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["hash_deleted"] == 1
        assert stats["bytes_freed"] == 100
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("_hash/schema/orphan_path", "test_store", config=mock_schema.connection._config)

    @patch("datajoint.gc.delete_schema_path")
    @patch("datajoint.gc.list_schema_paths")
    @patch("datajoint.gc.scan")
    def test_deletes_orphaned_schemas(self, mock_scan, mock_list_schemas, mock_delete):
        """Test that orphaned schema paths are deleted when dry_run=False."""
        mock_scan.return_value = {
            "referenced": 1,
            "stored": 2,
            "orphaned": 1,
            "orphaned_bytes": 500,
            "orphaned_hashes": [],
            "orphaned_paths": ["schema/table/pk/field"],
            "hash_orphaned": 0,
            "schema_paths_orphaned": 1,
        }
        mock_list_schemas.return_value = {"schema/table/pk/field": 500}
        mock_delete.return_value = True

        mock_schema = MagicMock()
        stats = gc.collect(mock_schema, store_name="test_store", dry_run=False)

        assert stats["deleted"] == 1
        assert stats["schema_paths_deleted"] == 1
        assert stats["bytes_freed"] == 500
        assert stats["dry_run"] is False
        mock_delete.assert_called_once_with("schema/table/pk/field", "test_store", config=mock_schema.connection._config)


class TestScanWithLiveData:
    """End-to-end tests for gc.scan() against real schemas with external storage.

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
        """scan() must report hash_referenced >= 1 for a populated <blob@> column.

        Decoded value type returned by BlobCodec.decode is numpy.ndarray, which
        does not satisfy the raw-metadata dict/JSON-string check in referenced_paths — this
        test fails before the cursor-based fix in scan_hash_references.
        """
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(64, dtype="uint8")})

        stats = gc.scan(schema_blob, store_name="local")

        assert stats["hash_referenced"] >= 1, f"scan should find the active <blob@> reference; got {stats}"

    def test_scan_finds_active_npy_reference(self, schema_npy):
        """scan() must report schema_paths_referenced >= 1 for a populated <npy@> column.

        Decoded value type returned by NpyCodec.decode is NpyRef (lazy handle),
        which does not satisfy the raw-metadata dict check in referenced_paths — this test
        fails before the cursor-based fix in scan_schema_references.
        """
        GcNpyTest.insert1({"rid": 1, "waveform": np.arange(64, dtype="float32")})

        stats = gc.scan(schema_npy, store_name="local")

        assert stats["schema_paths_referenced"] >= 1, f"scan should find the active <npy@> reference; got {stats}"

    def test_scan_finds_active_object_reference(self, schema_object):
        """scan() must report schema_paths_referenced >= 1 for a populated <object@> column.

        Decoded value type returned by ObjectCodec.decode is ObjectRef (lazy
        handle), which does not satisfy the raw-metadata dict check in referenced_paths —
        this test fails before the cursor-based fix in scan_schema_references.
        """
        GcObjectTest.insert1({"rid": 1, "results": b"hello-gc-test"})

        stats = gc.scan(schema_object, store_name="local")

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

        refs = gc.scan_schema_references(schema_custom, store_name="local")
        assert refs, "custom codec's live reference must be discovered (#1469)"
        live_path = next(iter(refs))

        stats = gc.scan(schema_custom, store_name="local")
        assert live_path not in stats["orphaned_paths"], f"live custom-codec file wrongly flagged orphan: {live_path}"

    def test_custom_codec_survives_collect(self, schema_custom):
        """#1469 end-to-end data-loss guard: collect() must delete only the
        deleted row's file and keep the surviving row's file (checked by exact
        path, robust to a shared store)."""
        GcCustomCodecTest.insert1({"rid": 1, "payload": b"row-one"})
        GcCustomCodecTest.insert1({"rid": 2, "payload": b"row-two"})

        refs_all = gc.scan_schema_references(schema_custom, store_name="local")
        assert len(refs_all) == 2, f"both live rows should be referenced; got {refs_all}"

        # Delete one row — its file becomes a genuine orphan (delete-then-GC).
        (GcCustomCodecTest & {"rid": 1}).delete(prompt=False)

        refs_live = gc.scan_schema_references(schema_custom, store_name="local")
        assert len(refs_live) == 1, f"one live row should remain referenced; got {refs_live}"
        live_path = next(iter(refs_live))
        deleted_paths = refs_all - refs_live

        gc.collect(schema_custom, store_name="local", dry_run=False)

        stored_after = set(
            gc.list_schema_paths("local", config=schema_custom.connection._config, schema_name=schema_custom.database)
        )
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
        its referenced prefix — none may appear in orphaned_paths."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "live.zarr"))})

        refs = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs) == 1
        ref = next(iter(refs))

        stored = gc.list_schema_paths("local", config=schema_dirobj.connection._config, schema_name=schema_dirobj.database)
        chunk_files = [p for p in stored if p.startswith(ref + "/")]
        assert len(chunk_files) >= 4, f"expected the folder's files under {ref}; got {sorted(stored)}"
        assert f"{ref}.manifest.json" in stored, "manifest sidecar must be listed (it is reclaimable state)"

        stats = gc.scan(schema_dirobj, store_name="local")
        flagged = [p for p in stats["orphaned_paths"] if p == ref or p.startswith(ref + "/") or p == f"{ref}.manifest.json"]
        assert not flagged, f"live directory object misclassified as orphaned: {flagged}"

    def test_orphaned_directory_object_fully_reclaimed(self, schema_dirobj, tmp_path):
        """After deleting one row, collect() must reclaim that folder object's
        chunks AND manifest while leaving the surviving row's folder intact."""
        GcObjectTest.insert1({"rid": 1, "results": str(self._make_store_dir(tmp_path, "gone.zarr"))})
        GcObjectTest.insert1({"rid": 2, "results": str(self._make_store_dir(tmp_path, "kept.zarr"))})

        refs_all = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs_all) == 2

        (GcObjectTest & {"rid": 1}).delete(prompt=False)
        refs_live = gc.scan_schema_references(schema_dirobj, store_name="local")
        assert len(refs_live) == 1
        live_ref = next(iter(refs_live))
        dead_ref = next(iter(refs_all - refs_live))

        gc.collect(schema_dirobj, store_name="local", dry_run=False)

        stored_after = set(
            gc.list_schema_paths("local", config=schema_dirobj.connection._config, schema_name=schema_dirobj.database)
        )
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

        refs = gc.scan_schema_references(schema_probe, store_name="local")
        assert len(refs) == 2
        stored = set(gc.list_schema_paths("local", config=schema_probe.connection._config, schema_name=schema_probe.database))
        missing = refs - stored
        assert not missing, (
            f"files of a table whose name contains '_hash' must be listed; missing {missing} "
            "(substring-based skip would hide them from GC forever)"
        )

        (GcProbeHash & {"rid": 2}).delete(prompt=False)
        refs_live = gc.scan_schema_references(schema_probe, store_name="local")
        dead_ref = next(iter(refs - refs_live))

        gc.collect(schema_probe, store_name="local", dry_run=False)
        stored_after = set(
            gc.list_schema_paths("local", config=schema_probe.connection._config, schema_name=schema_probe.database)
        )
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
        stored = gc.list_schema_paths("local", config=cfg, schema_name=schema_fp.database)
        assert "userfiles/deep/important.bin" not in stored
        assert stored, "the schema's own managed object must still be listed"

        gc.collect(schema_fp, store_name="local", dry_run=False)
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
        cfg = schema_prefixed.connection._config
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(16, dtype="uint8")})
        GcObjectTest.insert1({"rid": 1, "results": b"payload-one"})
        GcObjectTest.insert1({"rid": 2, "results": b"payload-two"})

        # Writers honored the configured sections (metadata records the real paths)
        hash_refs = gc.scan_hash_references(schema_prefixed, store_name="local")
        assert hash_refs and all(r.startswith("content/") for r in hash_refs), hash_refs
        obj_refs = gc.scan_schema_references(schema_prefixed, store_name="local")
        assert len(obj_refs) == 2 and all(r.startswith("objects/") for r in obj_refs), obj_refs

        # GC scans the same sections: everything referenced, nothing falsely orphaned
        stats = gc.scan(schema_prefixed, store_name="local")
        assert stats["hash_referenced"] >= 1
        assert not (hash_refs & set(stats["orphaned_hashes"]))
        assert not (obj_refs & set(stats["orphaned_paths"]))

        # And reclamation works within the configured sections
        (GcObjectTest & {"rid": 2}).delete(prompt=False)
        gc.collect(schema_prefixed, store_name="local", dry_run=False)
        stored_after = set(gc.list_schema_paths("local", config=cfg, schema_name=schema_prefixed.database))
        live = gc.scan_schema_references(schema_prefixed, store_name="local")
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
        hash_ref = next(iter(gc.scan_hash_references(schema_pfx, store_name="local")))
        assert hash_ref.startswith("_hash/")

        # Relocate the hash section: new writes would go to content/, but the
        # existing object stays at _hash/ (metadata keeps its full path).
        cfg.stores["local"]["hash_prefix"] = "content"
        try:
            # Read still works via the metadata path.
            assert (GcBlobTest & {"rid": 1}).fetch1("payload") is not None

            stats = gc.scan(schema_pfx, store_name="local")
            assert hash_ref not in set(
                stats["orphaned_paths"]
            ), "live hash object outside the current hash section must not be a schema orphan"
            assert hash_ref not in set(stats["orphaned_hashes"])

            # End-to-end: collect must not destroy the live hash object.
            gc.collect(schema_pfx, store_name="local", dry_run=False)
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
        cfg = a.connection._config
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        GcObjectTest.insert1({"rid": 1, "results": b"a-obj"})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-obj"})

        a_hashes = set(gc.list_hash_paths("local", config=cfg, schema_name=a.database))
        a_paths = set(gc.list_schema_paths("local", config=cfg, schema_name=a.database))
        assert a_hashes and all(f"/{a.database}/" in h for h in a_hashes), a_hashes
        assert a_paths and all(p.split("/")[0] == a.database or f"/{a.database}/" in p for p in a_paths), a_paths
        # nothing from schema b leaked into schema a's scoped listings
        assert not any(b.database in p for p in a_hashes | a_paths)

    def test_scan_one_schema_ignores_the_other(self, two_schemas):
        a, b, b_blob, b_obj = two_schemas
        GcBlobTest.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-obj"})

        stats = gc.scan(a, store_name="local")
        # b's live objects are outside a's subtree → not counted, not orphaned
        assert stats["orphaned"] == 0
        assert not any(b.database in p for p in stats["orphaned_paths"] + stats["orphaned_hashes"])

    def test_collect_one_schema_never_touches_the_other(self, two_schemas):
        a, b, b_blob, b_obj = two_schemas
        cfg = a.connection._config
        # a has an orphan (insert then delete); b has only live data
        GcObjectTest.insert1({"rid": 1, "results": b"a-doomed"})
        b_blob.insert1({"rid": 1, "payload": np.arange(4, dtype="uint8")})
        b_obj.insert1({"rid": 1, "results": b"b-live"})

        a_orphan = next(iter(gc.scan_schema_references(a, store_name="local")))
        (GcObjectTest & {"rid": 1}).delete(prompt=False)

        b_hashes_before = set(gc.list_hash_paths("local", config=cfg, schema_name=b.database))
        b_paths_before = set(gc.list_schema_paths("local", config=cfg, schema_name=b.database))
        assert b_hashes_before and b_paths_before

        gc.collect(a, store_name="local", dry_run=False)

        # a's orphan reclaimed; b entirely intact
        assert a_orphan not in set(gc.list_schema_paths("local", config=cfg, schema_name=a.database))
        assert set(gc.list_hash_paths("local", config=cfg, schema_name=b.database)) == b_hashes_before
        assert set(gc.list_schema_paths("local", config=cfg, schema_name=b.database)) == b_paths_before
        assert len(b_obj()) == 1  # b's row (and its object) untouched
