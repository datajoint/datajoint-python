"""Storage classification by codec chain, and the garbage collector's use of it.

A custom codec that stores through ``<hash@>`` (directly, or through ``<blob@>``
or ``<attach@>``) or through a schema-addressed codec must be classified by its
resolved codec chain, not by its name. Otherwise the collector never scans its
column, and its stored objects look orphaned. See datajoint/datajoint-python#1700.
"""

import json
import logging
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

import datajoint as dj
from datajoint.codecs import get_codec
from datajoint.errors import DataJointError
from datajoint.gc import GarbageCollector
from datajoint.heading import Attribute, Heading, _MissingType, default_attribute_properties


class GcUnitHashChain(dj.Codec):
    """Stores through ``<hash@>``: the HashChain codec of #1700."""

    name = "gc_unit_hashchain"

    def get_dtype(self, is_store):
        return "<hash>"

    def encode(self, value, *, key=None, context=None, store_name=None):
        return value.encode()

    def decode(self, stored, *, key=None, context=None):
        return stored.decode()


class GcUnitBlobChain(dj.Codec):
    """Stores through ``<blob>``, which is ``<blob@>`` with a store: the BlobChain codec of #1700."""

    name = "gc_unit_blobchain"

    def get_dtype(self, is_store):
        return "<blob>"

    def encode(self, value, *, key=None, context=None, store_name=None):
        return value

    def decode(self, stored, *, key=None, context=None):
        return stored


class GcUnitNpyChain(dj.Codec):
    """Stores through ``<npy@>``, a schema-addressed codec."""

    name = "gc_unit_npychain"

    def get_dtype(self, is_store):
        return "<npy>"

    def encode(self, value, *, key=None, context=None, store_name=None):
        return np.asarray(value)

    def decode(self, stored, *, key=None, context=None):
        return stored


class GcUnitMissingInner(dj.Codec):
    """Stores through a codec that is not installed."""

    name = "gc_unit_missing_inner"

    def get_dtype(self, is_store):
        return "<gc_unit_not_installed>"

    def encode(self, value, *, key=None, context=None, store_name=None):
        return value

    def decode(self, stored, *, key=None, context=None):
        return stored


def _attr(name="a", **overrides):
    return Attribute(**{**default_attribute_properties, "name": name, **overrides})


class TestClassificationByCodecChain:
    """``Attribute.is_hash_object`` and ``is_schema_object`` follow the codec chain."""

    @pytest.mark.parametrize(
        ("codec", "store", "is_hash", "is_schema"),
        [
            pytest.param("gc_unit_hashchain", "", True, False, id="custom-through-hash"),
            pytest.param("gc_unit_blobchain", "", True, False, id="custom-through-blob"),
            pytest.param("gc_unit_blobchain", "local", True, False, id="custom-through-blob-named-store"),
            pytest.param("gc_unit_blobchain", None, False, False, id="custom-through-inline-blob"),
            pytest.param("gc_unit_npychain", "", False, True, id="custom-through-npy"),
            pytest.param("hash", "", True, False, id="hash"),
            pytest.param("blob", "local", True, False, id="blob-external"),
            pytest.param("attach", "local", True, False, id="attach-external"),
            pytest.param("blob", None, False, False, id="blob-inline"),
            pytest.param("attach", None, False, False, id="attach-inline"),
            pytest.param("object", "local", False, True, id="object"),
            pytest.param("npy", "local", False, True, id="npy"),
            pytest.param("filepath", "local", False, False, id="filepath"),
        ],
    )
    def test_classification(self, codec, store, is_hash, is_schema):
        attr = _attr(codec=get_codec(codec), store=store)
        assert attr.is_hash_object is is_hash
        assert attr.is_schema_object is is_schema

    def test_no_codec(self):
        attr = _attr(codec=None)
        assert attr.is_hash_object is False
        assert attr.is_schema_object is False

    @pytest.mark.parametrize(
        "codec",
        [
            pytest.param(_MissingType("<gc_unit_not_installed@>"), id="codec-not-installed"),
            pytest.param(get_codec("gc_unit_missing_inner"), id="inner-codec-not-installed"),
        ],
    )
    def test_unresolvable_chain_counts_as_both(self, codec):
        """Where the values are stored is unknown, so the collector must scan the column."""
        attr = _attr(codec=codec, store="")
        assert attr.is_hash_object is True
        assert attr.is_schema_object is True

    def test_missing_codec_cannot_report_references(self):
        """A codec that is not installed raises instead of reporting no references."""
        with pytest.raises(DataJointError, match="is not registered"):
            _MissingType("<gc_unit_not_installed@>").referenced_paths('{"path": "_hash/s/x"}')


def _metadata(path):
    # MySQL returns JSON columns as text; Codec.referenced_paths parses either form.
    return json.dumps({"hash": path.rsplit("/", 1)[-1], "path": path, "store": "local", "size": 1})


NOTE_ROW = {
    "text": _metadata("_hash/s/aaaaaaaaaaaaaaaaaaaaaaaaaa"),
    "obj": _metadata("_hash/s/bbbbbbbbbbbbbbbbbbbbbbbbbb"),
    "ref": _metadata("_hash/s/cccccccccccccccccccccccccc"),
    "inline": b"inline bytes",
}
NOTE_PATHS = {"_hash/s/aaaaaaaaaaaaaaaaaaaaaaaaaa", "_hash/s/bbbbbbbbbbbbbbbbbbbbbbbbbb", "_hash/s/cccccccccccccccccccccccccc"}


def _note_heading(**extra):
    """The Note table of #1700, plus an inline column that holds no stored object."""
    specs = [
        _attr("note_id", type="int", in_key=True),
        _attr("text", type="<gc_unit_hashchain@>", codec=get_codec("gc_unit_hashchain"), store=""),
        _attr("obj", type="<gc_unit_blobchain@>", codec=get_codec("gc_unit_blobchain"), store=""),
        _attr("ref", type="<blob@>", codec=get_codec("blob"), store=""),
        _attr("inline", type="<gc_unit_blobchain>", codec=get_codec("gc_unit_blobchain")),
    ]
    specs += [_attr(name, **props) for name, props in extra.items()]
    return Heading([spec._asdict() for spec in specs])


def _collector(heading, row):
    """A collector over one mock schema ``s`` holding one table ``note`` with ``heading``
    and a single ``row``; the store ``local`` is the default store."""
    table = MagicMock()
    table.heading = heading
    table.proj.side_effect = lambda name: MagicMock(cursor=MagicMock(return_value=[{name: row[name]}]))
    schema = MagicMock()
    schema.database = "s"
    schema.list_tables.return_value = ["note"]
    schema.get_table.return_value = table
    config = MagicMock()
    config.stores = {"default": "local"}
    config.get_store_spec.return_value = {"hash_prefix": "_hash", "schema_prefix": "_schema"}
    with patch("datajoint.gc.get_store_backend", return_value=MagicMock()):
        return GarbageCollector(schema, store="local", config=config)


class TestReferenceScanByCodecChain:
    """The repro of #1700 at unit level: the collector scans the columns of custom
    codecs that store through ``<hash@>``, and keeps their objects."""

    def test_heading_lists_custom_hash_columns(self):
        heading = _note_heading()
        assert heading.hash_objects == ["text", "obj", "ref"]
        assert heading.schema_objects == []

    def test_hash_references_include_custom_codec_columns(self):
        assert _collector(_note_heading(), NOTE_ROW).hash_references() == NOTE_PATHS

    def test_collect_keeps_objects_of_custom_codecs(self):
        """collect(dry_run=False) deletes no object that a row references, whatever codec stored it."""
        collector = _collector(_note_heading(), NOTE_ROW)
        stored = {path: 10 for path in NOTE_PATHS}
        with (
            patch.object(GarbageCollector, "list_hash_paths", return_value=stored),
            patch.object(GarbageCollector, "list_schema_paths", return_value={}),
            patch("datajoint.gc.delete_path") as delete_path,
        ):
            stats = collector.collect(dry_run=False)

        assert stats["hash_paths_referenced"] == 3
        assert stats["orphaned_hash_paths"] == []
        assert stats["deleted"] == 0
        delete_path.assert_not_called()

    def test_missing_codec_column_scan_fails_without_hiding_other_columns(self, caplog):
        """A column whose codec is not installed is scanned, and its scan fails
        visibly. The other columns of the table are still scanned."""
        missing = _MissingType("<gc_unit_not_installed@>")
        heading = _note_heading(missing={"type": "<gc_unit_not_installed@>", "codec": missing, "store": ""})
        row = {**NOTE_ROW, "missing": _metadata("_hash/s/dddddddddddddddddddddddddd")}
        with caplog.at_level(logging.WARNING, logger="datajoint"):
            references = _collector(heading, row).hash_references()

        assert references == NOTE_PATHS
        assert any("note.missing" in r.getMessage() and "is not registered" in r.getMessage() for r in caplog.records)
