"""
Rows written directly to the database cannot direct fetches outside the store
or the download path.

Each test inserts a valid row through DataJoint, then rewrites the stored value
with SQL, as anyone with write access to the table could, and fetches it.
"""

import json
from pathlib import Path

import pytest

import datajoint as dj


class Document(dj.Manual):
    definition = """
    doc_id : int32
    ---
    inline_file : <attach>
    stored_file : <attach@local>
    """


class Reference(dj.Manual):
    definition = """
    ref_id : int32
    ---
    file_ref : <filepath@repo>
    """


class Output(dj.Manual):
    definition = """
    output_id : int32
    ---
    result : <object@local>
    """


@pytest.fixture
def schema_paths(connection_test, mock_stores, prefix):
    schema = dj.Schema(prefix + "_path_confinement", connection=connection_test)
    schema(Document)
    schema(Reference)
    schema(Output)
    yield schema
    schema.drop()


@pytest.fixture
def outside(tmp_path):
    """A file outside every store and download path."""
    path = tmp_path / "outside" / "secret.txt"
    path.parent.mkdir()
    path.write_bytes(b"secret")
    return path


def _overwrite(table, attr, value, **key):
    """Rewrite one stored value with SQL, bypassing the codec."""
    q = table.connection.adapter.quote_identifier
    (pk_name, pk_value) = next(iter(key.items()))
    table.connection.query(
        f"UPDATE {table.full_table_name} SET {q(attr)} = %s WHERE {q(pk_name)} = %s",
        args=(value, pk_value),
    )


def _json(table, attr, **key):
    q = table.connection.adapter.quote_identifier
    (pk_name, pk_value) = next(iter(key.items()))
    raw = table.connection.query(
        f"SELECT {q(attr)} FROM {table.full_table_name} WHERE {q(pk_name)} = %s", args=(pk_value,)
    ).fetchone()[0]
    return json.loads(raw) if isinstance(raw, (str, bytes)) else dict(raw)


@pytest.mark.parametrize("name", ["../escaped.txt", "../../escaped.txt", "sub/escaped.txt", "/tmp/escaped-dj.txt"])
def test_attach_fetch_refuses_name_with_directory(schema_paths, tmp_path, name):
    source = tmp_path / "report.txt"
    source.write_bytes(b"report")
    Document.insert1(dict(doc_id=1, inline_file=source, stored_file=source))
    _overwrite(Document, "inline_file", name.encode() + b"\x00payload", doc_id=1)

    download = tmp_path / "a" / "b" / "downloads"
    with dj.config.override(download_path=str(download)):
        with pytest.raises(dj.DataJointError, match="not a plain file name"):
            (Document & "doc_id=1").fetch1("inline_file")
    assert not (tmp_path / "a" / "b" / "escaped.txt").exists()
    assert not (tmp_path / "a" / "escaped.txt").exists()
    assert not (download / "sub").exists()


def test_attach_fetch_still_round_trips(schema_paths, tmp_path):
    source = tmp_path / "report.txt"
    source.write_bytes(b"report")
    Document.insert1(dict(doc_id=1, inline_file=source, stored_file=source))
    download = tmp_path / "downloads"
    with dj.config.override(download_path=str(download)):
        inline, stored = (Document & "doc_id=1").fetch1("inline_file", "stored_file")
    assert Path(inline).parent == download
    assert Path(inline).read_bytes() == b"report"
    assert Path(stored).read_bytes() == b"report"


def test_hash_fetch_refuses_path_outside_store(schema_paths, tmp_path, outside):
    source = tmp_path / "report.txt"
    source.write_bytes(b"report")
    Document.insert1(dict(doc_id=1, inline_file=source, stored_file=source))
    meta = _json(Document, "stored_file", doc_id=1)
    meta["path"] = str(outside)
    _overwrite(Document, "stored_file", json.dumps(meta), doc_id=1)
    with dj.config.override(download_path=str(tmp_path / "downloads")):
        with pytest.raises(dj.DataJointError, match="outside the store location"):
            (Document & "doc_id=1").fetch1("stored_file")


def test_filepath_insert_refuses_path_outside_store(schema_paths, outside):
    repo = Path(dj.config.stores["repo"]["location"])
    for path in (str(outside), "../" * len(repo.parts) + str(outside).lstrip("/")):
        with pytest.raises(dj.DataJointError, match="outside the store location"):
            Reference.insert1(dict(ref_id=1, file_ref=path))
    assert len(Reference()) == 0


@pytest.mark.parametrize("hostile", ["absolute", "relative", "device"])
def test_filepath_fetch_refuses_path_outside_store(schema_paths, outside, hostile):
    repo = Path(dj.config.stores["repo"]["location"])
    (repo / "inside.txt").write_bytes(b"inside")
    Reference.insert1(dict(ref_id=1, file_ref="inside.txt"))
    assert (Reference & "ref_id=1").fetch1("file_ref").read() == b"inside"

    meta = _json(Reference, "file_ref", ref_id=1)
    meta["path"] = {
        "absolute": str(outside),
        "relative": "../" * len(repo.parts) + str(outside).lstrip("/"),
        "device": "/dev/zero",
    }[hostile]
    _overwrite(Reference, "file_ref", json.dumps(meta), ref_id=1)
    ref = (Reference & "ref_id=1").fetch1("file_ref")
    with pytest.raises(dj.DataJointError, match="outside the store location"):
        ref.read()


def test_filepath_outside_store_when_allowed(schema_paths, outside):
    with dj.config.override(allow_filepath_outside_store=True):
        Reference.insert1(dict(ref_id=1, file_ref=str(outside)))
        assert (Reference & "ref_id=1").fetch1("file_ref").read() == b"secret"
    with pytest.raises(dj.DataJointError, match="outside the store location"):
        (Reference & "ref_id=1").fetch1("file_ref").read()


def test_object_fetch_refuses_path_outside_store(schema_paths, outside):
    Output.insert1(dict(output_id=1, result=b"result"))
    assert (Output & "output_id=1").fetch1("result").read() == b"result"
    meta = _json(Output, "result", output_id=1)
    meta["path"] = str(outside)
    _overwrite(Output, "result", json.dumps(meta), output_id=1)
    with pytest.raises(dj.DataJointError, match="outside the store location"):
        (Output & "output_id=1").fetch1("result").read()


def test_object_insert_refuses_metadata_outside_store(schema_paths, outside):
    with pytest.raises(dj.DataJointError, match="outside the store location"):
        Output.insert1(dict(output_id=1, result={"path": str(outside), "store": "local", "size": 6}))
    assert len(Output()) == 0
