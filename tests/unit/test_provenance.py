"""Unit tests for extrinsic provenance payload assembly and tier selection.

These cover the parts that need no database: which tables get the slot, what
goes into the payload, and that the payload is JSON-serializable.
"""

import json

import pytest

from datajoint import provenance
from datajoint.settings import Config


class FakeConnection:
    """Stands in for a Connection: only conn_info and _config are read."""

    def __init__(self, config, **conn_info):
        self.conn_info = conn_info
        self._config = config


@pytest.fixture
def config():
    cfg = Config()
    cfg.provenance.capture = True
    cfg.provenance.source = {}
    cfg.jobs.version_method = None
    return cfg


@pytest.mark.parametrize(
    "table_name, expected",
    [
        ("subject", True),  # Entry
        ("session_note", True),  # Entry, single underscores are ordinary
        ("#param", False),  # Lookup
        ("_ingest", False),  # Imported
        ("__analysis", False),  # Computed
        ("subject__detail", False),  # Part of an Entry master
        ("__analysis__unit", False),  # Part of a Computed master
        ("_ingest__row", False),  # Part of an Imported master
    ],
)
def test_is_entry_table(table_name, expected):
    """Only Entry tables get the slot; parts inherit their master's."""
    assert provenance.is_entry_table(table_name) is expected


def test_payload_is_none_without_anything_to_say(config):
    """A bare timestamp is noise, so the row is left NULL instead."""
    conn = FakeConnection(config)
    assert provenance.build_payload(conn, config) is None


def test_payload_records_the_connection(config):
    conn = FakeConnection(config, user="alice", host="db.example.org", database_name="lab")
    payload = provenance.build_payload(conn, config)
    assert payload["agent"] == {"user": "alice", "host": "db.example.org", "database_name": "lab"}
    assert "time" in payload
    assert "source" not in payload
    assert "context" not in payload


def test_payload_records_the_configured_source(config):
    config.provenance.source = {"system": "PyRat", "endpoint": "https://pyrat.example.org"}
    conn = FakeConnection(config)
    payload = provenance.build_payload(conn, config)
    assert payload["source"] == {"system": "PyRat", "endpoint": "https://pyrat.example.org"}


def test_payload_records_the_ingesting_make(config):
    """A fan-out write records what wrote it, without any foreign key."""
    conn = FakeConnection(config, user="worker")
    with provenance.ingesting("`lab`.`_ingest`", {"file_id": 7}, version="abc1234"):
        payload = provenance.build_payload(conn, config)
    assert payload["context"] == {
        "table": "`lab`.`_ingest`",
        "key": {"file_id": 7},
        "version": "abc1234",
    }
    # and the context does not leak past the block
    assert "context" not in provenance.build_payload(conn, config)


def test_ingesting_context_nests_and_restores(config):
    conn = FakeConnection(config, user="worker")
    with provenance.ingesting("`lab`.`_outer`", {"a": 1}):
        with provenance.ingesting("`lab`.`_inner`", {"b": 2}):
            assert provenance.build_payload(conn, config)["context"]["table"] == "`lab`.`_inner`"
        assert provenance.build_payload(conn, config)["context"]["table"] == "`lab`.`_outer`"


def test_payload_survives_unserializable_key_values(config):
    """A key value json cannot render is stringified rather than raising."""
    import datetime
    import uuid

    conn = FakeConnection(config, user="worker")
    key = {
        "when": datetime.datetime(2026, 9, 30, 12, 0),
        "who": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "raw": b"\xde\xad",
    }
    with provenance.ingesting("`lab`.`_ingest`", key):
        payload = provenance.build_payload(conn, config)
    rendered = json.loads(provenance.serialize(payload))
    assert rendered["context"]["key"]["when"] == "2026-09-30T12:00:00"
    assert rendered["context"]["key"]["raw"] == "dead"
    assert rendered["context"]["key"]["who"] == "12345678-1234-5678-1234-567812345678"


def test_settings_defaults_to_capturing():
    """A slot nobody can rely on is a slot nobody codes against."""
    assert Config().provenance.capture is True
    assert Config().provenance.source == {}


def test_settings_come_from_the_environment(monkeypatch):
    """The Platform sets provenance per project through the usual channels."""
    monkeypatch.setenv("DJ_PROVENANCE_CAPTURE", "false")
    monkeypatch.setenv("DJ_PROVENANCE_SOURCE", '{"system": "PyRat", "endpoint": "https://x"}')
    from datajoint.settings import ProvenanceSettings

    settings = ProvenanceSettings()
    assert settings.capture is False
    assert settings.source == {"system": "PyRat", "endpoint": "https://x"}
