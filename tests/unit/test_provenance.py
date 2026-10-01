"""Unit tests for extrinsic provenance payload assembly and tier selection.

These cover the parts that need no database: which tables get the slot, what
goes into the payload, and that the payload is JSON-serializable.
"""

import json
import re

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
        ("~jobs", False),  # job table
        ("~~analysis", False),  # per-table job queue
        ("~lineage", False),  # lineage table
    ],
)
def test_is_entry_table(table_name, expected):
    """Only Entry tables get the slot; parts inherit their master's.

    Exercises the predicate `declare` and `deploy` use: a match against the
    Manual tier itself, rather than a list of prefixes to exclude.
    """
    from datajoint.user_tables import Manual

    assert bool(re.fullmatch(Manual.tier_regexp, table_name)) is expected


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


def test_entry_test_follows_the_tier_definition_not_a_prefix_list():
    """Regression for #1555 review: `~` tables were Entry tables.

    Excluding the other tiers' prefixes one by one means any tier added later is
    an Entry table until someone remembers this function. Matching
    `Manual.tier_regexp` inverts that: a name is an Entry table only if the
    library says it is.
    """
    from datajoint.user_tables import Computed, Imported, Lookup, Manual, Part

    def grants_prov(name):
        return re.fullmatch(Manual.tier_regexp, name) is not None

    assert grants_prov("subject")
    for tier in (Lookup, Imported, Computed, Part):
        sample = {Lookup: "#param", Imported: "_ingest", Computed: "__analysis", Part: "subject__detail"}[tier]
        assert re.fullmatch(tier.tier_regexp, sample), f"{sample} is not a {tier.__name__}"
        assert not grants_prov(sample)
    # The job prefix belongs to no user tier at all, which is how it slipped through.
    assert not any(re.fullmatch(t.tier_regexp, "~~analysis") for t in (Manual, Lookup, Imported, Computed, Part))
    assert not grants_prov("~~analysis")


def test_serialize_survives_a_deployment_supplied_source(config):
    """`source` is dict[str, Any]; a date in it must not break an insert."""
    import datetime
    import pathlib

    where = pathlib.Path("/mnt/raw")
    payload = {
        "time": "t",
        "source": {"when": datetime.date(2026, 1, 1), "where": where},
    }
    rendered = json.loads(provenance.serialize(payload))
    # Compare against str(Path), not a literal: the separator is platform-specific
    # and the contract under test is that the value is stringified at all.
    assert rendered["source"] == {"when": "2026-01-01", "where": str(where)}


def test_source_must_be_serializable_at_assignment():
    """The error belongs where the setting is made, not inside an unrelated insert."""
    from pydantic import ValidationError

    from datajoint.settings import ProvenanceSettings

    cyclic: dict = {}
    cyclic["self"] = cyclic
    with pytest.raises(ValidationError, match="JSON-serializable"):
        ProvenanceSettings(source=cyclic)
