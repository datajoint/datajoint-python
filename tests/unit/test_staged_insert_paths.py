"""Staged insert path and store-name regression tests."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from datajoint.staged_insert import StagedInsert


class FakeConfig:
    def __init__(self, default="local"):
        self.default = default
        self.specs = {
            "local": {"schema_prefix": "_schema", "token_length": 8},
            "archive": {"schema_prefix": "_schema", "token_length": 8},
        }

    def resolve_store_name(self, store=None):
        return store or self.default

    def get_store_spec(self, store=None):
        return self.specs[self.resolve_store_name(store)]


def fake_table(*, class_name, table_name, store_name, config):
    return SimpleNamespace(
        class_name=class_name,
        table_name=table_name,
        database="test_schema",
        primary_key=["id"],
        heading={"data": SimpleNamespace(codec=SimpleNamespace(name="object"), store=store_name)},
        connection=SimpleNamespace(_config=config),
    )


@pytest.mark.parametrize(
    "class_name, table_name, declared_store, expected_store",
    [
        ("Recording", "recording", "", "local"),
        ("Item", "session__item", "", "local"),
        ("Recording", "recording", "archive", "archive"),
    ],
)
def test_staged_insert_uses_sql_table_name_and_resolved_store(
    monkeypatch, class_name, table_name, declared_store, expected_store
):
    config = FakeConfig()
    backend = MagicMock()
    backend.fs.isdir.return_value = False
    backend.size.return_value = 12
    get_backend = MagicMock(return_value=backend)
    monkeypatch.setattr(StagedInsert, "_field_backend", get_backend)

    staged = StagedInsert(fake_table(class_name=class_name, table_name=table_name, store_name=declared_store, config=config))
    staged.rec["id"] = 7
    path, selected_backend = staged._resolve_field("data", ".dat")

    assert path.startswith(f"_schema/test_schema/{table_name}/id=7/")
    assert selected_backend is backend
    assert staged._staged_objects["data"]["store_name"] == expected_store
    get_backend.assert_called_with(expected_store)

    metadata = staged._compute_metadata("data")
    assert metadata["store"] == expected_store
    assert metadata["path"] == path


def test_default_store_change_does_not_repoint_staged_object(monkeypatch):
    config = FakeConfig(default="local")
    backend = MagicMock()
    backend.fs.isdir.return_value = False
    backend.size.return_value = 3
    get_backend = MagicMock(return_value=backend)
    monkeypatch.setattr(StagedInsert, "_field_backend", get_backend)

    staged = StagedInsert(fake_table(class_name="Recording", table_name="recording", store_name="", config=config))
    staged.rec["id"] = 8
    staged._resolve_field("data", ".dat")

    config.default = "archive"  # Repoint the default after staging the object.
    cached_path, _ = staged._resolve_field("data", ".dat")
    assert cached_path == staged._staged_objects["data"]["relative_path"]
    assert get_backend.call_args.args[0] == "local"
    metadata = staged._compute_metadata("data")
    assert metadata["store"] == "local"
    assert get_backend.call_args.args[0] == "local"

    # Cleanup must continue using the original store after the default changes.
    backend.fs.exists.return_value = False
    staged._cleanup()
    assert get_backend.call_args.args[0] == "local"
