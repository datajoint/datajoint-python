"""Tests for the StorageAdapter plugin system."""

import pytest

from datajoint.errors import DataJointError
from datajoint.storage_adapter import (
    StorageAdapter,
    _adapter_registry,
    _COMMON_STORE_KEYS,
    get_storage_adapter,
)


class _DummyAdapter(StorageAdapter):
    """Test adapter for registry tests."""

    protocol = "dummy"
    required_keys = ("protocol", "endpoint")
    allowed_keys = ("protocol", "endpoint", "token")

    def create_filesystem(self, spec):
        return None  # Not testing actual filesystem creation


class TestStorageAdapterRegistry:
    def setup_method(self):
        _adapter_registry["dummy"] = _DummyAdapter()

    def teardown_method(self):
        _adapter_registry.pop("dummy", None)

    def test_get_registered_adapter(self):
        adapter = get_storage_adapter("dummy")
        assert adapter is not None
        assert adapter.protocol == "dummy"

    def test_get_unknown_adapter_returns_none(self):
        adapter = get_storage_adapter("nonexistent_protocol_xyz")
        assert adapter is None

    def test_adapter_protocol_attribute(self):
        adapter = get_storage_adapter("dummy")
        assert isinstance(adapter.protocol, str)
        assert adapter.protocol == "dummy"


class TestStorageAdapterValidation:
    def setup_method(self):
        self.adapter = _DummyAdapter()

    def test_valid_spec_passes(self):
        spec = {"protocol": "dummy", "endpoint": "https://example.com"}
        self.adapter.validate_spec(spec)

    def test_missing_required_key_raises(self):
        spec = {"protocol": "dummy"}
        with pytest.raises(DataJointError, match="missing.*endpoint"):
            self.adapter.validate_spec(spec)

    def test_invalid_key_raises(self):
        spec = {"protocol": "dummy", "endpoint": "https://example.com", "bogus": "val"}
        with pytest.raises(DataJointError, match="Invalid.*bogus"):
            self.adapter.validate_spec(spec)

    def test_common_store_keys_always_allowed(self):
        spec = {
            "protocol": "dummy",
            "endpoint": "https://example.com",
            "hash_prefix": "_hash",
            "subfolding": None,
            "schema_prefix": "_schema",
        }
        self.adapter.validate_spec(spec)

    def test_common_store_keys_content(self):
        assert "hash_prefix" in _COMMON_STORE_KEYS
        assert "schema_prefix" in _COMMON_STORE_KEYS
        assert "subfolding" in _COMMON_STORE_KEYS
        assert "protocol" in _COMMON_STORE_KEYS
        assert "location" in _COMMON_STORE_KEYS


class TestStorageAdapterFullPath:
    def setup_method(self):
        self.adapter = _DummyAdapter()

    def test_full_path_with_location(self):
        spec = {"location": "data/blobs"}
        assert self.adapter.full_path(spec, "schema/ab/cd/hash") == "data/blobs/schema/ab/cd/hash"

    def test_full_path_empty_location(self):
        spec = {"location": ""}
        assert self.adapter.full_path(spec, "schema/ab/cd/hash") == "schema/ab/cd/hash"

    def test_full_path_no_location_key(self):
        spec = {}
        assert self.adapter.full_path(spec, "schema/ab/cd/hash") == "schema/ab/cd/hash"


class TestStorageAdapterGetUrl:
    def setup_method(self):
        self.adapter = _DummyAdapter()

    def test_default_url_format(self):
        assert self.adapter.get_url({}, "data/file.dat") == "dummy://data/file.dat"


import datajoint as dj


class TestGetStoreSpecPluginDelegation:
    """Tests for plugin protocol handling in Config.get_store_spec()."""

    def setup_method(self):
        import datajoint.storage_adapter as sa_mod

        sa_mod._adapter_registry["dummy"] = _DummyAdapter()
        self._original_stores = dj.config.stores.copy()

    def teardown_method(self):
        import datajoint.storage_adapter as sa_mod

        sa_mod._adapter_registry.pop("dummy", None)
        dj.config.stores = self._original_stores

    def test_plugin_protocol_accepted(self):
        """Plugin protocol passes validation via adapter."""
        dj.config.stores["test_store"] = {
            "protocol": "dummy",
            "endpoint": "https://example.com",
            "location": "",
            "hash_prefix": "_hash",
            "schema_prefix": "_schema",
        }
        spec = dj.config.get_store_spec("test_store")
        assert spec["protocol"] == "dummy"

    def test_unknown_protocol_error_message(self):
        """Unknown protocol gives clear error mentioning plugin installation."""
        dj.config.stores["bad_store"] = {
            "protocol": "nonexistent_xyz",
            "location": "",
        }
        with pytest.raises(DataJointError, match="Install a plugin"):
            dj.config.get_store_spec("bad_store")
