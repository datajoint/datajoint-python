"""Tests for DataJoint settings module."""

import os
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

import datajoint as dj
from datajoint import settings
from datajoint.errors import DataJointError


class TestSettingsAccess:
    """Test accessing settings via different methods."""

    def test_attribute_access(self):
        """Test accessing settings via attributes."""
        assert dj.config.database.host == "localhost"
        assert dj.config.database.port == 3306
        assert dj.config.safemode is True

    def test_dict_style_access(self):
        """Test accessing settings via dict-style notation."""
        assert dj.config["database.host"] == "localhost"
        assert dj.config["database.port"] == 3306
        assert dj.config["safemode"] is True

    def test_get_with_default(self):
        """Test get() method with default values."""
        assert dj.config.get("database.host") == "localhost"
        assert dj.config.get("nonexistent.key", "default") == "default"
        assert dj.config.get("nonexistent.key") is None


class TestSettingsModification:
    """Test modifying settings."""

    def test_attribute_assignment(self):
        """Test setting values via attribute assignment."""
        original = dj.config.database.host
        try:
            dj.config.database.host = "testhost"
            assert dj.config.database.host == "testhost"
        finally:
            dj.config.database.host = original

    def test_dict_style_assignment(self):
        """Test setting values via dict-style notation."""
        original = dj.config["database.host"]
        try:
            dj.config["database.host"] = "testhost2"
            assert dj.config["database.host"] == "testhost2"
        finally:
            dj.config["database.host"] = original

    def test_nested_assignment(self):
        """Test setting nested values."""
        original = dj.config.display.limit
        try:
            dj.config.display.limit = 25
            assert dj.config.display.limit == 25
            assert dj.config["display.limit"] == 25
        finally:
            dj.config.display.limit = original


class TestTypeValidation:
    """Test pydantic type validation."""

    def test_port_must_be_integer(self):
        """Test that port must be an integer."""
        with pytest.raises(ValidationError):
            dj.config.database.port = "not_an_integer"

    def test_loglevel_validation(self):
        """Test that loglevel must be a valid level."""
        with pytest.raises(ValidationError):
            dj.config.loglevel = "INVALID_LEVEL"

    def test_fetch_format_validation(self):
        """Test that fetch_format must be array or frame."""
        with pytest.raises(ValidationError):
            dj.config.fetch_format = "invalid"

    def test_valid_loglevel_values(self):
        """Test setting valid log levels."""
        original = dj.config.loglevel
        try:
            for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
                dj.config.loglevel = level
                assert dj.config.loglevel == level
        finally:
            dj.config.loglevel = original


class TestContextManager:
    """Test the override context manager."""

    def test_override_simple_value(self):
        """Test overriding a simple value."""
        original = dj.config.safemode
        with dj.config.override(safemode=False):
            assert dj.config.safemode is False
        assert dj.config.safemode == original

    def test_override_nested_value(self):
        """Test overriding nested values with double underscore."""
        original = dj.config.database.host
        with dj.config.override(database__host="override_host"):
            assert dj.config.database.host == "override_host"
        assert dj.config.database.host == original

    def test_override_multiple_values(self):
        """Test overriding multiple values at once."""
        orig_safe = dj.config.safemode
        orig_host = dj.config.database.host
        with dj.config.override(safemode=False, database__host="multi_test"):
            assert dj.config.safemode is False
            assert dj.config.database.host == "multi_test"
        assert dj.config.safemode == orig_safe
        assert dj.config.database.host == orig_host

    def test_override_restores_on_exception(self):
        """Test that override restores values even when exception occurs."""
        original = dj.config.safemode
        try:
            with dj.config.override(safemode=False):
                assert dj.config.safemode is False
                raise ValueError("test exception")
        except ValueError:
            pass
        assert dj.config.safemode == original


class TestSaveLoad:
    """Test saving and loading configuration."""

    def test_save_and_load(self):
        """Test saving and loading configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            filename = f.name

        try:
            # Modify and save
            original_host = dj.config.database.host
            dj.config.database.host = "saved_host"
            dj.config.save(filename)

            # Reset and load
            dj.config.database.host = "reset_host"
            dj.config.load(filename)

            assert dj.config.database.host == "saved_host"
        finally:
            dj.config.database.host = original_host
            os.unlink(filename)

    def test_save_local(self):
        """Test save_local creates local config file."""
        backup_path = None
        if os.path.exists(settings.LOCALCONFIG):
            backup_path = settings.LOCALCONFIG + ".backup"
            os.rename(settings.LOCALCONFIG, backup_path)

        try:
            dj.config.save_local()
            assert os.path.exists(settings.LOCALCONFIG)
        finally:
            if os.path.exists(settings.LOCALCONFIG):
                os.remove(settings.LOCALCONFIG)
            if backup_path and os.path.exists(backup_path):
                os.rename(backup_path, settings.LOCALCONFIG)

    def test_load_nonexistent_file(self):
        """Test loading nonexistent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            dj.config.load("/nonexistent/path/config.json")


class TestStoreSpec:
    """Test external store configuration."""

    def test_get_store_spec_not_configured(self):
        """Test getting unconfigured store raises error."""
        with pytest.raises(DataJointError, match="not configured"):
            dj.config.get_store_spec("nonexistent_store")

    def test_get_store_spec_file_protocol(self):
        """Test file protocol store spec validation."""
        original_stores = dj.config.stores.copy()
        try:
            dj.config.stores["test_file"] = {
                "protocol": "file",
                "location": "/tmp/test",
            }
            spec = dj.config.get_store_spec("test_file")
            assert spec["protocol"] == "file"
            assert spec["location"] == "/tmp/test"
            assert spec["subfolding"] == settings.DEFAULT_SUBFOLDING
        finally:
            dj.config.stores = original_stores

    def test_get_store_spec_missing_required(self):
        """Test missing required keys raises error."""
        original_stores = dj.config.stores.copy()
        try:
            dj.config.stores["bad_store"] = {
                "protocol": "file",
                # missing location
            }
            with pytest.raises(DataJointError, match="missing"):
                dj.config.get_store_spec("bad_store")
        finally:
            dj.config.stores = original_stores

    def test_get_store_spec_invalid_key(self):
        """Test invalid keys in store spec raises error."""
        original_stores = dj.config.stores.copy()
        try:
            dj.config.stores["bad_store"] = {
                "protocol": "file",
                "location": "/tmp/test",
                "invalid_key": "value",
            }
            with pytest.raises(DataJointError, match="Invalid"):
                dj.config.get_store_spec("bad_store")
        finally:
            dj.config.stores = original_stores


class TestDisplaySettings:
    """Test display-related settings."""

    def test_display_limit(self):
        """Test display limit setting."""
        original = dj.config.display.limit
        try:
            dj.config.display.limit = 50
            assert dj.config.display.limit == 50
        finally:
            dj.config.display.limit = original

    def test_display_width(self):
        """Test display width setting."""
        original = dj.config.display.width
        try:
            dj.config.display.width = 20
            assert dj.config.display.width == 20
        finally:
            dj.config.display.width = original


class TestCachePaths:
    """Test cache path settings."""

    def test_cache_path_string(self):
        """Test setting cache path as string."""
        original = dj.config.cache
        try:
            dj.config.cache = "/tmp/cache"
            assert dj.config.cache == Path("/tmp/cache")
        finally:
            dj.config.cache = original

    def test_cache_path_none(self):
        """Test cache path can be None."""
        original = dj.config.cache
        try:
            dj.config.cache = None
            assert dj.config.cache is None
        finally:
            dj.config.cache = original

    def test_query_cache_path(self):
        """Test query cache path setting."""
        original = dj.config.query_cache
        try:
            dj.config.query_cache = "/tmp/query_cache"
            assert dj.config.query_cache == Path("/tmp/query_cache")
        finally:
            dj.config.query_cache = original
