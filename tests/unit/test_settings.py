"""Tests for DataJoint settings module."""

from pathlib import Path

import pytest
from pydantic import SecretStr, ValidationError

import datajoint as dj
from datajoint import settings
from datajoint.errors import DataJointError
from datajoint.settings import (
    CONFIG_FILENAME,
    SECRETS_DIRNAME,
    find_config_file,
    find_secrets_dir,
    read_secret_file,
)


class TestConfigFileSearch:
    """Test recursive config file search."""

    def test_find_in_current_directory(self, tmp_path):
        """Config file in current directory is found."""
        config_file = tmp_path / CONFIG_FILENAME
        config_file.write_text("{}")

        found = find_config_file(tmp_path)
        assert found == config_file

    def test_find_in_parent_directory(self, tmp_path):
        """Config file in parent directory is found."""
        subdir = tmp_path / "src" / "pipeline"
        subdir.mkdir(parents=True)
        config_file = tmp_path / CONFIG_FILENAME
        config_file.write_text("{}")

        found = find_config_file(subdir)
        assert found == config_file

    def test_stop_at_git_boundary(self, tmp_path):
        """Search stops at .git directory."""
        (tmp_path / ".git").mkdir()
        subdir = tmp_path / "src"
        subdir.mkdir()
        # No config file - should return None, not search above .git

        found = find_config_file(subdir)
        assert found is None

    def test_stop_at_hg_boundary(self, tmp_path):
        """Search stops at .hg directory."""
        (tmp_path / ".hg").mkdir()
        subdir = tmp_path / "src"
        subdir.mkdir()

        found = find_config_file(subdir)
        assert found is None

    def test_config_found_before_git(self, tmp_path):
        """Config file found before reaching .git boundary."""
        (tmp_path / ".git").mkdir()
        config_file = tmp_path / CONFIG_FILENAME
        config_file.write_text("{}")
        subdir = tmp_path / "src"
        subdir.mkdir()

        found = find_config_file(subdir)
        assert found == config_file

    def test_returns_none_when_not_found(self, tmp_path):
        """Returns None when no config file exists."""
        (tmp_path / ".git").mkdir()  # Create boundary
        subdir = tmp_path / "src"
        subdir.mkdir()

        found = find_config_file(subdir)
        assert found is None


class TestSecretsDirectory:
    """Test secrets directory detection and loading."""

    def test_find_secrets_next_to_config(self, tmp_path):
        """Finds .secrets/ directory next to config file."""
        config_file = tmp_path / CONFIG_FILENAME
        config_file.write_text("{}")
        secrets_dir = tmp_path / SECRETS_DIRNAME
        secrets_dir.mkdir()

        found = find_secrets_dir(config_file)
        assert found == secrets_dir

    def test_no_secrets_dir_returns_none(self, tmp_path):
        """Returns None when no secrets directory exists."""
        config_file = tmp_path / CONFIG_FILENAME
        config_file.write_text("{}")

        found = find_secrets_dir(config_file)
        # May return system secrets dir if it exists, otherwise None
        if found is not None:
            assert found == settings.SYSTEM_SECRETS_DIR

    def test_read_secret_file(self, tmp_path):
        """Reads secret value from file."""
        (tmp_path / "database.password").write_text("my_secret\n")

        value = read_secret_file(tmp_path, "database.password")
        assert value == "my_secret"  # Strips whitespace

    def test_read_missing_secret_returns_none(self, tmp_path):
        """Returns None for missing secret file."""
        value = read_secret_file(tmp_path, "nonexistent")
        assert value is None

    def test_read_secret_from_none_dir(self):
        """Returns None when secrets_dir is None."""
        value = read_secret_file(None, "database.password")
        assert value is None


class TestSecretStr:
    """Test SecretStr handling for sensitive fields."""

    def test_password_is_secret_str(self):
        """Password field uses SecretStr type."""
        dj.config.database.password = "test_password"
        assert isinstance(dj.config.database.password, SecretStr)
        dj.config.database.password = None

    def test_secret_str_masked_in_repr(self):
        """SecretStr values are masked in repr."""
        dj.config.database.password = "super_secret"
        repr_str = repr(dj.config.database.password)
        assert "super_secret" not in repr_str
        assert "**" in repr_str
        dj.config.database.password = None

    def test_dict_access_unwraps_secret(self):
        """Dict-style access returns plain string for secrets."""
        dj.config.database.password = "unwrapped_secret"
        value = dj.config["database.password"]
        assert value == "unwrapped_secret"
        assert isinstance(value, str)
        assert not isinstance(value, SecretStr)
        dj.config.database.password = None

    def test_aws_secret_key_is_secret_str(self):
        """AWS secret key uses SecretStr type."""
        dj.config.external.aws_secret_access_key = "aws_secret"
        assert isinstance(dj.config.external.aws_secret_access_key, SecretStr)
        dj.config.external.aws_secret_access_key = None


class TestSettingsAccess:
    """Test accessing settings via different methods."""

    def test_attribute_access(self):
        """Test accessing settings via attributes."""
        # Host can be localhost or db (docker), just verify it's a string
        assert isinstance(dj.config.database.host, str)
        assert len(dj.config.database.host) > 0
        assert dj.config.database.port == 3306
        # safemode may be modified by conftest fixtures
        assert isinstance(dj.config.safemode, bool)

    def test_dict_style_access(self):
        """Test accessing settings via dict-style notation."""
        # Host can be localhost or db (docker), just verify it's a string
        assert isinstance(dj.config["database.host"], str)
        assert len(dj.config["database.host"]) > 0
        assert dj.config["database.port"] == 3306
        # safemode may be modified by conftest fixtures
        assert isinstance(dj.config["safemode"], bool)

    def test_get_with_default(self):
        """Test get() method with default values."""
        # Host can be localhost or db (docker), just verify it exists
        assert dj.config.get("database.host") is not None
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


class TestLoad:
    """Test loading configuration."""

    def test_load_config_file(self, tmp_path, monkeypatch):
        """Test loading configuration from file.

        Note: Environment variables take precedence over config file values.
        We need to clear DJ_HOST to test file loading.
        """
        filename = tmp_path / "test_config.json"
        filename.write_text('{"database": {"host": "loaded_host"}}')
        original_host = dj.config.database.host

        # Clear env var so file value takes effect
        monkeypatch.delenv("DJ_HOST", raising=False)

        try:
            dj.config.load(filename)
            assert dj.config.database.host == "loaded_host"
        finally:
            dj.config.database.host = original_host

    def test_env_var_overrides_config_file(self, tmp_path, monkeypatch):
        """Test that environment variables take precedence over config file.

        When DJ_HOST is set, loading a config file should NOT override the value.
        The env var value should be preserved.
        """
        filename = tmp_path / "test_config.json"
        filename.write_text('{"database": {"host": "file_host"}}')
        original_host = dj.config.database.host

        # Set env var - it should take precedence over file
        monkeypatch.setenv("DJ_HOST", "env_host")
        # Reset config to pick up new env var
        dj.config.database.host = "env_host"

        try:
            dj.config.load(filename)
            # File value should be skipped because DJ_HOST is set
            # The env var value should be preserved
            assert dj.config.database.host == "env_host"
        finally:
            dj.config.database.host = original_host

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


class TestSaveTemplate:
    """Test save_template method for creating configuration templates."""

    def test_save_minimal_template(self, tmp_path):
        """Test creating a minimal template."""
        config_path = tmp_path / "datajoint.json"
        result = dj.config.save_template(config_path, minimal=True, create_secrets_dir=False)

        assert result == config_path.absolute()
        assert config_path.exists()

        import json

        with open(config_path) as f:
            content = json.load(f)

        assert "database" in content
        assert content["database"]["host"] == "localhost"
        assert content["database"]["port"] == 3306
        # Minimal template should not have credentials
        assert "password" not in content["database"]
        assert "user" not in content["database"]

    def test_save_full_template(self, tmp_path):
        """Test creating a full template."""
        config_path = tmp_path / "datajoint.json"
        result = dj.config.save_template(config_path, minimal=False, create_secrets_dir=False)

        assert result == config_path.absolute()
        assert config_path.exists()

        import json

        with open(config_path) as f:
            content = json.load(f)

        # Full template should have all settings groups
        assert "database" in content
        assert "connection" in content
        assert "display" in content
        assert "object_storage" in content
        assert "stores" in content
        assert "loglevel" in content
        assert "safemode" in content
        # But still no credentials
        assert "password" not in content["database"]
        assert "user" not in content["database"]

    def test_save_template_creates_secrets_dir(self, tmp_path):
        """Test that save_template creates .secrets/ directory."""
        config_path = tmp_path / "datajoint.json"
        dj.config.save_template(config_path, create_secrets_dir=True)

        secrets_dir = tmp_path / SECRETS_DIRNAME
        assert secrets_dir.exists()
        assert secrets_dir.is_dir()

        # Check placeholder files created
        assert (secrets_dir / "database.user").exists()
        assert (secrets_dir / "database.password").exists()

        # Check .gitignore created
        gitignore = secrets_dir / ".gitignore"
        assert gitignore.exists()
        assert "*" in gitignore.read_text()

    def test_save_template_refuses_overwrite(self, tmp_path):
        """Test that save_template won't overwrite existing file."""
        config_path = tmp_path / "datajoint.json"
        config_path.write_text("{}")

        with pytest.raises(FileExistsError, match="already exists"):
            dj.config.save_template(config_path)

    def test_save_template_secrets_dir_idempotent(self, tmp_path):
        """Test that creating secrets dir doesn't overwrite existing secrets."""
        config_path = tmp_path / "datajoint.json"
        secrets_dir = tmp_path / SECRETS_DIRNAME
        secrets_dir.mkdir()

        # Pre-populate a secret
        password_file = secrets_dir / "database.password"
        password_file.write_text("existing_password")

        dj.config.save_template(config_path, create_secrets_dir=True)

        # Original password should be preserved
        assert password_file.read_text() == "existing_password"
