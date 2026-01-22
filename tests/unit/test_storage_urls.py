"""Unit tests for storage URL functions."""

import pytest

from datajoint.storage import (
    URL_PROTOCOLS,
    is_url,
    normalize_to_url,
    parse_url,
)


class TestURLProtocols:
    """Test URL protocol constants."""

    def test_url_protocols_includes_file(self):
        """URL_PROTOCOLS should include file://."""
        assert "file://" in URL_PROTOCOLS

    def test_url_protocols_includes_s3(self):
        """URL_PROTOCOLS should include s3://."""
        assert "s3://" in URL_PROTOCOLS

    def test_url_protocols_includes_cloud_providers(self):
        """URL_PROTOCOLS should include major cloud providers."""
        assert "gs://" in URL_PROTOCOLS
        assert "az://" in URL_PROTOCOLS


class TestIsUrl:
    """Test is_url function."""

    def test_s3_url(self):
        assert is_url("s3://bucket/key")

    def test_gs_url(self):
        assert is_url("gs://bucket/key")

    def test_file_url(self):
        assert is_url("file:///path/to/file")

    def test_http_url(self):
        assert is_url("http://example.com/file")

    def test_https_url(self):
        assert is_url("https://example.com/file")

    def test_local_path_not_url(self):
        assert not is_url("/path/to/file")

    def test_relative_path_not_url(self):
        assert not is_url("relative/path/file.dat")

    def test_case_insensitive(self):
        assert is_url("S3://bucket/key")
        assert is_url("FILE:///path")


class TestNormalizeToUrl:
    """Test normalize_to_url function."""

    def test_local_path_to_file_url(self):
        url = normalize_to_url("/data/file.dat")
        assert url.startswith("file://")
        assert "data/file.dat" in url

    def test_s3_url_unchanged(self):
        url = "s3://bucket/key/file.dat"
        assert normalize_to_url(url) == url

    def test_file_url_unchanged(self):
        url = "file:///data/file.dat"
        assert normalize_to_url(url) == url

    def test_relative_path_becomes_absolute(self):
        url = normalize_to_url("relative/path.dat")
        assert url.startswith("file://")
        # Should be absolute (contain full path)
        assert "/" in url[7:]  # After "file://"


class TestParseUrl:
    """Test parse_url function."""

    def test_parse_s3(self):
        protocol, path = parse_url("s3://bucket/key/file.dat")
        assert protocol == "s3"
        assert path == "bucket/key/file.dat"

    def test_parse_gs(self):
        protocol, path = parse_url("gs://bucket/key")
        assert protocol == "gcs"
        assert path == "bucket/key"

    def test_parse_gcs(self):
        protocol, path = parse_url("gcs://bucket/key")
        assert protocol == "gcs"
        assert path == "bucket/key"

    def test_parse_file(self):
        protocol, path = parse_url("file:///data/file.dat")
        assert protocol == "file"
        assert path == "/data/file.dat"

    def test_parse_http(self):
        protocol, path = parse_url("http://example.com/file")
        assert protocol == "http"
        assert path == "example.com/file"

    def test_parse_https(self):
        protocol, path = parse_url("https://example.com/file")
        assert protocol == "https"
        assert path == "example.com/file"

    def test_unsupported_protocol_raises(self):
        with pytest.raises(Exception, match="Unsupported URL protocol"):
            parse_url("ftp://example.com/file")

    def test_local_path_raises(self):
        with pytest.raises(Exception, match="Unsupported URL protocol"):
            parse_url("/local/path")
