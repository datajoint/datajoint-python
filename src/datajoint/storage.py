"""
Storage backend abstraction using fsspec for unified file operations.

This module provides a unified interface for storage operations across different
backends (local filesystem, S3, GCS, Azure, etc.) using the fsspec library.
"""

import logging
from io import BytesIO
from pathlib import Path, PurePosixPath
from typing import Any

import fsspec

from . import errors

logger = logging.getLogger(__name__.split(".")[0])


class StorageBackend:
    """
    Unified storage backend using fsspec.

    Provides a consistent interface for file operations across different storage
    backends including local filesystem and cloud object storage (S3, GCS, Azure).
    """

    def __init__(self, spec: dict[str, Any]):
        """
        Initialize storage backend from configuration spec.

        Args:
            spec: Storage configuration dictionary containing:
                - protocol: Storage protocol ('file', 's3', 'gcs', 'azure')
                - location: Base path or bucket prefix
                - bucket: Bucket name (for cloud storage)
                - endpoint: Endpoint URL (for S3-compatible storage)
                - access_key: Access key (for cloud storage)
                - secret_key: Secret key (for cloud storage)
                - secure: Use HTTPS (default: True for cloud)
                - Additional protocol-specific options
        """
        self.spec = spec
        self.protocol = spec.get("protocol", "file")
        self._fs = None
        self._validate_spec()

    def _validate_spec(self):
        """Validate configuration spec for the protocol."""
        if self.protocol == "file":
            location = self.spec.get("location")
            if location and not Path(location).is_dir():
                raise FileNotFoundError(f"Inaccessible local directory {location}")
        elif self.protocol == "s3":
            required = ["endpoint", "bucket", "access_key", "secret_key"]
            missing = [k for k in required if not self.spec.get(k)]
            if missing:
                raise errors.DataJointError(f"Missing S3 configuration: {', '.join(missing)}")

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """Get or create the fsspec filesystem instance."""
        if self._fs is None:
            self._fs = self._create_filesystem()
        return self._fs

    def _create_filesystem(self) -> fsspec.AbstractFileSystem:
        """Create fsspec filesystem based on protocol."""
        if self.protocol == "file":
            return fsspec.filesystem("file")

        elif self.protocol == "s3":
            # Build S3 configuration
            endpoint = self.spec["endpoint"]
            # Determine if endpoint includes protocol
            if not endpoint.startswith(("http://", "https://")):
                secure = self.spec.get("secure", False)
                endpoint_url = f"{'https' if secure else 'http'}://{endpoint}"
            else:
                endpoint_url = endpoint

            return fsspec.filesystem(
                "s3",
                key=self.spec["access_key"],
                secret=self.spec["secret_key"],
                client_kwargs={"endpoint_url": endpoint_url},
            )

        elif self.protocol == "gcs":
            return fsspec.filesystem(
                "gcs",
                token=self.spec.get("token"),
                project=self.spec.get("project"),
            )

        elif self.protocol == "azure":
            return fsspec.filesystem(
                "abfs",
                account_name=self.spec.get("account_name"),
                account_key=self.spec.get("account_key"),
                connection_string=self.spec.get("connection_string"),
            )

        else:
            raise errors.DataJointError(f"Unsupported storage protocol: {self.protocol}")

    def _full_path(self, path: str | PurePosixPath) -> str:
        """
        Construct full path including bucket for cloud storage.

        Args:
            path: Relative path within the storage location

        Returns:
            Full path suitable for fsspec operations
        """
        path = str(path)
        if self.protocol == "s3":
            bucket = self.spec["bucket"]
            return f"{bucket}/{path}"
        elif self.protocol in ("gcs", "azure"):
            bucket = self.spec.get("bucket") or self.spec.get("container")
            return f"{bucket}/{path}"
        else:
            # Local filesystem - path is already absolute or relative to cwd
            return path

    def put_file(self, local_path: str | Path, remote_path: str | PurePosixPath, metadata: dict | None = None):
        """
        Upload a file from local filesystem to storage.

        Args:
            local_path: Path to local file
            remote_path: Destination path in storage
            metadata: Optional metadata to attach to the file
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"put_file: {local_path} -> {self.protocol}:{full_path}")

        if self.protocol == "file":
            # For local filesystem, use safe copy with atomic rename
            from .utils import safe_copy
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)
            safe_copy(local_path, full_path, overwrite=True)
        else:
            # For cloud storage, use fsspec put
            self.fs.put_file(str(local_path), full_path)

    def get_file(self, remote_path: str | PurePosixPath, local_path: str | Path):
        """
        Download a file from storage to local filesystem.

        Args:
            remote_path: Path in storage
            local_path: Destination path on local filesystem
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"get_file: {self.protocol}:{full_path} -> {local_path}")

        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        if self.protocol == "file":
            from .utils import safe_copy
            safe_copy(full_path, local_path)
        else:
            self.fs.get_file(full_path, str(local_path))

    def put_buffer(self, buffer: bytes, remote_path: str | PurePosixPath):
        """
        Write bytes to storage.

        Args:
            buffer: Bytes to write
            remote_path: Destination path in storage
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"put_buffer: {len(buffer)} bytes -> {self.protocol}:{full_path}")

        if self.protocol == "file":
            from .utils import safe_write
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)
            safe_write(full_path, buffer)
        else:
            self.fs.pipe_file(full_path, buffer)

    def get_buffer(self, remote_path: str | PurePosixPath) -> bytes:
        """
        Read bytes from storage.

        Args:
            remote_path: Path in storage

        Returns:
            File contents as bytes
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"get_buffer: {self.protocol}:{full_path}")

        try:
            if self.protocol == "file":
                return Path(full_path).read_bytes()
            else:
                return self.fs.cat_file(full_path)
        except FileNotFoundError:
            raise errors.MissingExternalFile(f"Missing external file {full_path}") from None

    def exists(self, remote_path: str | PurePosixPath) -> bool:
        """
        Check if a file exists in storage.

        Args:
            remote_path: Path in storage

        Returns:
            True if file exists
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"exists: {self.protocol}:{full_path}")

        if self.protocol == "file":
            return Path(full_path).is_file()
        else:
            return self.fs.exists(full_path)

    def remove(self, remote_path: str | PurePosixPath):
        """
        Remove a file from storage.

        Args:
            remote_path: Path in storage
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"remove: {self.protocol}:{full_path}")

        try:
            if self.protocol == "file":
                Path(full_path).unlink(missing_ok=True)
            else:
                self.fs.rm(full_path)
        except FileNotFoundError:
            pass  # Already gone

    def size(self, remote_path: str | PurePosixPath) -> int:
        """
        Get file size in bytes.

        Args:
            remote_path: Path in storage

        Returns:
            File size in bytes
        """
        full_path = self._full_path(remote_path)

        if self.protocol == "file":
            return Path(full_path).stat().st_size
        else:
            return self.fs.size(full_path)

    def open(self, remote_path: str | PurePosixPath, mode: str = "rb"):
        """
        Open a file in storage.

        Args:
            remote_path: Path in storage
            mode: File mode ('rb', 'wb', etc.)

        Returns:
            File-like object
        """
        full_path = self._full_path(remote_path)
        return self.fs.open(full_path, mode)


def get_storage_backend(spec: dict[str, Any]) -> StorageBackend:
    """
    Factory function to create a storage backend from configuration.

    Args:
        spec: Storage configuration dictionary

    Returns:
        StorageBackend instance
    """
    return StorageBackend(spec)
