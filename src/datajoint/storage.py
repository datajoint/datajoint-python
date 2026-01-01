"""
Storage backend abstraction using fsspec for unified file operations.

This module provides a unified interface for storage operations across different
backends (local filesystem, S3, GCS, Azure, etc.) using the fsspec library.
"""

import json
import logging
import secrets
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

import fsspec

from . import errors

logger = logging.getLogger(__name__.split(".")[0])

# Characters safe for use in filenames and URLs
TOKEN_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"

# Supported remote URL protocols for copy insert
REMOTE_PROTOCOLS = ("s3://", "gs://", "gcs://", "az://", "abfs://", "http://", "https://")


def is_remote_url(path: str) -> bool:
    """
    Check if a path is a remote URL.

    Args:
        path: Path string to check

    Returns:
        True if path is a remote URL
    """
    if not isinstance(path, str):
        return False
    return path.lower().startswith(REMOTE_PROTOCOLS)


def parse_remote_url(url: str) -> tuple[str, str]:
    """
    Parse a remote URL into protocol and path.

    Args:
        url: Remote URL (e.g., 's3://bucket/path/file.dat')

    Returns:
        Tuple of (protocol, path) where protocol is fsspec-compatible
    """
    url_lower = url.lower()

    # Map URL schemes to fsspec protocols
    protocol_map = {
        "s3://": "s3",
        "gs://": "gcs",
        "gcs://": "gcs",
        "az://": "abfs",
        "abfs://": "abfs",
        "http://": "http",
        "https://": "https",
    }

    for prefix, protocol in protocol_map.items():
        if url_lower.startswith(prefix):
            path = url[len(prefix) :]
            return protocol, path

    raise errors.DataJointError(f"Unsupported remote URL protocol: {url}")


def generate_token(length: int = 8) -> str:
    """
    Generate a random token for filename collision avoidance.

    Args:
        length: Token length (4-16 characters, default 8)

    Returns:
        Random URL-safe string
    """
    length = max(4, min(16, length))
    return "".join(secrets.choice(TOKEN_ALPHABET) for _ in range(length))


def encode_pk_value(value: Any) -> str:
    """
    Encode a primary key value for use in storage paths.

    Args:
        value: Primary key value (int, str, date, etc.)

    Returns:
        Path-safe string representation
    """
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, datetime):
        # Use ISO format with safe separators
        return value.strftime("%Y-%m-%dT%H-%M-%S")
    if hasattr(value, "isoformat"):
        # Handle date objects
        return value.isoformat()

    # String handling
    s = str(value)
    # Check if path-safe (no special characters)
    unsafe_chars = '/\\:*?"<>|'
    if any(c in s for c in unsafe_chars) or len(s) > 100:
        # URL-encode unsafe strings or truncate long ones
        if len(s) > 100:
            # Truncate and add hash suffix for uniqueness
            import hashlib

            hash_suffix = hashlib.md5(s.encode()).hexdigest()[:8]
            s = s[:50] + "_" + hash_suffix
        return urllib.parse.quote(s, safe="")
    return s


def build_object_path(
    schema: str,
    table: str,
    field: str,
    primary_key: dict[str, Any],
    ext: str | None,
    partition_pattern: str | None = None,
    token_length: int = 8,
) -> tuple[str, str]:
    """
    Build the storage path for an object attribute.

    Args:
        schema: Schema name
        table: Table name
        field: Field/attribute name
        primary_key: Dict of primary key attribute names to values
        ext: File extension (e.g., ".dat") or None
        partition_pattern: Optional partition pattern with {attr} placeholders
        token_length: Length of random token suffix

    Returns:
        Tuple of (relative_path, token)
    """
    token = generate_token(token_length)

    # Build filename: field_token.ext
    filename = f"{field}_{token}"
    if ext:
        if not ext.startswith("."):
            ext = "." + ext
        filename += ext

    # Build primary key path components
    pk_parts = []
    partition_attrs = set()

    # Extract partition attributes if pattern specified
    if partition_pattern:
        import re

        partition_attrs = set(re.findall(r"\{(\w+)\}", partition_pattern))

    # Build partition prefix (attributes specified in partition pattern)
    partition_parts = []
    for attr in partition_attrs:
        if attr in primary_key:
            partition_parts.append(f"{attr}={encode_pk_value(primary_key[attr])}")

    # Build remaining PK path (attributes not in partition)
    for attr, value in primary_key.items():
        if attr not in partition_attrs:
            pk_parts.append(f"{attr}={encode_pk_value(value)}")

    # Construct full path
    # Pattern: {partition_attrs}/{schema}/{table}/objects/{remaining_pk}/{filename}
    parts = []
    if partition_parts:
        parts.extend(partition_parts)
    parts.append(schema)
    parts.append(table)
    parts.append("objects")
    if pk_parts:
        parts.extend(pk_parts)
    parts.append(filename)

    return "/".join(parts), token


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
        Construct full path including location/bucket prefix.

        Args:
            path: Relative path within the storage location

        Returns:
            Full path suitable for fsspec operations
        """
        path = str(path)
        if self.protocol == "s3":
            bucket = self.spec["bucket"]
            location = self.spec.get("location", "")
            if location:
                return f"{bucket}/{location}/{path}"
            return f"{bucket}/{path}"
        elif self.protocol in ("gcs", "azure"):
            bucket = self.spec.get("bucket") or self.spec.get("container")
            location = self.spec.get("location", "")
            if location:
                return f"{bucket}/{location}/{path}"
            return f"{bucket}/{path}"
        else:
            # Local filesystem - prepend location if specified
            location = self.spec.get("location", "")
            if location:
                return str(Path(location) / path)
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

        # For write modes on local filesystem, ensure parent directory exists
        if self.protocol == "file" and "w" in mode:
            Path(full_path).parent.mkdir(parents=True, exist_ok=True)

        return self.fs.open(full_path, mode)

    def put_folder(self, local_path: str | Path, remote_path: str | PurePosixPath) -> dict:
        """
        Upload a folder to storage.

        Args:
            local_path: Path to local folder
            remote_path: Destination path in storage

        Returns:
            Manifest dict with file list, total_size, and item_count
        """
        local_path = Path(local_path)
        if not local_path.is_dir():
            raise errors.DataJointError(f"Not a directory: {local_path}")

        full_path = self._full_path(remote_path)
        logger.debug(f"put_folder: {local_path} -> {self.protocol}:{full_path}")

        # Collect file info for manifest
        files = []
        total_size = 0

        for root, dirs, filenames in local_path.walk():
            for filename in filenames:
                file_path = root / filename
                rel_path = file_path.relative_to(local_path).as_posix()
                file_size = file_path.stat().st_size
                files.append({"path": rel_path, "size": file_size})
                total_size += file_size

        # Upload folder contents
        if self.protocol == "file":
            import shutil

            dest = Path(full_path)
            dest.mkdir(parents=True, exist_ok=True)
            for item in local_path.iterdir():
                if item.is_file():
                    shutil.copy2(item, dest / item.name)
                else:
                    shutil.copytree(item, dest / item.name, dirs_exist_ok=True)
        else:
            self.fs.put(str(local_path), full_path, recursive=True)

        # Build manifest
        manifest = {
            "files": files,
            "total_size": total_size,
            "item_count": len(files),
            "created": datetime.now(timezone.utc).isoformat(),
        }

        # Write manifest alongside folder
        manifest_path = f"{remote_path}.manifest.json"
        self.put_buffer(json.dumps(manifest, indent=2).encode(), manifest_path)

        return manifest

    def remove_folder(self, remote_path: str | PurePosixPath):
        """
        Remove a folder and its manifest from storage.

        Args:
            remote_path: Path to folder in storage
        """
        full_path = self._full_path(remote_path)
        logger.debug(f"remove_folder: {self.protocol}:{full_path}")

        try:
            if self.protocol == "file":
                import shutil

                shutil.rmtree(full_path, ignore_errors=True)
            else:
                self.fs.rm(full_path, recursive=True)
        except FileNotFoundError:
            pass

        # Also remove manifest
        manifest_path = f"{remote_path}.manifest.json"
        self.remove(manifest_path)

    def get_fsmap(self, remote_path: str | PurePosixPath) -> fsspec.FSMap:
        """
        Get an FSMap for a path (useful for Zarr/xarray).

        Args:
            remote_path: Path in storage

        Returns:
            fsspec.FSMap instance
        """
        full_path = self._full_path(remote_path)
        return fsspec.FSMap(full_path, self.fs)

    def copy_from_url(self, source_url: str, dest_path: str | PurePosixPath) -> int:
        """
        Copy a file from a remote URL to managed storage.

        Args:
            source_url: Remote URL (s3://, gs://, http://, etc.)
            dest_path: Destination path in managed storage

        Returns:
            Size of copied file in bytes
        """
        protocol, source_path = parse_remote_url(source_url)
        full_dest = self._full_path(dest_path)

        logger.debug(f"copy_from_url: {protocol}://{source_path} -> {self.protocol}:{full_dest}")

        # Get source filesystem
        source_fs = fsspec.filesystem(protocol)

        # Check if source is a directory
        if source_fs.isdir(source_path):
            return self._copy_folder_from_url(source_fs, source_path, dest_path)

        # Copy single file
        if self.protocol == "file":
            # Download to local destination
            Path(full_dest).parent.mkdir(parents=True, exist_ok=True)
            source_fs.get_file(source_path, full_dest)
            return Path(full_dest).stat().st_size
        else:
            # Remote-to-remote copy via streaming
            with source_fs.open(source_path, "rb") as src:
                content = src.read()
            self.fs.pipe_file(full_dest, content)
            return len(content)

    def _copy_folder_from_url(
        self, source_fs: fsspec.AbstractFileSystem, source_path: str, dest_path: str | PurePosixPath
    ) -> dict:
        """
        Copy a folder from a remote URL to managed storage.

        Args:
            source_fs: Source filesystem
            source_path: Path in source filesystem
            dest_path: Destination path in managed storage

        Returns:
            Manifest dict with file list, total_size, and item_count
        """
        full_dest = self._full_path(dest_path)
        logger.debug(f"copy_folder_from_url: {source_path} -> {self.protocol}:{full_dest}")

        # Collect file info for manifest
        files = []
        total_size = 0

        # Walk source directory
        for root, dirs, filenames in source_fs.walk(source_path):
            for filename in filenames:
                src_file = f"{root}/{filename}" if root != source_path else f"{source_path}/{filename}"
                rel_path = src_file[len(source_path) :].lstrip("/")
                file_size = source_fs.size(src_file)
                files.append({"path": rel_path, "size": file_size})
                total_size += file_size

                # Copy file
                dest_file = f"{full_dest}/{rel_path}"
                if self.protocol == "file":
                    Path(dest_file).parent.mkdir(parents=True, exist_ok=True)
                    source_fs.get_file(src_file, dest_file)
                else:
                    with source_fs.open(src_file, "rb") as src:
                        content = src.read()
                    self.fs.pipe_file(dest_file, content)

        # Build manifest
        manifest = {
            "files": files,
            "total_size": total_size,
            "item_count": len(files),
            "created": datetime.now(timezone.utc).isoformat(),
        }

        # Write manifest alongside folder
        manifest_path = f"{dest_path}.manifest.json"
        self.put_buffer(json.dumps(manifest, indent=2).encode(), manifest_path)

        return manifest

    def source_is_directory(self, source: str) -> bool:
        """
        Check if a source path (local or remote URL) is a directory.

        Args:
            source: Local path or remote URL

        Returns:
            True if source is a directory
        """
        if is_remote_url(source):
            protocol, path = parse_remote_url(source)
            source_fs = fsspec.filesystem(protocol)
            return source_fs.isdir(path)
        else:
            return Path(source).is_dir()

    def source_exists(self, source: str) -> bool:
        """
        Check if a source path (local or remote URL) exists.

        Args:
            source: Local path or remote URL

        Returns:
            True if source exists
        """
        if is_remote_url(source):
            protocol, path = parse_remote_url(source)
            source_fs = fsspec.filesystem(protocol)
            return source_fs.exists(path)
        else:
            return Path(source).exists()

    def get_source_size(self, source: str) -> int | None:
        """
        Get the size of a source file (local or remote URL).

        Args:
            source: Local path or remote URL

        Returns:
            Size in bytes, or None if directory or cannot determine
        """
        try:
            if is_remote_url(source):
                protocol, path = parse_remote_url(source)
                source_fs = fsspec.filesystem(protocol)
                if source_fs.isdir(path):
                    return None
                return source_fs.size(path)
            else:
                p = Path(source)
                if p.is_dir():
                    return None
                return p.stat().st_size
        except Exception:
            return None


STORE_METADATA_FILENAME = "datajoint_store.json"


def get_storage_backend(spec: dict[str, Any]) -> StorageBackend:
    """
    Factory function to create a storage backend from configuration.

    Args:
        spec: Storage configuration dictionary

    Returns:
        StorageBackend instance
    """
    return StorageBackend(spec)


def verify_or_create_store_metadata(backend: StorageBackend, spec: dict[str, Any]) -> dict:
    """
    Verify or create the store metadata file at the storage root.

    On first use, creates the datajoint_store.json file with project info.
    On subsequent uses, verifies the project_name matches.

    Args:
        backend: StorageBackend instance
        spec: Object storage configuration spec

    Returns:
        Store metadata dict

    Raises:
        DataJointError: If project_name mismatch detected
    """
    from .version import __version__ as dj_version

    project_name = spec.get("project_name")
    location = spec.get("location", "")

    # Metadata file path at storage root
    metadata_path = f"{location}/{STORE_METADATA_FILENAME}" if location else STORE_METADATA_FILENAME

    try:
        # Try to read existing metadata
        if backend.exists(metadata_path):
            metadata_content = backend.get_buffer(metadata_path)
            metadata = json.loads(metadata_content)

            # Verify project_name matches
            store_project = metadata.get("project_name")
            if store_project and store_project != project_name:
                raise errors.DataJointError(
                    f"Object store project name mismatch.\n"
                    f'  Client configured: "{project_name}"\n'
                    f'  Store metadata: "{store_project}"\n'
                    f"Ensure all clients use the same object_storage.project_name setting."
                )

            return metadata
        else:
            # Create new metadata
            metadata = {
                "project_name": project_name,
                "created": datetime.now(timezone.utc).isoformat(),
                "format_version": "1.0",
                "datajoint_version": dj_version,
            }

            # Optional database info - not enforced, just informational
            # These would need to be passed in from the connection context
            # For now, omit them

            backend.put_buffer(json.dumps(metadata, indent=2).encode(), metadata_path)
            return metadata

    except errors.DataJointError:
        raise
    except Exception as e:
        # Log warning but don't fail - metadata is informational
        logger.warning(f"Could not verify/create store metadata: {e}")
        return {"project_name": project_name}
