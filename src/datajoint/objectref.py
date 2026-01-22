"""
ObjectRef class for handling fetched object type attributes.

This module provides the ObjectRef class which represents a reference to a file
or folder stored in the pipeline's object storage backend. It provides metadata
access and direct fsspec-based file operations.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Iterator

import fsspec

from .errors import DataJointError
from .hash_registry import compute_hash
from .storage import StorageBackend


class IntegrityError(DataJointError):
    """Raised when object integrity verification fails."""

    pass


@dataclass
class ObjectRef:
    """
    Handle to a file or folder stored in the pipeline's object storage backend.

    This class is returned when fetching object-type attributes. It provides
    metadata access without I/O, and methods for reading content directly
    from the storage backend.

    Attributes:
        path: Relative path within the store (includes token)
        url: Full URI to the object (e.g., 's3://bucket/path/to/object.dat')
        store: Store name (None for default store)
        size: Total size in bytes (sum for folders), or None if not computed.
            For large hierarchical data like Zarr stores, size computation can
            be expensive and is optional.
        hash: Content hash with algorithm prefix, or None if not computed
        ext: File extension as tooling hint (e.g., ".dat", ".zarr") or None.
            This is a conventional suffix for tooling, not a content-type declaration.
        is_dir: True if stored content is a directory/key-prefix (e.g., Zarr store)
        timestamp: ISO 8601 upload timestamp
        mime_type: MIME type (files only, auto-detected from extension)
        item_count: Number of files (folders only), or None if not computed
    """

    path: str
    size: int | None
    hash: str | None
    ext: str | None
    is_dir: bool
    timestamp: datetime
    url: str | None = None
    store: str | None = None
    mime_type: str | None = None
    item_count: int | None = None
    _backend: StorageBackend | None = None

    @classmethod
    def from_json(cls, json_data: dict | str, backend: StorageBackend | None = None) -> "ObjectRef":
        """
        Create an ObjectRef from JSON metadata stored in the database.

        Parameters
        ----------
        json_data : dict or str
            JSON string or dict containing object metadata.
        backend : StorageBackend, optional
            StorageBackend instance for file operations.

        Returns
        -------
        ObjectRef
            ObjectRef instance.
        """
        if isinstance(json_data, str):
            data = json.loads(json_data)
        else:
            data = json_data

        timestamp = data.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))

        return cls(
            path=data["path"],
            url=data.get("url"),
            store=data.get("store"),
            size=data["size"],
            hash=data.get("hash"),
            ext=data.get("ext"),
            is_dir=data.get("is_dir", False),
            timestamp=timestamp,
            mime_type=data.get("mime_type"),
            item_count=data.get("item_count"),
            _backend=backend,
        )

    def to_json(self) -> dict:
        """
        Convert ObjectRef to JSON-serializable dict for database storage.

        Returns
        -------
        dict
            Dict suitable for JSON serialization.
        """
        data = {
            "path": self.path,
            "size": self.size,
            "hash": self.hash,
            "ext": self.ext,
            "is_dir": self.is_dir,
            "timestamp": self.timestamp.isoformat() if self.timestamp else None,
        }
        if self.url:
            data["url"] = self.url
        if self.store:
            data["store"] = self.store
        if self.mime_type:
            data["mime_type"] = self.mime_type
        if self.item_count is not None:
            data["item_count"] = self.item_count
        return data

    def _ensure_backend(self):
        """Ensure storage backend is available for I/O operations."""
        if self._backend is None:
            raise DataJointError(
                "ObjectRef has no storage backend configured. "
                "This usually means the object was created without a connection context."
            )

    @property
    def fs(self) -> fsspec.AbstractFileSystem:
        """
        Return fsspec filesystem for direct access.

        This allows integration with libraries like Zarr and xarray that
        work with fsspec filesystems.
        """
        self._ensure_backend()
        return self._backend.fs

    @property
    def fsmap(self) -> fsspec.FSMap:
        """
        Return FSMap suitable for Zarr/xarray.

        This provides a dict-like interface to the storage location,
        compatible with zarr.open() and xarray.open_zarr().

        Example:
            >>> z = zarr.open(obj_ref.fsmap, mode='r')
        """
        self._ensure_backend()
        full_path = self._backend._full_path(self.path)
        return fsspec.FSMap(full_path, self._backend.fs)

    @property
    def full_path(self) -> str:
        """
        Return full URI (e.g., 's3://bucket/path').

        This is the complete path including protocol and bucket/location.
        """
        self._ensure_backend()
        protocol = self._backend.protocol
        if protocol == "file":
            return str(Path(self._backend.spec.get("location", "")) / self.path)
        elif protocol == "s3":
            bucket = self._backend.spec["bucket"]
            return f"s3://{bucket}/{self.path}"
        elif protocol == "gcs":
            bucket = self._backend.spec["bucket"]
            return f"gs://{bucket}/{self.path}"
        elif protocol == "azure":
            container = self._backend.spec["container"]
            return f"az://{container}/{self.path}"
        else:
            return self.path

    def read(self) -> bytes:
        """
        Read entire file content as bytes.

        Returns
        -------
        bytes
            File contents as bytes.

        Raises
        ------
        DataJointError
            If object is a directory.
        """
        if self.is_dir:
            raise DataJointError("Cannot read() a directory. Use listdir() or walk() instead.")
        self._ensure_backend()
        return self._backend.get_buffer(self.path)

    def open(self, subpath: str | None = None, mode: str = "rb") -> IO:
        """
        Open file for reading.

        Parameters
        ----------
        subpath : str, optional
            Path within directory (for folder objects).
        mode : str, optional
            File mode ('rb' for binary read, 'r' for text). Default 'rb'.

        Returns
        -------
        IO
            File-like object.
        """
        self._ensure_backend()
        path = self.path
        if subpath:
            if not self.is_dir:
                raise DataJointError("Cannot use subpath on a file object")
            path = f"{self.path}/{subpath}"
        return self._backend.open(path, mode)

    def listdir(self, subpath: str = "") -> list[str]:
        """
        List contents of directory.

        Parameters
        ----------
        subpath : str, optional
            Subdirectory path. Default empty string (root).

        Returns
        -------
        list[str]
            List of filenames/directory names.
        """
        if not self.is_dir:
            raise DataJointError("Cannot listdir() on a file. Use read() or open() instead.")
        self._ensure_backend()
        path = f"{self.path}/{subpath}" if subpath else self.path
        full_path = self._backend._full_path(path)
        entries = self._backend.fs.ls(full_path, detail=False)
        # Return just the basename of each entry
        return [e.split("/")[-1] for e in entries]

    def walk(self) -> Iterator[tuple[str, list[str], list[str]]]:
        """
        Walk directory tree, similar to os.walk().

        Yields
        ------
        tuple[str, list[str], list[str]]
            Tuples of (dirpath, dirnames, filenames).
        """
        if not self.is_dir:
            raise DataJointError("Cannot walk() on a file.")
        self._ensure_backend()
        full_path = self._backend._full_path(self.path)
        for root, dirs, files in self._backend.fs.walk(full_path):
            # Make paths relative to the object root
            rel_root = root[len(full_path) :].lstrip("/")
            yield rel_root, dirs, files

    def download(self, destination: Path | str, subpath: str | None = None) -> Path:
        """
        Download object to local filesystem.

        Parameters
        ----------
        destination : Path or str
            Local directory or file path.
        subpath : str, optional
            Path within directory (for folder objects).

        Returns
        -------
        Path
            Path to downloaded file/directory.
        """
        self._ensure_backend()
        destination = Path(destination)

        if subpath:
            if not self.is_dir:
                raise DataJointError("Cannot use subpath on a file object")
            remote_path = f"{self.path}/{subpath}"
        else:
            remote_path = self.path

        if self.is_dir and not subpath:
            # Download entire directory
            destination.mkdir(parents=True, exist_ok=True)
            full_path = self._backend._full_path(remote_path)
            self._backend.fs.get(full_path, str(destination), recursive=True)
        else:
            # Download single file
            if destination.is_dir():
                filename = remote_path.split("/")[-1]
                destination = destination / filename
            destination.parent.mkdir(parents=True, exist_ok=True)
            self._backend.get_file(remote_path, destination)

        return destination

    def exists(self, subpath: str | None = None) -> bool:
        """
        Check if object (or subpath within it) exists.

        Parameters
        ----------
        subpath : str, optional
            Path within directory.

        Returns
        -------
        bool
            True if exists.
        """
        self._ensure_backend()
        path = f"{self.path}/{subpath}" if subpath else self.path
        return self._backend.exists(path)

    def verify(self) -> bool:
        """
        Verify object integrity.

        For files: checks size matches, and hash if available.
        For folders: validates manifest (all files exist with correct sizes).

        Returns
        -------
        bool
            True if valid.

        Raises
        ------
        IntegrityError
            If verification fails with details.
        """
        self._ensure_backend()

        if self.is_dir:
            return self._verify_folder()
        else:
            return self._verify_file()

    def _verify_file(self) -> bool:
        """Verify a single file."""
        # Check existence
        if not self._backend.exists(self.path):
            raise IntegrityError(f"File does not exist: {self.path}")

        # Check size if available
        if self.size is not None:
            actual_size = self._backend.size(self.path)
            if actual_size != self.size:
                raise IntegrityError(f"Size mismatch for {self.path}: expected {self.size}, got {actual_size}")

        # Check hash if available
        if self.hash:
            content = self._backend.get_buffer(self.path)
            actual_hash = compute_hash(content)
            if actual_hash != self.hash:
                raise IntegrityError(f"Hash mismatch for {self.path}: expected {self.hash}, got {actual_hash}")

        return True

    def _verify_folder(self) -> bool:
        """Verify a folder using its manifest."""
        manifest_path = f"{self.path}.manifest.json"

        if not self._backend.exists(manifest_path):
            raise IntegrityError(f"Manifest file missing: {manifest_path}")

        # Read manifest
        manifest_data = self._backend.get_buffer(manifest_path)
        manifest = json.loads(manifest_data)

        # Verify each file in manifest
        errors = []
        for file_info in manifest.get("files", []):
            file_path = f"{self.path}/{file_info['path']}"
            expected_size = file_info["size"]

            if not self._backend.exists(file_path):
                errors.append(f"Missing file: {file_info['path']}")
            else:
                actual_size = self._backend.size(file_path)
                if actual_size != expected_size:
                    errors.append(f"Size mismatch for {file_info['path']}: expected {expected_size}, got {actual_size}")

        if errors:
            raise IntegrityError("Folder verification failed:\n" + "\n".join(errors))

        return True

    def __repr__(self) -> str:
        type_str = "folder" if self.is_dir else "file"
        return f"ObjectRef({type_str}: {self.path}, size={self.size})"

    def __str__(self) -> str:
        return self.path
