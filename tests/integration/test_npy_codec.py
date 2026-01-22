"""
Tests for the NpyCodec - schema-addressed numpy array storage.

These tests verify:
- NpyCodec encode/decode roundtrip
- NpyRef lazy loading behavior
- NpyRef metadata access without I/O
- NpyRef numpy integration via __array__
- Schema-addressed path construction
"""

import numpy as np
import pytest

import datajoint as dj
from datajoint.builtin_codecs import NpyCodec, NpyRef, SchemaCodec


# =============================================================================
# Test Schema Definition
# =============================================================================


class Recording(dj.Manual):
    definition = """
    recording_id : int
    ---
    waveform : <npy@repo-s3>
    """


class MultiArray(dj.Manual):
    definition = """
    item_id : int
    ---
    small_array : <npy@repo-s3>
    large_array : <npy@repo-s3>
    """


LOCALS_NPY = {"Recording": Recording, "MultiArray": MultiArray}


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def schema_name(prefix):
    return prefix + "_test_npy_codec"


@pytest.fixture
def schema_npy(connection_test, s3_creds, tmpdir, schema_name, mock_stores):
    """Create schema with NpyCodec tables."""
    # mock_stores fixture sets up object_storage.stores with repo-s3, etc.
    context = dict(LOCALS_NPY)
    schema = dj.Schema(schema_name, context=context, connection=connection_test)
    schema(Recording)
    schema(MultiArray)
    yield schema
    schema.drop()


# =============================================================================
# Unit Tests (no database required)
# =============================================================================


class TestNpyRefUnit:
    """Unit tests for NpyRef without database."""

    def test_npy_ref_metadata_access(self):
        """NpyRef should provide metadata without I/O."""
        # Mock metadata as would be stored in JSON
        metadata = {
            "path": "test/recording/recording_id=1/waveform.npy",
            "store": "default",
            "dtype": "float64",
            "shape": [1000, 32],
        }

        # Create NpyRef with mock backend
        class MockBackend:
            def get_buffer(self, path):
                raise AssertionError("Should not be called for metadata access")

        ref = NpyRef(metadata, MockBackend())

        # These should NOT trigger I/O
        assert ref.shape == (1000, 32)
        assert ref.dtype == np.dtype("float64")
        assert ref.ndim == 2
        assert ref.size == 32000
        assert ref.nbytes == 32000 * 8  # float64 = 8 bytes
        assert ref.path == "test/recording/recording_id=1/waveform.npy"
        assert ref.store == "default"
        assert ref.is_loaded is False

    def test_npy_ref_repr(self):
        """NpyRef repr should show shape, dtype, and load status."""
        metadata = {
            "path": "test.npy",
            "store": None,
            "dtype": "int32",
            "shape": [100],
        }

        class MockBackend:
            pass

        ref = NpyRef(metadata, MockBackend())
        repr_str = repr(ref)

        assert "NpyRef" in repr_str
        assert "(100,)" in repr_str
        assert "int32" in repr_str
        assert "not loaded" in repr_str

    def test_npy_ref_len(self):
        """NpyRef should support len() for first dimension."""
        metadata = {"path": "test.npy", "store": None, "dtype": "float32", "shape": [50, 10]}

        class MockBackend:
            pass

        ref = NpyRef(metadata, MockBackend())
        assert len(ref) == 50

    def test_npy_ref_len_0d_raises(self):
        """NpyRef len() should raise for 0-d arrays."""
        metadata = {"path": "test.npy", "store": None, "dtype": "float32", "shape": []}

        class MockBackend:
            pass

        ref = NpyRef(metadata, MockBackend())
        with pytest.raises(TypeError, match="0-dimensional"):
            len(ref)

    def test_npy_ref_mmap_local_filesystem(self, tmp_path):
        """NpyRef mmap_mode should work directly on local filesystem."""
        # Create a real .npy file
        test_array = np.arange(100, dtype=np.float64)
        npy_path = tmp_path / "test.npy"
        np.save(npy_path, test_array)

        metadata = {
            "path": "test.npy",
            "store": None,
            "dtype": "float64",
            "shape": [100],
        }

        # Mock backend that simulates local filesystem
        class MockFileBackend:
            protocol = "file"

            def _full_path(self, path):
                return str(tmp_path / path)

            def get_buffer(self, path):
                return (tmp_path / path).read_bytes()

        ref = NpyRef(metadata, MockFileBackend())

        # Load with mmap_mode
        mmap_arr = ref.load(mmap_mode="r")

        # Should be a memmap
        assert isinstance(mmap_arr, np.memmap)
        np.testing.assert_array_equal(mmap_arr, test_array)

        # Standard load should still work and cache
        regular_arr = ref.load()
        assert isinstance(regular_arr, np.ndarray)
        assert not isinstance(regular_arr, np.memmap)
        np.testing.assert_array_equal(regular_arr, test_array)

    def test_npy_ref_mmap_remote_storage(self, tmp_path):
        """NpyRef mmap_mode should download to cache for remote storage."""
        # Create test data
        test_array = np.array([1, 2, 3, 4, 5], dtype=np.int32)
        np.save(tmp_path / "temp.npy", test_array)
        npy_bytes = (tmp_path / "temp.npy").read_bytes()

        metadata = {
            "path": "remote/path/data.npy",
            "store": "s3-store",
            "dtype": "int32",
            "shape": [5],
        }

        # Mock backend that simulates remote storage
        class MockS3Backend:
            protocol = "s3"

            def get_buffer(self, path):
                return npy_bytes

        ref = NpyRef(metadata, MockS3Backend())

        # Load with mmap_mode - should download to cache
        mmap_arr = ref.load(mmap_mode="r")

        assert isinstance(mmap_arr, np.memmap)
        np.testing.assert_array_equal(mmap_arr, test_array)


class TestNpyCodecUnit:
    """Unit tests for NpyCodec without database."""

    def test_codec_is_schema_codec(self):
        """NpyCodec should inherit from SchemaCodec."""
        codec = NpyCodec()
        assert isinstance(codec, SchemaCodec)

    def test_codec_name(self):
        """NpyCodec should be registered as 'npy'."""
        codec = NpyCodec()
        assert codec.name == "npy"

    def test_codec_requires_store(self):
        """NpyCodec should require @ modifier."""
        codec = NpyCodec()

        # Should raise without @
        with pytest.raises(dj.DataJointError, match="requires @"):
            codec.get_dtype(is_store=False)

        # Should return json with @
        assert codec.get_dtype(is_store=True) == "json"

    def test_codec_validate_requires_ndarray(self):
        """NpyCodec should reject non-ndarray values."""
        codec = NpyCodec()

        # Should reject list
        with pytest.raises(dj.DataJointError, match="requires numpy.ndarray"):
            codec.validate([1, 2, 3])

        # Should reject dict
        with pytest.raises(dj.DataJointError, match="requires numpy.ndarray"):
            codec.validate({"data": [1, 2, 3]})

        # Should accept ndarray
        codec.validate(np.array([1, 2, 3]))  # No exception

    def test_codec_validate_rejects_object_dtype(self):
        """NpyCodec should reject object dtype arrays."""
        codec = NpyCodec()

        obj_array = np.array([{}, []], dtype=object)
        with pytest.raises(dj.DataJointError, match="object dtype"):
            codec.validate(obj_array)


# =============================================================================
# Integration Tests (require database + MinIO)
# =============================================================================


class TestNpyCodecIntegration:
    """Integration tests for NpyCodec with real storage."""

    def test_insert_fetch_roundtrip(self, schema_npy, minio_client):
        """Basic insert and fetch should preserve array data."""
        rec = Recording()
        rec.delete()

        # Insert array
        original = np.random.randn(100, 32).astype(np.float64)
        rec.insert1({"recording_id": 1, "waveform": original})

        # Fetch returns NpyRef
        result = rec.fetch1("waveform")
        assert isinstance(result, NpyRef)

        # Load and compare
        loaded = result.load()
        assert isinstance(loaded, np.ndarray)
        np.testing.assert_array_equal(loaded, original)

        rec.delete()

    def test_npy_ref_caching(self, schema_npy, minio_client):
        """NpyRef should cache loaded data."""
        rec = Recording()
        rec.delete()

        original = np.array([1, 2, 3, 4, 5])
        rec.insert1({"recording_id": 1, "waveform": original})

        ref = rec.fetch1("waveform")

        # First load
        arr1 = ref.load()
        assert ref.is_loaded is True

        # Second load should return same object (cached)
        arr2 = ref.load()
        assert arr1 is arr2

        rec.delete()

    def test_npy_ref_array_protocol(self, schema_npy, minio_client):
        """NpyRef should work transparently in numpy operations."""
        rec = Recording()
        rec.delete()

        original = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        rec.insert1({"recording_id": 1, "waveform": original})

        ref = rec.fetch1("waveform")

        # __array__ is triggered by numpy functions, not Python operators
        # Use np.asarray() or pass to numpy functions
        result = np.asarray(ref) + 1
        np.testing.assert_array_equal(result, original + 1)

        result = np.mean(ref)
        assert result == np.mean(original)

        result = np.asarray(ref)
        np.testing.assert_array_equal(result, original)

        # Also test that numpy ufuncs work
        result = np.add(ref, 1)
        np.testing.assert_array_equal(result, original + 1)

        rec.delete()

    def test_npy_ref_indexing(self, schema_npy, minio_client):
        """NpyRef should support indexing/slicing."""
        rec = Recording()
        rec.delete()

        original = np.arange(100).reshape(10, 10)
        rec.insert1({"recording_id": 1, "waveform": original})

        ref = rec.fetch1("waveform")

        # Indexing
        assert ref[0, 0] == 0
        assert ref[5, 5] == 55

        # Slicing
        np.testing.assert_array_equal(ref[0:2], original[0:2])
        np.testing.assert_array_equal(ref[:, 0], original[:, 0])

        rec.delete()

    def test_bulk_fetch_lazy(self, schema_npy, minio_client):
        """Fetching via to_dicts should return NpyRefs that are lazy."""
        rec = Recording()
        rec.delete()

        # Insert multiple arrays
        for i in range(5):
            rec.insert1({"recording_id": i, "waveform": np.random.randn(10, 10)})

        # Fetch all using to_dicts - should return NpyRefs
        results = rec.to_dicts()
        assert len(results) == 5

        refs = [r["waveform"] for r in results]
        for ref in refs:
            assert isinstance(ref, NpyRef)
            assert ref.is_loaded is False  # Not loaded yet

        # Access metadata without loading
        shapes = [ref.shape for ref in refs]
        assert all(s == (10, 10) for s in shapes)
        assert all(not ref.is_loaded for ref in refs)  # Still not loaded

        # Now load one
        refs[0].load()
        assert refs[0].is_loaded is True
        assert not refs[1].is_loaded  # Others still not loaded

        rec.delete()

    def test_different_dtypes(self, schema_npy, minio_client):
        """NpyCodec should handle various numpy dtypes."""
        rec = Recording()
        rec.delete()

        test_cases = [
            (1, np.array([1, 2, 3], dtype=np.int32)),
            (2, np.array([1.0, 2.0, 3.0], dtype=np.float32)),
            (3, np.array([1.0, 2.0, 3.0], dtype=np.float64)),
            (4, np.array([True, False, True], dtype=np.bool_)),
            (5, np.array([1 + 2j, 3 + 4j], dtype=np.complex128)),
        ]

        for rec_id, arr in test_cases:
            rec.insert1({"recording_id": rec_id, "waveform": arr})

        for rec_id, original in test_cases:
            ref = (rec & f"recording_id={rec_id}").fetch1("waveform")
            loaded = ref.load()
            assert loaded.dtype == original.dtype
            np.testing.assert_array_equal(loaded, original)

        rec.delete()

    def test_multidimensional_arrays(self, schema_npy, minio_client):
        """NpyCodec should handle various array shapes."""
        rec = Recording()
        rec.delete()

        test_cases = [
            (1, np.array([1, 2, 3])),  # 1D
            (2, np.array([[1, 2], [3, 4]])),  # 2D
            (3, np.random.randn(2, 3, 4)),  # 3D
            (4, np.random.randn(2, 3, 4, 5)),  # 4D
            (5, np.array(42)),  # 0D scalar
        ]

        for rec_id, arr in test_cases:
            rec.insert1({"recording_id": rec_id, "waveform": arr})

        for rec_id, original in test_cases:
            ref = (rec & f"recording_id={rec_id}").fetch1("waveform")
            assert ref.shape == original.shape
            assert ref.ndim == original.ndim
            loaded = ref.load()
            np.testing.assert_array_equal(loaded, original)

        rec.delete()

    def test_schema_addressed_path(self, schema_npy, minio_client):
        """NpyCodec should store files with .npy extension."""
        rec = Recording()
        rec.delete()

        rec.insert1({"recording_id": 42, "waveform": np.array([1, 2, 3])})

        ref = rec.fetch1("waveform")
        path = ref.path

        # Path should end with .npy extension
        assert path.endswith(".npy"), f"Path should end with .npy, got: {path}"

        # Verify the file can be loaded
        arr = ref.load()
        np.testing.assert_array_equal(arr, np.array([1, 2, 3]))

        rec.delete()


class TestNpyCodecEdgeCases:
    """Edge case tests for NpyCodec."""

    def test_empty_array(self, schema_npy, minio_client):
        """NpyCodec should handle empty arrays."""
        rec = Recording()
        rec.delete()

        empty = np.array([])
        rec.insert1({"recording_id": 1, "waveform": empty})

        ref = rec.fetch1("waveform")
        assert ref.shape == (0,)
        assert ref.size == 0

        loaded = ref.load()
        np.testing.assert_array_equal(loaded, empty)

        rec.delete()

    def test_large_array(self, schema_npy, minio_client):
        """NpyCodec should handle large arrays."""
        rec = Recording()
        rec.delete()

        # 10MB array
        large = np.random.randn(1000, 1000).astype(np.float64)
        rec.insert1({"recording_id": 1, "waveform": large})

        ref = rec.fetch1("waveform")
        assert ref.shape == (1000, 1000)
        assert ref.nbytes == 8_000_000

        loaded = ref.load()
        np.testing.assert_array_equal(loaded, large)

        rec.delete()

    def test_structured_array(self, schema_npy, minio_client):
        """NpyCodec should handle structured arrays."""
        rec = Recording()
        rec.delete()

        dt = np.dtype([("x", np.float64), ("y", np.float64), ("label", "U10")])
        structured = np.array([(1.0, 2.0, "a"), (3.0, 4.0, "b")], dtype=dt)

        rec.insert1({"recording_id": 1, "waveform": structured})

        ref = rec.fetch1("waveform")
        loaded = ref.load()

        assert loaded.dtype == structured.dtype
        np.testing.assert_array_equal(loaded, structured)

        rec.delete()
