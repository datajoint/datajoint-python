"""Tests for insert API improvements: validate(), chunk_size, insert_dataframe(), deprecation warnings."""

import warnings

import numpy as np
import pandas
import pytest

import datajoint as dj


class SimpleTable(dj.Manual):
    definition = """
    id : int32
    ---
    value : varchar(100)
    score=null : float64
    """


class AutoIncrementTable(dj.Manual):
    definition = """
    # auto_increment requires native int type
    id : int auto_increment
    ---
    value : varchar(100)
    """


@pytest.fixture
def schema_insert(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_insert_test",
        context=dict(SimpleTable=SimpleTable, AutoIncrementTable=AutoIncrementTable),
        connection=connection_test,
    )
    schema(SimpleTable)
    schema(AutoIncrementTable)
    yield schema
    schema.drop()


class TestValidate:
    """Tests for the validate() method."""

    def test_validate_valid_rows(self, schema_insert):
        """Test that valid rows pass validation."""
        table = SimpleTable()
        rows = [
            {"id": 1, "value": "one", "score": 1.0},
            {"id": 2, "value": "two", "score": 2.0},
        ]
        result = table.validate(rows)
        assert result.is_valid
        assert len(result.errors) == 0
        assert result.rows_checked == 2
        assert bool(result) is True

    def test_validate_missing_required_field(self, schema_insert):
        """Test that missing required fields are detected."""
        table = SimpleTable()
        rows = [{"value": "one"}]  # Missing 'id' which is PK
        result = table.validate(rows)
        assert not result.is_valid
        assert len(result.errors) > 0
        assert "id" in result.errors[0][2]  # Error message mentions 'id'

    def test_validate_unknown_field(self, schema_insert):
        """Test that unknown fields are detected."""
        table = SimpleTable()
        rows = [{"id": 1, "value": "one", "unknown_field": "test"}]
        result = table.validate(rows)
        assert not result.is_valid
        assert any("unknown_field" in err[2] for err in result.errors)

    def test_validate_ignore_extra_fields(self, schema_insert):
        """Test that ignore_extra_fields works."""
        table = SimpleTable()
        rows = [{"id": 1, "value": "one", "unknown_field": "test"}]
        result = table.validate(rows, ignore_extra_fields=True)
        assert result.is_valid

    def test_validate_wrong_tuple_length(self, schema_insert):
        """Test that wrong tuple length is detected."""
        table = SimpleTable()
        rows = [(1, "one")]  # Missing score
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = table.validate(rows)
        assert not result.is_valid
        assert "Incorrect number of attributes" in result.errors[0][2]

    def test_validate_nullable_field(self, schema_insert):
        """Test that nullable fields can be omitted."""
        table = SimpleTable()
        rows = [{"id": 1, "value": "one"}]  # score is nullable, can be omitted
        result = table.validate(rows)
        assert result.is_valid

    def test_validate_result_summary(self, schema_insert):
        """Test that summary() produces readable output."""
        table = SimpleTable()
        rows = [{"id": 1, "value": "one"}]
        result = table.validate(rows)
        summary = result.summary()
        assert "Validation passed" in summary

        rows = [{"value": "one"}]  # Missing id
        result = table.validate(rows)
        summary = result.summary()
        assert "Validation failed" in summary

    def test_validate_raise_if_invalid(self, schema_insert):
        """Test that raise_if_invalid() raises for invalid rows."""
        table = SimpleTable()
        rows = [{"value": "one"}]  # Missing id
        result = table.validate(rows)
        with pytest.raises(dj.DataJointError):
            result.raise_if_invalid()

    def test_validate_dataframe(self, schema_insert):
        """Test validating a DataFrame."""
        table = SimpleTable()
        df = pandas.DataFrame({"id": [1, 2], "value": ["one", "two"], "score": [1.0, 2.0]})
        result = table.validate(df)
        assert result.is_valid

    def test_validate_autoincrement_pk(self, schema_insert):
        """Test that autoincrement PK doesn't require value."""
        table = AutoIncrementTable()
        rows = [{"value": "one"}]  # id is auto_increment, can be omitted
        result = table.validate(rows)
        assert result.is_valid


class TestChunkedInsert:
    """Tests for chunk_size parameter in insert()."""

    def test_chunked_insert(self, schema_insert):
        """Test inserting with chunk_size."""
        table = SimpleTable()
        rows = [{"id": i, "value": f"val{i}", "score": float(i)} for i in range(100)]
        table.insert(rows, chunk_size=10)
        assert len(table) == 100

    def test_chunked_insert_single_chunk(self, schema_insert):
        """Test chunked insert where data fits in one chunk."""
        table = SimpleTable()
        rows = [{"id": i, "value": f"val{i}"} for i in range(5)]
        table.insert(rows, chunk_size=100)  # chunk_size larger than data
        assert len(table) == 5

    def test_chunked_insert_exact_chunks(self, schema_insert):
        """Test chunked insert where data divides evenly."""
        table = SimpleTable()
        rows = [{"id": i, "value": f"val{i}"} for i in range(20)]
        table.insert(rows, chunk_size=5)  # 4 chunks of 5
        assert len(table) == 20

    def test_chunked_insert_with_skip_duplicates(self, schema_insert):
        """Test chunked insert with skip_duplicates."""
        table = SimpleTable()
        rows = [{"id": i, "value": f"val{i}"} for i in range(10)]
        table.insert(rows)
        # Insert again with duplicates
        more_rows = [{"id": i, "value": f"val{i}"} for i in range(15)]
        table.insert(more_rows, chunk_size=5, skip_duplicates=True)
        assert len(table) == 15

    def test_chunked_insert_query_expression_error(self, schema_insert):
        """Test that chunk_size raises error for QueryExpression inserts."""
        table = SimpleTable()
        with pytest.raises(dj.DataJointError, match="chunk_size is not supported"):
            table.insert(table.proj(), chunk_size=10)


class TestInsertDataFrame:
    """Tests for insert_dataframe() method."""

    def test_insert_dataframe_basic(self, schema_insert):
        """Test basic DataFrame insert."""
        table = SimpleTable()
        df = pandas.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"], "score": [1.0, 2.0, 3.0]})
        table.insert_dataframe(df)
        assert len(table) == 3

    def test_insert_dataframe_index_as_pk_auto(self, schema_insert):
        """Test auto-detection of index as PK."""
        table = SimpleTable()
        # Create DataFrame with PK as index
        df = pandas.DataFrame({"value": ["a", "b"], "score": [1.0, 2.0]})
        df.index = pandas.Index([1, 2], name="id")
        table.insert_dataframe(df)  # Auto-detects index as PK
        assert len(table) == 2
        assert set(table.to_arrays("id")) == {1, 2}

    def test_insert_dataframe_index_as_pk_true(self, schema_insert):
        """Test explicit index_as_pk=True."""
        table = SimpleTable()
        df = pandas.DataFrame({"value": ["a", "b"], "score": [1.0, 2.0]})
        df.index = pandas.Index([1, 2], name="id")
        table.insert_dataframe(df, index_as_pk=True)
        assert len(table) == 2

    def test_insert_dataframe_index_as_pk_false(self, schema_insert):
        """Test explicit index_as_pk=False."""
        table = SimpleTable()
        df = pandas.DataFrame({"id": [1, 2], "value": ["a", "b"], "score": [1.0, 2.0]})
        df = df.set_index("id")  # Set id as index
        # With index_as_pk=False, index is dropped and we need id as column
        df = df.reset_index()  # Put id back as column
        table.insert_dataframe(df, index_as_pk=False)
        assert len(table) == 2

    def test_insert_dataframe_rangeindex_dropped(self, schema_insert):
        """Test that RangeIndex is automatically dropped."""
        table = SimpleTable()
        df = pandas.DataFrame({"id": [1, 2], "value": ["a", "b"], "score": [1.0, 2.0]})
        # df has default RangeIndex which should be dropped
        table.insert_dataframe(df)
        assert len(table) == 2

    def test_insert_dataframe_index_mismatch_error(self, schema_insert):
        """Test error when index doesn't match PK."""
        table = SimpleTable()
        df = pandas.DataFrame({"value": ["a", "b"], "score": [1.0, 2.0]})
        df.index = pandas.Index([1, 2], name="wrong_name")
        with pytest.raises(dj.DataJointError, match="do not match"):
            table.insert_dataframe(df, index_as_pk=True)

    def test_insert_dataframe_not_dataframe_error(self, schema_insert):
        """Test error when not a DataFrame."""
        table = SimpleTable()
        with pytest.raises(dj.DataJointError, match="requires a pandas DataFrame"):
            table.insert_dataframe([{"id": 1, "value": "a"}])

    def test_insert_dataframe_roundtrip(self, schema_insert):
        """Test roundtrip: to_pandas() -> modify -> insert_dataframe()."""
        table = SimpleTable()
        # Insert initial data
        table.insert([{"id": i, "value": f"val{i}", "score": float(i)} for i in range(3)])

        # Fetch as DataFrame
        df = table.to_pandas()

        # Clear table and re-insert
        with dj.config.override(safemode=False):
            table.delete()

        table.insert_dataframe(df)
        assert len(table) == 3

    def test_insert_dataframe_with_chunk_size(self, schema_insert):
        """Test insert_dataframe with chunk_size."""
        table = SimpleTable()
        df = pandas.DataFrame({"id": range(100), "value": [f"v{i}" for i in range(100)], "score": np.arange(100.0)})
        table.insert_dataframe(df, chunk_size=25)
        assert len(table) == 100


class TestDeprecationWarning:
    """Tests for positional insert deprecation warning."""

    def test_positional_insert_warning(self, schema_insert):
        """Test that positional inserts emit deprecation warning."""
        table = SimpleTable()
        with pytest.warns(DeprecationWarning, match="Positional inserts"):
            table.insert1((1, "value1", 1.0))

    def test_positional_insert_multiple_warning(self, schema_insert):
        """Test that positional inserts in insert() emit warning."""
        table = SimpleTable()
        with pytest.warns(DeprecationWarning, match="Positional inserts"):
            table.insert([(2, "value2", 2.0)])

    def test_dict_insert_no_warning(self, schema_insert):
        """Test that dict inserts don't emit warning."""
        table = SimpleTable()
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            # Should not raise DeprecationWarning
            table.insert1({"id": 3, "value": "value3", "score": 3.0})

    def test_numpy_record_no_warning(self, schema_insert):
        """Test that numpy record inserts don't emit warning."""
        table = SimpleTable()
        # Create numpy record
        dtype = [("id", int), ("value", "U100"), ("score", float)]
        record = np.array([(4, "value4", 4.0)], dtype=dtype)[0]
        with warnings.catch_warnings():
            warnings.simplefilter("error", DeprecationWarning)
            # Should not raise DeprecationWarning
            table.insert1(record)


class TestValidationResult:
    """Tests for ValidationResult class."""

    def test_validation_result_bool(self, schema_insert):
        """Test ValidationResult boolean behavior."""
        valid = dj.ValidationResult(is_valid=True, errors=[], rows_checked=1)
        invalid = dj.ValidationResult(is_valid=False, errors=[(0, "field", "error")], rows_checked=1)
        assert bool(valid) is True
        assert bool(invalid) is False

    def test_validation_result_summary_valid(self, schema_insert):
        """Test ValidationResult summary for valid result."""
        result = dj.ValidationResult(is_valid=True, errors=[], rows_checked=5)
        assert "Validation passed" in result.summary()
        assert "5 rows checked" in result.summary()

    def test_validation_result_summary_invalid(self, schema_insert):
        """Test ValidationResult summary for invalid result."""
        errors = [(0, "field1", "error1"), (1, "field2", "error2")]
        result = dj.ValidationResult(is_valid=False, errors=errors, rows_checked=2)
        summary = result.summary()
        assert "Validation failed" in summary
        assert "2 error(s)" in summary
        assert "Row 0" in summary
        assert "Row 1" in summary

    def test_validation_result_summary_truncated(self, schema_insert):
        """Test that summary truncates long error lists."""
        errors = [(i, f"field{i}", f"error{i}") for i in range(20)]
        result = dj.ValidationResult(is_valid=False, errors=errors, rows_checked=20)
        summary = result.summary()
        assert "and 10 more errors" in summary
