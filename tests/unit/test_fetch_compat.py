"""Tests for backward-compatible fetch() method."""

import warnings
from unittest.mock import MagicMock, patch

import numpy as np
import pytest


class TestFetchBackwardCompat:
    """Test backward-compatible fetch() emits deprecation warning and delegates correctly."""

    @pytest.fixture
    def mock_expression(self):
        """Create a mock QueryExpression with mocked output methods."""
        from datajoint.expression import QueryExpression

        expr = MagicMock(spec=QueryExpression)
        # Make fetch() callable by using the real implementation
        expr.fetch = QueryExpression.fetch.__get__(expr, QueryExpression)

        # Mock the output methods
        expr.to_arrays = MagicMock(return_value=np.array([(1, "a"), (2, "b")]))
        expr.to_dicts = MagicMock(return_value=[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        expr.to_pandas = MagicMock()
        expr.proj = MagicMock(return_value=expr)

        return expr

    def test_fetch_emits_deprecation_warning(self, mock_expression):
        """fetch() should emit a DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            mock_expression.fetch()

            assert len(w) == 1
            assert issubclass(w[0].category, DeprecationWarning)
            assert "fetch() is deprecated" in str(w[0].message)

    def test_fetch_default_returns_arrays(self, mock_expression):
        """fetch() with no args should call to_arrays()."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch()

        mock_expression.to_arrays.assert_called_once_with(
            order_by=None, limit=None, offset=None, squeeze=False
        )

    def test_fetch_as_dict_true(self, mock_expression):
        """fetch(as_dict=True) should call to_dicts()."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch(as_dict=True)

        mock_expression.to_dicts.assert_called_once_with(
            order_by=None, limit=None, offset=None, squeeze=False
        )

    def test_fetch_with_attrs_returns_dicts(self, mock_expression):
        """fetch('col1', 'col2') should call proj().to_dicts()."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch("col1", "col2")

        mock_expression.proj.assert_called_once_with("col1", "col2")
        mock_expression.to_dicts.assert_called_once()

    def test_fetch_with_attrs_as_dict_false(self, mock_expression):
        """fetch('col1', 'col2', as_dict=False) should call to_arrays('col1', 'col2')."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch("col1", "col2", as_dict=False)

        mock_expression.to_arrays.assert_called_once_with(
            "col1", "col2", order_by=None, limit=None, offset=None, squeeze=False
        )

    def test_fetch_format_frame(self, mock_expression):
        """fetch(format='frame') should call to_pandas()."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch(format="frame")

        mock_expression.to_pandas.assert_called_once_with(
            order_by=None, limit=None, offset=None, squeeze=False
        )

    def test_fetch_format_frame_with_attrs_raises(self, mock_expression):
        """fetch(format='frame') with attrs should raise error."""
        from datajoint.errors import DataJointError

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            with pytest.raises(DataJointError, match="format='frame' cannot be combined"):
                mock_expression.fetch("col1", format="frame")

    def test_fetch_passes_order_by_limit_offset(self, mock_expression):
        """fetch() should pass order_by, limit, offset to output methods."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch(order_by="id", limit=10, offset=5)

        mock_expression.to_arrays.assert_called_once_with(
            order_by="id", limit=10, offset=5, squeeze=False
        )

    def test_fetch_passes_squeeze(self, mock_expression):
        """fetch(squeeze=True) should pass squeeze to output methods."""
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            mock_expression.fetch(squeeze=True)

        mock_expression.to_arrays.assert_called_once_with(
            order_by=None, limit=None, offset=None, squeeze=True
        )
