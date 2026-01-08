"""Unit tests for condition.py - Top class and merge logic."""

import pytest
from datajoint.condition import Top


class TestTopMerge:
    """Tests for Top.merge() method."""

    def test_merge_inherits_order(self):
        """When other.order_by is None, ordering is inherited."""
        top1 = Top(limit=10, order_by="score desc")
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.order_by == ["score desc"]
        assert merged.limit == 5
        assert merged.offset == 0

    def test_merge_limits_take_min(self):
        """Merged limit is minimum of both."""
        top1 = Top(limit=10, order_by="id")
        top2 = Top(limit=3, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 3

        # Reverse order
        top1 = Top(limit=3, order_by="id")
        top2 = Top(limit=10, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 3

    def test_merge_none_limit_preserved(self):
        """None limit (unlimited) is handled correctly."""
        top1 = Top(limit=None, order_by="id")
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 5

        top1 = Top(limit=5, order_by="id")
        top2 = Top(limit=None, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit == 5

        top1 = Top(limit=None, order_by="id")
        top2 = Top(limit=None, order_by=None)
        merged = top1.merge(top2)
        assert merged.limit is None

    def test_merge_offsets_add(self):
        """Offsets are added together."""
        top1 = Top(limit=10, order_by="id", offset=5)
        top2 = Top(limit=3, order_by=None, offset=2)
        merged = top1.merge(top2)
        assert merged.offset == 7

    def test_merge_preserves_existing_order(self):
        """Merged Top preserves first Top's ordering."""
        top1 = Top(limit=10, order_by=["col1 desc", "col2 asc"])
        top2 = Top(limit=5, order_by=None)
        merged = top1.merge(top2)
        assert merged.order_by == ["col1 desc", "col2 asc"]


class TestTopValidation:
    """Tests for Top validation."""

    def test_order_by_none_allowed(self):
        """order_by=None is valid (means inherit)."""
        top = Top(limit=5, order_by=None)
        assert top.order_by is None

    def test_order_by_string_converted_to_list(self):
        """Single string order_by is converted to list."""
        top = Top(order_by="id desc")
        assert top.order_by == ["id desc"]

    def test_order_by_list_preserved(self):
        """List order_by is preserved."""
        top = Top(order_by=["col1", "col2 desc"])
        assert top.order_by == ["col1", "col2 desc"]

    def test_invalid_limit_type_raises(self):
        """Non-integer limit raises TypeError."""
        with pytest.raises(TypeError):
            Top(limit="5")

    def test_invalid_order_by_type_raises(self):
        """Non-string order_by raises TypeError."""
        with pytest.raises(TypeError):
            Top(order_by=123)

    def test_invalid_offset_type_raises(self):
        """Non-integer offset raises TypeError."""
        with pytest.raises(TypeError):
            Top(offset="1")
