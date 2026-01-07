"""
Unit tests for primary key determination rules.

These tests verify the functional dependency logic used to determine
primary keys in join operations.
"""

from datajoint.heading import Heading


def make_heading(pk_attrs, secondary_attrs=None):
    """Helper to create a Heading with specified PK and secondary attributes."""
    secondary_attrs = secondary_attrs or []
    attrs = []
    for name in pk_attrs:
        attrs.append(
            {
                "name": name,
                "type": "int",
                "original_type": None,
                "in_key": True,
                "nullable": False,
                "default": None,
                "comment": "",
                "autoincrement": False,
                "numeric": True,
                "string": False,
                "uuid": False,
                "json": False,
                "is_blob": False,
                "is_hidden": False,
                "codec": None,
                "store": None,
                "unsupported": False,
                "attribute_expression": None,
                "dtype": object,
                "lineage": None,
            }
        )
    for name in secondary_attrs:
        attrs.append(
            {
                "name": name,
                "type": "int",
                "original_type": None,
                "in_key": False,
                "nullable": True,
                "default": None,
                "comment": "",
                "autoincrement": False,
                "numeric": True,
                "string": False,
                "uuid": False,
                "json": False,
                "is_blob": False,
                "is_hidden": False,
                "codec": None,
                "store": None,
                "unsupported": False,
                "attribute_expression": None,
                "dtype": object,
                "lineage": None,
            }
        )
    return Heading(attrs)


class TestDetermines:
    """Tests for Heading.determines() method."""

    def test_a_determines_b_when_b_pk_subset_of_a(self):
        """A → B when all of B's PK is in A."""
        a = make_heading(["x", "y"], ["z"])
        b = make_heading(["x"])
        assert a.determines(b)

    def test_a_determines_b_when_b_pk_in_a_secondary(self):
        """A → B when B's PK attrs are in A's secondary."""
        a = make_heading(["x"], ["y", "z"])
        b = make_heading(["y"])
        assert a.determines(b)

    def test_a_not_determines_b_when_attr_missing(self):
        """A ↛ B when B has PK attr not in A at all."""
        a = make_heading(["x", "y"])
        b = make_heading(["x", "z"])
        assert not a.determines(b)

    def test_both_determine_each_other(self):
        """Both A → B and B → A can be true (bijection-like)."""
        a = make_heading(["x", "y"], ["z"])
        b = make_heading(["y", "z"], ["x"])
        assert a.determines(b)
        assert b.determines(a)

    def test_neither_determines(self):
        """Neither direction when each has attrs not in the other."""
        a = make_heading(["x", "y"])
        b = make_heading(["y", "z"])
        assert not a.determines(b)
        assert not b.determines(a)

    def test_empty_pk_always_determined(self):
        """Empty PK is always determined by any heading."""
        a = make_heading(["x", "y"])
        b = make_heading([])
        assert a.determines(b)

    def test_session_trial_example(self):
        """Classic FK example: Trial → Session (session_id in Trial's PK)."""
        session = make_heading(["session_id"], ["date"])
        trial = make_heading(["session_id", "trial_num"], ["stimulus"])
        # Session → Trial? No (trial_num not in Session)
        assert not session.determines(trial)
        # Trial → Session? Yes (session_id in Trial)
        assert trial.determines(session)


class TestJoinPrimaryKey:
    """Tests for Heading.join() primary key determination."""

    def test_join_a_determines_b(self):
        """When A → B, result PK = PK(A)."""
        a = make_heading(["x", "y"], ["z"])
        b = make_heading(["x"])
        result = a.join(b)
        assert result.primary_key == ["x", "y"]

    def test_join_b_determines_a(self):
        """When B → A (not A → B), result PK = PK(B), B's attrs first."""
        a = make_heading(["x", "y"])
        b = make_heading(["x", "z"], ["y"])
        # A → B? No (z not in A)
        # B → A? Yes (y is secondary in B)
        result = a.join(b)
        assert result.primary_key == ["x", "z"]
        # B's attributes should come first
        assert result.names[0] == "x"
        assert result.names[1] == "z"

    def test_join_both_determine(self):
        """When both A → B and B → A, prefer A (left operand)."""
        a = make_heading(["x", "y"], ["z"])
        b = make_heading(["y", "z"], ["x"])
        result = a.join(b)
        assert result.primary_key == ["x", "y"]

    def test_join_neither_determines(self):
        """When neither determines, result PK = union."""
        a = make_heading(["x", "y"])
        b = make_heading(["y", "z"])
        result = a.join(b)
        # PK should be union: {x, y, z}
        assert set(result.primary_key) == {"x", "y", "z"}
        # A's PK first, then B's new PK attrs
        assert result.primary_key == ["x", "y", "z"]

    def test_join_preserves_secondary_attrs(self):
        """Secondary attributes should be preserved in join."""
        a = make_heading(["x"], ["a"])
        b = make_heading(["x"], ["b"])
        result = a.join(b)
        assert "a" in result.secondary_attributes
        assert "b" in result.secondary_attributes

    def test_join_session_trial(self):
        """Session * Trial should have Trial's PK."""
        session = make_heading(["session_id"], ["date"])
        trial = make_heading(["session_id", "trial_num"], ["stimulus"])
        result = session.join(trial)
        # B → A, so PK = PK(B) = {session_id, trial_num}
        assert set(result.primary_key) == {"session_id", "trial_num"}

    def test_join_nullable_pk_forces_union(self):
        """nullable_pk=True should force union PK."""
        a = make_heading(["x", "y"], ["z"])
        b = make_heading(["x"])
        # Normally A → B, so PK = PK(A)
        normal_result = a.join(b)
        assert normal_result.primary_key == ["x", "y"]
        # With nullable_pk=True, force union
        nullable_result = a.join(b, nullable_pk=True)
        assert nullable_result.primary_key == ["x", "y"]  # Still same since B's PK is subset


class TestJoinAttributeOrdering:
    """Tests for attribute ordering in join results."""

    def test_a_determines_b_ordering(self):
        """When A → B, A's attributes come first."""
        a = make_heading(["x"], ["a"])
        b = make_heading(["x"], ["b"])
        result = a.join(b)
        names = result.names
        assert names.index("x") < names.index("a")
        assert names.index("a") < names.index("b")

    def test_b_determines_a_ordering(self):
        """When B → A, B's attributes come first."""
        a = make_heading(["x", "y"])
        b = make_heading(["x", "z"], ["y"])
        result = a.join(b)
        names = result.names
        # B's attrs first: x, z, then A's non-overlapping attrs
        assert names.index("x") < names.index("z")
        # y should be secondary (demoted from A's PK)
        assert "y" in result.secondary_attributes
