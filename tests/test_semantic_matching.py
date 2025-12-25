"""
Tests for semantic matching in joins.

Semantic matching ensures that attributes are only matched when they share
both the same name AND the same lineage (origin).
"""

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture
def schema_lineage(connection):
    """
    Create a schema with tables for testing lineage tracking and semantic matching.
    """
    schema = dj.Schema("test_lineage", connection=connection, create_schema=True)

    @schema
    class Person(dj.Manual):
        definition = """
        person_id : int
        ---
        name : varchar(100)
        """

    @schema
    class Course(dj.Manual):
        definition = """
        course_id : int
        ---
        title : varchar(100)
        """

    @schema
    class Student(dj.Manual):
        definition = """
        -> Person
        ---
        enrollment_year : int
        """

    @schema
    class Instructor(dj.Manual):
        definition = """
        -> Person
        ---
        department : varchar(100)
        """

    @schema
    class Enrollment(dj.Manual):
        definition = """
        -> Student
        -> Course
        ---
        grade : varchar(2)
        """

    @schema
    class Teaching(dj.Manual):
        definition = """
        -> Instructor
        -> Course
        ---
        semester : varchar(20)
        """

    # Tables with non-homologous namesakes (different lineages for same name)
    @schema
    class TableWithId1(dj.Manual):
        definition = """
        id : int   # native PK - lineage is this table
        ---
        value1 : int
        """

    @schema
    class TableWithId2(dj.Manual):
        definition = """
        id : int   # native PK - lineage is this table (different from TableWithId1)
        ---
        value2 : int
        """

    # Insert test data
    Person.insert(
        [
            {"person_id": 1, "name": "Alice"},
            {"person_id": 2, "name": "Bob"},
            {"person_id": 3, "name": "Charlie"},
        ],
        skip_duplicates=True,
    )
    Course.insert(
        [
            {"course_id": 101, "title": "Math"},
            {"course_id": 102, "title": "Physics"},
        ],
        skip_duplicates=True,
    )
    Student.insert(
        [
            {"person_id": 1, "enrollment_year": 2020},
            {"person_id": 2, "enrollment_year": 2021},
        ],
        skip_duplicates=True,
    )
    Instructor.insert(
        [
            {"person_id": 3, "department": "Science"},
        ],
        skip_duplicates=True,
    )
    Enrollment.insert(
        [
            {"person_id": 1, "course_id": 101, "grade": "A"},
            {"person_id": 1, "course_id": 102, "grade": "B"},
            {"person_id": 2, "course_id": 101, "grade": "B"},
        ],
        skip_duplicates=True,
    )
    Teaching.insert(
        [
            {"person_id": 3, "course_id": 101, "semester": "Fall 2023"},
            {"person_id": 3, "course_id": 102, "semester": "Spring 2024"},
        ],
        skip_duplicates=True,
    )
    TableWithId1.insert(
        [{"id": 1, "value1": 10}, {"id": 2, "value1": 20}],
        skip_duplicates=True,
    )
    TableWithId2.insert(
        [{"id": 1, "value2": 100}, {"id": 2, "value2": 200}],
        skip_duplicates=True,
    )

    yield {
        "schema": schema,
        "Person": Person,
        "Course": Course,
        "Student": Student,
        "Instructor": Instructor,
        "Enrollment": Enrollment,
        "Teaching": Teaching,
        "TableWithId1": TableWithId1,
        "TableWithId2": TableWithId2,
    }

    schema.drop(force=True)


class TestLineageTracking:
    """Test that lineage is correctly tracked for attributes."""

    def test_native_pk_has_lineage(self, schema_lineage):
        """Native primary key attributes should have lineage pointing to their table."""
        Person = schema_lineage["Person"]
        lineage = Person.heading["person_id"].lineage
        assert lineage is not None
        assert "person" in lineage
        assert "person_id" in lineage

    def test_fk_inherited_has_parent_lineage(self, schema_lineage):
        """FK-inherited attributes should have lineage tracing to their origin."""
        Student = schema_lineage["Student"]
        # person_id is inherited from Person
        lineage = Student.heading["person_id"].lineage
        assert lineage is not None
        assert "person" in lineage  # Should trace to Person table
        assert "person_id" in lineage

    def test_native_secondary_has_no_lineage(self, schema_lineage):
        """Native secondary attributes should have no lineage."""
        Person = schema_lineage["Person"]
        lineage = Person.heading["name"].lineage
        assert lineage is None

    def test_fk_chain_preserves_lineage(self, schema_lineage):
        """Lineage should be preserved through FK chains."""
        Enrollment = schema_lineage["Enrollment"]
        # person_id traces through Student -> Person
        lineage = Enrollment.heading["person_id"].lineage
        assert lineage is not None
        assert "person" in lineage


class TestHomologousNamesakes:
    """Test that homologous namesakes (same name AND same lineage) are correctly matched."""

    def test_join_on_homologous_namesakes(self, schema_lineage):
        """Tables with shared FK origin should join on that attribute."""
        Student = schema_lineage["Student"]
        Enrollment = schema_lineage["Enrollment"]

        # Both have person_id with same lineage (from Person)
        result = Student * Enrollment
        assert len(result) > 0
        assert "person_id" in result.heading.names

    def test_multi_table_join_with_shared_fk(self, schema_lineage):
        """Multiple tables sharing same FK origin should join correctly."""
        Enrollment = schema_lineage["Enrollment"]
        Teaching = schema_lineage["Teaching"]

        # Both have course_id with same lineage (from Course)
        result = Enrollment * Teaching
        assert len(result) > 0
        assert "course_id" in result.heading.names


class TestNonHomologousNamesakes:
    """Test that non-homologous namesakes (same name, different lineage) raise errors."""

    def test_different_pk_origins_raise_error(self, schema_lineage):
        """Tables with same-named PKs but different origins should raise error."""
        TableWithId1 = schema_lineage["TableWithId1"]
        TableWithId2 = schema_lineage["TableWithId2"]

        # Both have 'id' but with different lineages
        with pytest.raises(DataJointError) as exc_info:
            TableWithId1 * TableWithId2

        assert "different lineages" in str(exc_info.value)
        assert "id" in str(exc_info.value)

    def test_semantic_check_false_bypasses_error(self, schema_lineage):
        """semantic_check=False should bypass the lineage check."""
        TableWithId1 = schema_lineage["TableWithId1"]
        TableWithId2 = schema_lineage["TableWithId2"]

        # Should not raise with semantic_check=False
        result = TableWithId1().join(TableWithId2(), semantic_check=False)
        assert result is not None


class TestDeprecatedOperators:
    """Test that deprecated operators raise appropriate errors."""

    def test_matmul_operator_removed(self, schema_lineage):
        """The @ operator should raise an error directing to .join()."""
        Person = schema_lineage["Person"]
        Student = schema_lineage["Student"]

        with pytest.raises(DataJointError) as exc_info:
            Person @ Student

        assert "@ operator has been removed" in str(exc_info.value)
        assert ".join" in str(exc_info.value)
        assert "semantic_check=False" in str(exc_info.value)

    def test_u_mul_deprecated(self, schema_lineage):
        """dj.U * table should raise deprecation error."""
        Person = schema_lineage["Person"]

        with pytest.raises(DataJointError) as exc_info:
            dj.U("person_id") * Person

        assert "deprecated" in str(exc_info.value).lower()
        assert "&" in str(exc_info.value)

    def test_u_sub_not_supported(self, schema_lineage):
        """dj.U - table should raise error (infinite set)."""
        Person = schema_lineage["Person"]

        with pytest.raises(DataJointError) as exc_info:
            dj.U("person_id") - Person

        assert "infinite" in str(exc_info.value).lower()


class TestUniversalSet:
    """Test dj.U operations with semantic matching."""

    def test_u_restriction_works(self, schema_lineage):
        """dj.U & table should work correctly."""
        Person = schema_lineage["Person"]

        result = dj.U("person_id") & Person
        assert len(result) == len(Person)
        assert "person_id" in result.primary_key

    def test_u_empty_restriction(self, schema_lineage):
        """dj.U() & table should return distinct primary keys."""
        Person = schema_lineage["Person"]

        result = dj.U() & Person
        assert len(result) == len(Person)

    def test_u_aggr_works(self, schema_lineage):
        """dj.U().aggr() should work correctly."""
        Person = schema_lineage["Person"]

        result = dj.U().aggr(Person, n="count(*)")
        assert len(result) == 1
        row = result.fetch1()
        assert row["n"] == len(Person)

    def test_u_is_always_compatible(self, schema_lineage):
        """dj.U should be compatible with any expression (contains all lineages)."""
        TableWithId1 = schema_lineage["TableWithId1"]

        # U should be compatible even with tables that have unique lineages
        result = dj.U("id") & TableWithId1
        assert len(result) > 0


class TestLineageInProjection:
    """Test that lineage is preserved correctly in projection operations."""

    def test_included_attrs_preserve_lineage(self, schema_lineage):
        """Projected attributes should preserve their lineage."""
        Student = schema_lineage["Student"]

        projected = Student.proj()
        assert projected.heading["person_id"].lineage == Student.heading["person_id"].lineage

    def test_renamed_attrs_preserve_lineage(self, schema_lineage):
        """Renamed attributes should preserve their original lineage."""
        Student = schema_lineage["Student"]

        renamed = Student.proj(pid="person_id")
        # The renamed attribute should have the same lineage as the original
        assert renamed.heading["pid"].lineage == Student.heading["person_id"].lineage

    def test_computed_attrs_have_no_lineage(self, schema_lineage):
        """Computed attributes should have no lineage."""
        Student = schema_lineage["Student"]

        computed = Student.proj(doubled="enrollment_year * 2")
        assert computed.heading["doubled"].lineage is None


@pytest.fixture
def schema_pk_rules(connection):
    """
    Create a schema with tables for testing join primary key rules.

    These tables are designed to test the functional dependency rules:
    - A → B: every attr in PK(B) is either in PK(A) or secondary in A
    - B → A: every attr in PK(A) is either in PK(B) or secondary in B
    """
    schema = dj.Schema("test_pk_rules", connection=connection, create_schema=True)

    # Base tables for testing various scenarios
    @schema
    class TableX(dj.Manual):
        """Table with single PK attribute x."""

        definition = """
        x : int
        ---
        x_data : int
        """

    @schema
    class TableXY(dj.Manual):
        """Table with composite PK (x, y)."""

        definition = """
        x : int
        y : int
        ---
        xy_data : int
        """

    @schema
    class TableXZ(dj.Manual):
        """Table with composite PK (x, z)."""

        definition = """
        x : int
        z : int
        ---
        xz_data : int
        """

    @schema
    class TableZ(dj.Manual):
        """Table with single PK z and secondary x."""

        definition = """
        z : int
        ---
        x : int
        z_data : int
        """

    @schema
    class TableXZwithY(dj.Manual):
        """Table with PK (x, z) and secondary y."""

        definition = """
        x : int
        z : int
        ---
        y : int
        xzy_data : int
        """

    @schema
    class TableYZwithX(dj.Manual):
        """Table with PK (y, z) and secondary x."""

        definition = """
        y : int
        z : int
        ---
        x : int
        yzx_data : int
        """

    @schema
    class TableXYwithZ(dj.Manual):
        """Table with PK (x, y) and secondary z."""

        definition = """
        x : int
        y : int
        ---
        z : int
        xyz_data : int
        """

    # Insert test data
    TableX.insert([{"x": 1, "x_data": 10}, {"x": 2, "x_data": 20}], skip_duplicates=True)
    TableXY.insert(
        [
            {"x": 1, "y": 1, "xy_data": 11},
            {"x": 1, "y": 2, "xy_data": 12},
            {"x": 2, "y": 1, "xy_data": 21},
        ],
        skip_duplicates=True,
    )
    TableXZ.insert(
        [
            {"x": 1, "z": 1, "xz_data": 11},
            {"x": 1, "z": 2, "xz_data": 12},
            {"x": 2, "z": 1, "xz_data": 21},
        ],
        skip_duplicates=True,
    )
    TableZ.insert(
        [
            {"z": 1, "x": 1, "z_data": 10},
            {"z": 2, "x": 1, "z_data": 20},
            {"z": 3, "x": 2, "z_data": 30},
        ],
        skip_duplicates=True,
    )
    TableXZwithY.insert(
        [
            {"x": 1, "z": 1, "y": 1, "xzy_data": 111},
            {"x": 1, "z": 2, "y": 2, "xzy_data": 122},
            {"x": 2, "z": 1, "y": 1, "xzy_data": 211},
        ],
        skip_duplicates=True,
    )
    TableYZwithX.insert(
        [
            {"y": 1, "z": 1, "x": 1, "yzx_data": 111},
            {"y": 1, "z": 2, "x": 2, "yzx_data": 122},
            {"y": 2, "z": 1, "x": 1, "yzx_data": 211},
        ],
        skip_duplicates=True,
    )
    TableXYwithZ.insert(
        [
            {"x": 1, "y": 1, "z": 1, "xyz_data": 111},
            {"x": 1, "y": 2, "z": 2, "xyz_data": 122},
            {"x": 2, "y": 1, "z": 1, "xyz_data": 211},
        ],
        skip_duplicates=True,
    )

    yield {
        "schema": schema,
        "TableX": TableX,
        "TableXY": TableXY,
        "TableXZ": TableXZ,
        "TableZ": TableZ,
        "TableXZwithY": TableXZwithY,
        "TableYZwithX": TableYZwithX,
        "TableXYwithZ": TableXYwithZ,
    }

    schema.drop(force=True)


class TestJoinPrimaryKeyRules:
    """
    Test the join primary key determination rules.

    The rules are:
    - A → B: PK(A * B) = PK(A), A's attributes first
    - B → A (not A → B): PK(A * B) = PK(B), B's attributes first
    - Both A → B and B → A: PK(A * B) = PK(A) (left preference)
    - Neither: PK(A * B) = PK(A) ∪ PK(B)
    """

    def test_b_determines_a(self, schema_pk_rules):
        """
        Test case: B → A (y is secondary in B, so PK(B) determines y).

        A: x*, y*           PK(A) = {x, y}
        B: x*, z*, y        PK(B) = {x, z}, y is secondary

        A → B? z not in PK(A) and z not secondary in A → No
        B → A? y secondary in B → Yes

        Result: PK = {x, z}, B's attributes first
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        result = TableXY * TableXZwithY

        # PK should be {x, z} (PK of B)
        assert set(result.primary_key) == {"x", "z"}
        # B's attributes should come first (x, z are both in B's PK)
        assert result.heading.names[0] in {"x", "z"}
        assert result.heading.names[1] in {"x", "z"}

    def test_both_directions_bijection_like(self, schema_pk_rules):
        """
        Test case: Both A → B and B → A (bijection-like).

        A: x*, y*, z        PK(A) = {x, y}, z is secondary
        B: y*, z*, x        PK(B) = {y, z}, x is secondary

        A → B? z secondary in A → Yes
        B → A? x secondary in B → Yes

        Both hold, prefer left: PK = {x, y}, A's attributes first
        """
        TableXYwithZ = schema_pk_rules["TableXYwithZ"]
        TableYZwithX = schema_pk_rules["TableYZwithX"]

        result = TableXYwithZ * TableYZwithX

        # PK should be {x, y} (PK of A, left preference)
        assert set(result.primary_key) == {"x", "y"}
        # A's PK attributes should come first
        assert result.heading.names[0] in {"x", "y"}
        assert result.heading.names[1] in {"x", "y"}

    def test_neither_direction(self, schema_pk_rules):
        """
        Test case: Neither A → B nor B → A.

        A: x*, y*           PK(A) = {x, y}
        B: z*, x            PK(B) = {z}, x is secondary

        A → B? z not in PK(A) and z not secondary in A → No
        B → A? y not in PK(B) and y not secondary in B → No

        Result: PK = {x, y, z} (union), A's attributes first
        """
        TableXY = schema_pk_rules["TableXY"]
        TableZ = schema_pk_rules["TableZ"]

        result = TableXY * TableZ

        # PK should be {x, y, z} (union)
        assert set(result.primary_key) == {"x", "y", "z"}
        # A's PK attributes should come first
        pk_names = result.primary_key
        assert pk_names[0] in {"x", "y"}
        assert pk_names[1] in {"x", "y"}
        assert pk_names[2] == "z"

    def test_a_determines_b_simple(self, schema_pk_rules):
        """
        Test case: A → B (simple subordinate relationship).

        A: x*               PK(A) = {x}
        B: x*, y*           PK(B) = {x, y}

        A → B? x in PK(A), y not in PK(A), y not secondary in A → No
        B → A? x in PK(B) → Yes

        Result: PK = {x, y} (PK of B), B's attributes first
        """
        TableX = schema_pk_rules["TableX"]
        TableXY = schema_pk_rules["TableXY"]

        result = TableX * TableXY

        # B → A holds (x is in PK(B)), A → B doesn't (y not in A)
        # Result: PK = PK(B) = {x, y}
        assert set(result.primary_key) == {"x", "y"}

    def test_non_commutativity_pk_selection(self, schema_pk_rules):
        """
        Test that A * B may have different PK than B * A.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        result_ab = TableXY * TableXZwithY
        result_ba = TableXZwithY * TableXY

        # For A * B: B → A, so PK = {x, z}
        assert set(result_ab.primary_key) == {"x", "z"}

        # For B * A: A is now the "other", and A → B doesn't hold,
        # B → A still means the new A (old B) determines new B (old A)
        # Actually, let's recalculate:
        # New A = TableXZwithY: PK = {x, z}, y is secondary
        # New B = TableXY: PK = {x, y}
        # New A → New B? y secondary in new A → Yes
        # So PK = PK(new A) = {x, z}
        assert set(result_ba.primary_key) == {"x", "z"}

        # In this case, both have the same PK but potentially different attribute order

    def test_non_commutativity_attribute_order(self, schema_pk_rules):
        """
        Test that attribute order depends on which operand provides the PK.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        result_ab = TableXY * TableXZwithY  # B → A, B's attrs first
        result_ba = TableXZwithY * TableXY  # A → B, A's attrs first

        # In result_ab, B (TableXZwithY) provides PK, so its attrs come first
        # In result_ba, A (TableXZwithY) provides PK, so its attrs come first
        # Both should have TableXZwithY's attributes first
        ab_names = result_ab.heading.names
        ba_names = result_ba.heading.names

        # The first attributes should be from the PK-providing table
        # Both cases have TableXZwithY providing the PK
        assert ab_names[0] in {"x", "z"}
        assert ba_names[0] in {"x", "z"}

    def test_join_preserves_all_attributes(self, schema_pk_rules):
        """
        Test that all attributes from both tables are included in the result.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        result = TableXY * TableXZwithY

        # All unique attributes should be present
        all_expected = {"x", "y", "z", "xy_data", "xzy_data"}
        assert set(result.heading.names) == all_expected

    def test_pk_attributes_come_first(self, schema_pk_rules):
        """
        Test that primary key attributes always come first in the heading.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableZ = schema_pk_rules["TableZ"]

        result = TableXY * TableZ

        # PK = {x, y, z}
        pk = set(result.primary_key)
        names = result.heading.names

        # All PK attributes should come before any secondary attributes
        pk_indices = [names.index(attr) for attr in pk]
        secondary_indices = [names.index(attr) for attr in names if attr not in pk]

        if secondary_indices:  # If there are secondary attributes
            assert max(pk_indices) < min(secondary_indices)


class TestLeftJoinConstraint:
    """
    Test that left joins require A → B (left operand determines right operand).

    For left joins, B's attributes could be NULL for unmatched rows, so the PK
    must be PK(A) only. This is only valid when A → B.
    """

    def test_left_join_valid_when_a_determines_b(self, schema_pk_rules):
        """
        Left join should work when A → B.

        A: x*, y*, z        PK(A) = {x, y}, z is secondary
        B: y*, z*, x        PK(B) = {y, z}, x is secondary

        A → B? z secondary in A → Yes
        Left join is valid, PK = {x, y}
        """
        TableXYwithZ = schema_pk_rules["TableXYwithZ"]
        TableYZwithX = schema_pk_rules["TableYZwithX"]

        # This should work - A → B holds
        result = TableXYwithZ().join(TableYZwithX(), left=True)

        # PK should be PK(A) = {x, y}
        assert set(result.primary_key) == {"x", "y"}

    def test_left_join_fails_when_b_determines_a_only(self, schema_pk_rules):
        """
        Left join should fail when only B → A (not A → B).

        A: x*, y*           PK(A) = {x, y}
        B: x*, z*, y        PK(B) = {x, z}, y is secondary

        A → B? z not in PK(A) and z not secondary in A → No
        B → A? y secondary in B → Yes

        Left join is invalid because z would need to be in PK but could be NULL.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        # This should fail - A → B does not hold
        with pytest.raises(DataJointError) as exc_info:
            TableXY().join(TableXZwithY(), left=True)

        assert "Left join requires" in str(exc_info.value)
        assert "A → B" in str(exc_info.value) or "determine" in str(exc_info.value)

    def test_left_join_fails_when_neither_direction(self, schema_pk_rules):
        """
        Left join should fail when neither A → B nor B → A.

        A: x*, y*           PK(A) = {x, y}
        B: z*, x            PK(B) = {z}, x is secondary

        A → B? z not in A → No
        B → A? y not in B → No

        Left join is invalid.
        """
        TableXY = schema_pk_rules["TableXY"]
        TableZ = schema_pk_rules["TableZ"]

        # This should fail - A → B does not hold
        with pytest.raises(DataJointError) as exc_info:
            TableXY().join(TableZ(), left=True)

        assert "Left join requires" in str(exc_info.value)

    def test_inner_join_still_works_when_b_determines_a(self, schema_pk_rules):
        """
        Inner join should still work normally when B → A (even though left join fails).
        """
        TableXY = schema_pk_rules["TableXY"]
        TableXZwithY = schema_pk_rules["TableXZwithY"]

        # Inner join should work - B → A applies
        result = TableXY * TableXZwithY

        # PK should be {x, z} (B's PK)
        assert set(result.primary_key) == {"x", "z"}
