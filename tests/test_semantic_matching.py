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
