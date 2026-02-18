"""
Tests for semantic matching in joins.

These tests verify the lineage-based semantic matching system
that prevents incorrect joins on attributes with the same name
but different origins.
"""

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


# Schema definitions for semantic matching tests
LOCALS_SEMANTIC = {}


class Student(dj.Manual):
    definition = """
    student_id : int
    ---
    name : varchar(100)
    """


class Course(dj.Manual):
    definition = """
    course_id : int
    ---
    title : varchar(100)
    """


class Enrollment(dj.Manual):
    definition = """
    -> Student
    -> Course
    ---
    grade : varchar(2)
    """


class Session(dj.Manual):
    definition = """
    session_id : int
    ---
    date : date
    """


class Trial(dj.Manual):
    definition = """
    -> Session
    trial_num : int
    ---
    stimulus : varchar(100)
    """


class Response(dj.Computed):
    definition = """
    -> Trial
    ---
    response_time : float
    """


# Tables with generic 'id' attribute for collision testing
class TableWithId1(dj.Manual):
    definition = """
    id : int
    ---
    value1 : int
    """


class TableWithId2(dj.Manual):
    definition = """
    id : int
    ---
    value2 : int
    """


# Register all classes in LOCALS_SEMANTIC
for cls in [
    Student,
    Course,
    Enrollment,
    Session,
    Trial,
    Response,
    TableWithId1,
    TableWithId2,
]:
    LOCALS_SEMANTIC[cls.__name__] = cls


@pytest.fixture(scope="module")
def schema_semantic(connection_test, prefix):
    """Schema for semantic matching tests."""
    schema = dj.Schema(
        prefix + "_semantic",
        context=LOCALS_SEMANTIC,
        connection=connection_test,
    )
    # Declare tables
    schema(Student)
    schema(Course)
    schema(Enrollment)
    schema(Session)
    schema(Trial)
    # Skip Response for now - it's a computed table
    schema(TableWithId1)
    schema(TableWithId2)

    yield schema
    schema.drop()


class TestLineageComputation:
    """Tests for lineage computation from dependency graph."""

    def test_native_primary_key_has_lineage(self, schema_semantic):
        """Native primary key attributes should have lineage pointing to their table."""
        student = Student()
        lineage = student.heading["student_id"].lineage
        assert lineage is not None
        assert "student_id" in lineage
        # The lineage should include schema and table name
        assert "student" in lineage.lower()

    def test_inherited_attribute_traces_to_origin(self, schema_semantic):
        """FK-inherited attributes should trace lineage to the original table."""
        enrollment = Enrollment()
        # student_id is inherited from Student
        student_lineage = enrollment.heading["student_id"].lineage
        assert student_lineage is not None
        assert "student" in student_lineage.lower()

        # course_id is inherited from Course
        course_lineage = enrollment.heading["course_id"].lineage
        assert course_lineage is not None
        assert "course" in course_lineage.lower()

    def test_secondary_attribute_no_lineage(self, schema_semantic):
        """Native secondary attributes should have no lineage."""
        student = Student()
        name_lineage = student.heading["name"].lineage
        assert name_lineage is None

    def test_multi_hop_inheritance(self, schema_semantic):
        """Lineage should trace through multiple FK hops."""
        trial = Trial()
        # session_id in Trial is inherited from Session
        session_lineage = trial.heading["session_id"].lineage
        assert session_lineage is not None
        assert "session" in session_lineage.lower()


class TestJoinCompatibility:
    """Tests for join compatibility checking."""

    def test_join_on_shared_lineage_works(self, schema_semantic):
        """Joining tables with shared lineage should work."""
        student = Student()
        enrollment = Enrollment()

        # This should work - student_id has same lineage in both
        result = student * enrollment
        assert "student_id" in result.heading.names

    def test_join_different_lineage_default_fails(self, schema_semantic):
        """By default (semantic_check=True), non-homologous namesakes cause an error."""
        table1 = TableWithId1()
        table2 = TableWithId2()

        # Default is semantic_check=True, this should fail
        with pytest.raises(DataJointError) as exc_info:
            table1 * table2

        assert "lineage" in str(exc_info.value).lower()
        assert "id" in str(exc_info.value)

    def test_join_different_lineage_semantic_check_false_works(self, schema_semantic):
        """With semantic_check=False, no lineage checking - natural join proceeds."""
        table1 = TableWithId1()
        table2 = TableWithId2()

        # With semantic_check=False, no error even with different lineages
        result = table1.join(table2, semantic_check=False)
        assert "id" in result.heading.names


class TestRestrictCompatibility:
    """Tests for restriction compatibility checking."""

    def test_restrict_shared_lineage_works(self, schema_semantic):
        """Restricting with shared lineage should work."""
        student = Student()
        enrollment = Enrollment()

        # This should work - student_id has same lineage
        result = student & enrollment
        assert "student_id" in result.heading.names

    def test_restrict_different_lineage_default_fails(self, schema_semantic):
        """By default (semantic_check=True), non-homologous namesakes cause an error."""
        table1 = TableWithId1()
        table2 = TableWithId2()

        # Default is semantic_check=True, this should fail
        with pytest.raises(DataJointError) as exc_info:
            table1 & table2

        assert "lineage" in str(exc_info.value).lower()

    def test_restrict_different_lineage_semantic_check_false_works(self, schema_semantic):
        """With semantic_check=False, no lineage checking - restriction proceeds."""
        table1 = TableWithId1()
        table2 = TableWithId2()

        # With semantic_check=False, no error even with different lineages
        result = table1.restrict(table2, semantic_check=False)
        assert "id" in result.heading.names


class TestProjectionLineage:
    """Tests for lineage preservation in projections."""

    def test_projection_preserves_lineage(self, schema_semantic):
        """Projected attributes should preserve their lineage."""
        enrollment = Enrollment()

        projected = enrollment.proj("grade")
        # Primary key attributes should still have lineage
        assert projected.heading["student_id"].lineage is not None

    def test_renamed_attribute_preserves_lineage(self, schema_semantic):
        """Renamed attributes should preserve their original lineage."""
        student = Student()

        renamed = student.proj(sid="student_id")
        # The renamed attribute should have the same lineage as original
        original_lineage = student.heading["student_id"].lineage
        renamed_lineage = renamed.heading["sid"].lineage
        assert renamed_lineage == original_lineage

    def test_computed_attribute_no_lineage(self, schema_semantic):
        """Computed attributes should have no lineage."""
        student = Student()

        computed = student.proj(doubled="student_id * 2")
        # Computed attributes have no lineage
        assert computed.heading["doubled"].lineage is None


class TestRemovedOperators:
    """Tests for removed operators."""

    def test_matmul_operator_removed(self, schema_semantic):
        """The @ operator should raise an error."""
        student = Student()
        course = Course()

        with pytest.raises(DataJointError) as exc_info:
            student @ course

        assert "@" in str(exc_info.value) or "matmul" in str(exc_info.value).lower()
        assert "removed" in str(exc_info.value).lower()

    def test_xor_operator_removed(self, schema_semantic):
        """The ^ operator should raise an error."""
        student = Student()
        course = Course()

        with pytest.raises(DataJointError) as exc_info:
            student ^ course

        assert "^" in str(exc_info.value) or "removed" in str(exc_info.value).lower()


class TestUniversalSetOperators:
    """Tests for dj.U operations."""

    def test_u_mul_raises_error(self, schema_semantic):
        """dj.U * table should raise an error."""
        student = Student()

        with pytest.raises(DataJointError) as exc_info:
            dj.U("student_id") * student

        assert "no longer supported" in str(exc_info.value).lower()

    def test_table_mul_u_raises_error(self, schema_semantic):
        """table * dj.U should raise an error."""
        student = Student()

        with pytest.raises(DataJointError) as exc_info:
            student * dj.U("student_id")

        assert "no longer supported" in str(exc_info.value).lower()

    def test_u_sub_raises_error(self, schema_semantic):
        """dj.U - table should raise an error (infinite set)."""
        student = Student()

        with pytest.raises(DataJointError) as exc_info:
            dj.U("student_id") - student

        assert "infinite" in str(exc_info.value).lower()

    def test_u_and_works(self, schema_semantic):
        """dj.U & table should work for restriction."""
        student = Student()
        student.insert([{"student_id": 1, "name": "Alice"}, {"student_id": 2, "name": "Bob"}])

        result = dj.U("student_id") & student
        assert len(result) == 2


class TestRebuildLineageUtility:
    """Tests for the lineage rebuild utility."""

    def test_rebuild_lineage_method_exists(self):
        """The rebuild_lineage method should exist on Schema."""
        from datajoint.schemas import Schema as _Schema

        assert hasattr(_Schema, "rebuild_lineage")

    def test_rebuild_lineage_populates_table(self, schema_semantic):
        """schema.rebuild_lineage() should populate the ~lineage table."""
        from datajoint.lineage import get_table_lineages

        # Run rebuild using Schema method
        schema_semantic.rebuild_lineage()

        # Check that ~lineage table was created
        assert schema_semantic.lineage_table_exists

        # Check that lineages were populated for Student table
        lineages = get_table_lineages(schema_semantic.connection, schema_semantic.database, "student")
        assert "student_id" in lineages
