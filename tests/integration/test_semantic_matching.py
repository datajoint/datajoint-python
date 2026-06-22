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
        assert hasattr(dj.Schema, "rebuild_lineage")

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


class TestLineageRefreshOnDecoration:
    """Tests for #1454: @schema decoration auto-heals missing ~lineage entries.

    Contract: when an already-declared table's heading reports any PK attribute
    with lineage=None, decoration triggers a refresh. The check is in-memory
    against the heading's already-loaded lineage values — no extra DB queries
    on healthy schemas. Stale-but-non-None entries (e.g. DJ version skew) are
    NOT auto-healed and require manual rebuild_lineage().
    """

    def test_redecorate_restores_missing_lineage(self, schema_semantic):
        """
        Delete a table's ~lineage rows entirely, then re-decorate — rows are
        recreated. Primary auto-heal path: PK lineage=None triggers refresh.
        """
        from datajoint.lineage import get_lineage, delete_table_lineages
        from datajoint.heading import Heading

        delete_table_lineages(schema_semantic.connection, schema_semantic.database, "trial")
        # Force heading reload so the deleted state is reflected in memory
        old_heading = Trial._heading
        Trial._heading = Heading(table_info=old_heading.table_info)
        assert get_lineage(schema_semantic.connection, schema_semantic.database, "trial", "session_id") is None

        schema_semantic(Trial)

        refreshed = get_lineage(schema_semantic.connection, schema_semantic.database, "trial", "session_id")
        assert refreshed is not None and "session" in refreshed.lower()

    def test_redecorate_heals_partial_lineage(self, schema_semantic):
        """
        Mixed state: one row stale (non-None bogus), another missing. The in-memory
        check fires on the missing row and the refresh fixes both.
        """
        from datajoint.lineage import get_lineage, delete_table_lineages, insert_lineages
        from datajoint.heading import Heading

        correct_student = get_lineage(schema_semantic.connection, schema_semantic.database, "enrollment", "student_id")
        assert correct_student is not None

        # Wipe both rows, then re-insert ONLY student_id with a stale value.
        # course_id is now missing → triggers auto-heal of all enrollment rows.
        delete_table_lineages(schema_semantic.connection, schema_semantic.database, "enrollment")
        insert_lineages(
            schema_semantic.connection,
            schema_semantic.database,
            [("enrollment", "student_id", "stale_schema.stale_table.stale_attr")],
        )
        old_heading = Enrollment._heading
        Enrollment._heading = Heading(table_info=old_heading.table_info)

        schema_semantic(Enrollment)

        assert get_lineage(schema_semantic.connection, schema_semantic.database, "enrollment", "student_id") == correct_student
        course_lineage = get_lineage(schema_semantic.connection, schema_semantic.database, "enrollment", "course_id")
        assert course_lineage is not None and "course" in course_lineage.lower()

    def test_redecorate_skips_when_lineage_healthy(self, schema_semantic):
        """
        Healthy schema: re-decoration must issue no DELETE/INSERT against ~lineage.
        Verifies the zero-cost path — the in-memory check skips the refresh.
        """
        from datajoint.lineage import get_table_lineages

        # Pre-condition: healthy lineage state
        assert get_table_lineages(schema_semantic.connection, schema_semantic.database, "trial")

        # Intercept any ~lineage write
        connection = schema_semantic.connection
        original_query = connection.query
        write_calls = []

        def counting_query(sql, *args, **kwargs):
            if "lineage" in sql.lower() and any(tok in sql.lower() for tok in ("delete", "insert")):
                write_calls.append(sql)
            return original_query(sql, *args, **kwargs)

        connection.query = counting_query
        try:
            schema_semantic(Trial)
        finally:
            connection.query = original_query

        assert not write_calls, (
            f"Healthy schema decoration must not write to ~lineage; " f"observed {len(write_calls)} write(s): {write_calls}"
        )

    def test_stale_non_none_lineage_not_auto_refreshed(self, schema_semantic):
        """
        Stale-but-non-None lineage values are NOT auto-healed. Users with this
        case must call dj.migrate.rebuild_lineage(schema) or schema.rebuild_lineage().
        Documents the limitation explicitly.
        """
        from datajoint.lineage import (
            get_lineage,
            delete_table_lineages,
            insert_lineages,
            get_table_lineages,
        )
        from datajoint.heading import Heading

        # Replace ALL trial rows with non-None stale values — no None state.
        original = get_table_lineages(schema_semantic.connection, schema_semantic.database, "trial")
        delete_table_lineages(schema_semantic.connection, schema_semantic.database, "trial")
        stale_entries = [("trial", attr, f"stale_schema.stale.{attr}") for attr in original]
        insert_lineages(schema_semantic.connection, schema_semantic.database, stale_entries)
        old_heading = Trial._heading
        Trial._heading = Heading(table_info=old_heading.table_info)

        try:
            schema_semantic(Trial)
            still_stale = get_lineage(schema_semantic.connection, schema_semantic.database, "trial", "session_id")
            assert still_stale == "stale_schema.stale.session_id", (
                f"Expected stale value to persist (no auto-heal for non-None stale); " f"got {still_stale!r}"
            )

            # Manual rebuild fixes it
            schema_semantic.rebuild_lineage()
            fixed = get_lineage(schema_semantic.connection, schema_semantic.database, "trial", "session_id")
            assert fixed is not None and fixed != "stale_schema.stale.session_id"
        finally:
            schema_semantic.rebuild_lineage()
            Trial._heading = Heading(table_info=old_heading.table_info)

    def test_missing_lineage_error_points_to_rebuild(self, schema_semantic):
        """
        When a join fails because one side has None lineage, the error must
        point the user at `schema.rebuild_lineage()`.
        """
        from datajoint.lineage import delete_table_lineages
        from datajoint.heading import Heading

        # Wipe enrollment.student_id lineage by deleting the row, then force the
        # class-level heading to reload from DB so it reflects the missing row.
        delete_table_lineages(schema_semantic.connection, schema_semantic.database, "enrollment")
        old_heading = Enrollment._heading
        Enrollment._heading = Heading(table_info=old_heading.table_info)
        try:
            assert Enrollment().heading["student_id"].lineage is None

            with pytest.raises(DataJointError) as exc_info:
                Student() * Enrollment()
            assert "rebuild_lineage" in str(exc_info.value), f"Error must mention rebuild_lineage(); got: {exc_info.value}"
            assert "stale" in str(exc_info.value).lower() or "missing" in str(exc_info.value).lower()
        finally:
            # Restore lineage so subsequent tests see clean state
            schema_semantic.rebuild_lineage()
            Enrollment._heading = Heading(table_info=old_heading.table_info)
