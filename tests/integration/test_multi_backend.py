"""
Integration tests that verify backend-agnostic behavior.

These tests run against both MySQL and PostgreSQL to ensure:
1. DDL generation is correct
2. SQL queries work identically
3. Data types map correctly

To run these tests:
    pytest tests/integration/test_multi_backend.py  # Run against both backends
    pytest -m "mysql" tests/integration/test_multi_backend.py  # MySQL only
    pytest -m "postgresql" tests/integration/test_multi_backend.py  # PostgreSQL only
"""

import pytest
import datajoint as dj


@pytest.mark.backend_agnostic
def test_simple_table_declaration(connection_by_backend, backend, prefix):
    """Test that simple tables can be declared on both backends."""
    schema = dj.Schema(
        f"{prefix}_multi_backend_{backend}_simple",
        connection=connection_by_backend,
    )

    @schema
    class User(dj.Manual):
        definition = """
        user_id : int
        ---
        username : varchar(255)
        created_at : datetime
        """

    # Verify table exists
    assert User.is_declared

    # Insert and fetch data
    from datetime import datetime

    User.insert1((1, "alice", datetime(2025, 1, 1)))
    data = User.fetch1()

    assert data["user_id"] == 1
    assert data["username"] == "alice"

    # Cleanup
    schema.drop()


@pytest.mark.backend_agnostic
def test_foreign_keys(connection_by_backend, backend, prefix):
    """Test foreign key declarations work on both backends."""
    schema = dj.Schema(
        f"{prefix}_multi_backend_{backend}_fk",
        connection=connection_by_backend,
    )

    @schema
    class Animal(dj.Manual):
        definition = """
        animal_id : int
        ---
        name : varchar(255)
        """

    @schema
    class Observation(dj.Manual):
        definition = """
        -> Animal
        obs_id : int
        ---
        notes : varchar(1000)
        """

    # Insert data
    Animal.insert1((1, "Mouse"))
    Observation.insert1((1, 1, "Active"))

    # Verify data was inserted
    assert len(Animal) == 1
    assert len(Observation) == 1

    # Cleanup
    schema.drop()


@pytest.mark.backend_agnostic
def test_data_types(connection_by_backend, backend, prefix):
    """Test that core data types work on both backends."""
    schema = dj.Schema(
        f"{prefix}_multi_backend_{backend}_types",
        connection=connection_by_backend,
    )

    @schema
    class TypeTest(dj.Manual):
        definition = """
        id : int
        ---
        int_value : int
        str_value : varchar(255)
        float_value : float
        bool_value : bool
        """

    # Insert data
    TypeTest.insert1((1, 42, "test", 3.14, True))

    # Fetch and verify
    data = (TypeTest & {"id": 1}).fetch1()
    assert data["int_value"] == 42
    assert data["str_value"] == "test"
    assert abs(data["float_value"] - 3.14) < 0.001
    assert data["bool_value"] == 1  # MySQL stores as tinyint(1)

    # Cleanup
    schema.drop()


@pytest.mark.backend_agnostic
def test_table_comments(connection_by_backend, backend, prefix):
    """Test that table comments are preserved on both backends."""
    schema = dj.Schema(
        f"{prefix}_multi_backend_{backend}_comments",
        connection=connection_by_backend,
    )

    @schema
    class Commented(dj.Manual):
        definition = """
        # This is a test table for backend testing
        id : int  # primary key
        ---
        value : varchar(255)  # some value
        """

    # Verify table was created
    assert Commented.is_declared

    # Cleanup
    schema.drop()
