"""
Integration tests for cascade delete on multiple backends.
"""

import os

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a schema for cascade delete tests."""
    backend = db_creds_by_backend["backend"]
    # Use unique schema name for each test
    import time
    test_id = str(int(time.time() * 1000))[-8:]  # Last 8 digits of timestamp
    schema_name = f"djtest_cascade_{backend}_{test_id}"[:64]  # Limit length

    # Drop schema if exists (cleanup from any previous failed runs)
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass  # Ignore errors during cleanup

    # Create fresh schema
    schema = dj.Schema(schema_name, connection=connection_by_backend)

    yield schema

    # Cleanup after test
    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass  # Ignore errors during cleanup


def test_simple_cascade_delete(schema_by_backend):
    """Test basic cascade delete with foreign keys."""

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(255)
        """

    # Insert test data
    Parent.insert1((1, "Parent1"))
    Parent.insert1((2, "Parent2"))
    Child.insert1((1, 1, "Child1-1"))
    Child.insert1((1, 2, "Child1-2"))
    Child.insert1((2, 1, "Child2-1"))

    assert len(Parent()) == 2
    assert len(Child()) == 3

    # Delete parent with cascade
    (Parent & {"parent_id": 1}).delete()

    # Check cascade worked
    assert len(Parent()) == 1
    assert len(Child()) == 1
    assert (Child & {"parent_id": 2, "child_id": 1}).fetch1("data") == "Child2-1"


def test_multi_level_cascade_delete(schema_by_backend):
    """Test cascade delete through multiple levels of foreign keys."""

    @schema_by_backend
    class GrandParent(dj.Manual):
        definition = """
        gp_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        -> GrandParent
        parent_id : int
        ---
        name : varchar(255)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int
        ---
        data : varchar(255)
        """

    # Insert test data
    GrandParent.insert1((1, "GP1"))
    Parent.insert1((1, 1, "P1"))
    Parent.insert1((1, 2, "P2"))
    Child.insert1((1, 1, 1, "C1"))
    Child.insert1((1, 1, 2, "C2"))
    Child.insert1((1, 2, 1, "C3"))

    assert len(GrandParent()) == 1
    assert len(Parent()) == 2
    assert len(Child()) == 3

    # Delete grandparent - should cascade through parent to child
    (GrandParent & {"gp_id": 1}).delete()

    # Check everything is deleted
    assert len(GrandParent()) == 0
    assert len(Parent()) == 0
    assert len(Child()) == 0


def test_cascade_delete_with_renamed_attrs(schema_by_backend):
    """Test cascade delete when foreign key renames attributes."""

    @schema_by_backend
    class Animal(dj.Manual):
        definition = """
        animal_id : int
        ---
        species : varchar(255)
        """

    @schema_by_backend
    class Observation(dj.Manual):
        definition = """
        obs_id : int
        ---
        -> Animal.proj(subject_id='animal_id')
        measurement : float
        """

    # Insert test data
    Animal.insert1((1, "Mouse"))
    Animal.insert1((2, "Rat"))
    Observation.insert1((1, 1, 10.5))
    Observation.insert1((2, 1, 11.2))
    Observation.insert1((3, 2, 15.3))

    assert len(Animal()) == 2
    assert len(Observation()) == 3

    # Delete animal - should cascade to observations
    (Animal & {"animal_id": 1}).delete()

    # Check cascade worked
    assert len(Animal()) == 1
    assert len(Observation()) == 1
    assert (Observation & {"obs_id": 3}).fetch1("measurement") == 15.3
