"""
Tests for skip_duplicates behavior with secondary unique constraints.

Verifies that skip_duplicates=True on PostgreSQL skips primary key
duplicates while still raising on secondary unique constraint violations.
Resolves #1049.
"""

import time

import pytest

import datajoint as dj
from datajoint.errors import DuplicateError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_skipdup_{backend}_{test_id}"[:64]

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


def test_skip_duplicates_pk_match(schema_by_backend):
    """skip_duplicates=True silently skips rows whose PK already exists."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int
        ---
        name : varchar(100)
        email : varchar(100)
        unique index (email)
        """

    Item.insert1(dict(item_id=1, name="Alice", email="alice@example.com"))

    # Same PK, different values — should be silently skipped
    Item.insert1(
        dict(item_id=1, name="Bob", email="bob@example.com"),
        skip_duplicates=True,
    )

    # Original row unchanged
    row = (Item & "item_id=1").fetch1()
    assert row["name"] == "Alice"
    assert row["email"] == "alice@example.com"


def test_skip_duplicates_unique_violation_raises_on_postgres(schema_by_backend, db_creds_by_backend):
    """On PostgreSQL, skip_duplicates=True still raises on secondary unique violations.

    Regression test for #1049: a row with a *new* PK but a *conflicting*
    secondary unique index value must raise DuplicateError on PostgreSQL.
    """
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific: ON CONFLICT (pk) DO NOTHING preserves unique constraints")

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int
        ---
        name : varchar(100)
        email : varchar(100)
        unique index (email)
        """

    Item.insert1(dict(item_id=1, name="Alice", email="alice@example.com"))

    # New PK (2) but email conflicts with existing row (1)
    with pytest.raises(DuplicateError):
        Item.insert1(
            dict(item_id=2, name="Bob", email="alice@example.com"),
            skip_duplicates=True,
        )


def test_skip_duplicates_unique_on_mysql(schema_by_backend, db_creds_by_backend):
    """On MySQL, skip_duplicates=True silently skips secondary unique conflicts.

    Documents the known MySQL asymmetry: ON DUPLICATE KEY UPDATE catches
    all unique key conflicts, not just primary key.
    """
    if db_creds_by_backend["backend"] != "mysql":
        pytest.skip("MySQL-specific: ON DUPLICATE KEY UPDATE catches all unique keys")

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int
        ---
        name : varchar(100)
        email : varchar(100)
        unique index (email)
        """

    Item.insert1(dict(item_id=1, name="Alice", email="alice@example.com"))

    # New PK (2) but email conflicts — MySQL silently skips
    Item.insert1(
        dict(item_id=2, name="Bob", email="alice@example.com"),
        skip_duplicates=True,
    )

    # Only the original row exists
    assert len(Item()) == 1
    assert (Item & "item_id=1").fetch1()["name"] == "Alice"


def test_skip_duplicates_no_unique_index(schema_by_backend):
    """skip_duplicates=True works normally on tables without secondary unique indexes."""

    @schema_by_backend
    class Simple(dj.Manual):
        definition = """
        item_id : int
        ---
        name : varchar(100)
        """

    Simple.insert1(dict(item_id=1, name="Alice"))

    # Same PK, different name — silently skipped
    Simple.insert1(dict(item_id=1, name="Bob"), skip_duplicates=True)
    assert (Simple & "item_id=1").fetch1()["name"] == "Alice"

    # New PK — inserted
    Simple.insert1(dict(item_id=2, name="Bob"), skip_duplicates=True)
    assert len(Simple()) == 2


def test_skip_duplicates_composite_unique(schema_by_backend, db_creds_by_backend):
    """skip_duplicates=True with a composite secondary unique index."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific unique constraint enforcement")

    @schema_by_backend
    class Record(dj.Manual):
        definition = """
        record_id : int
        ---
        first_name : varchar(100)
        last_name : varchar(100)
        data : varchar(255)
        unique index (first_name, last_name)
        """

    Record.insert1(dict(record_id=1, first_name="Alice", last_name="Smith", data="v1"))

    # New PK but composite unique (first_name, last_name) conflicts
    with pytest.raises(DuplicateError):
        Record.insert1(
            dict(record_id=2, first_name="Alice", last_name="Smith", data="v2"),
            skip_duplicates=True,
        )


def test_skip_duplicates_batch_mixed(schema_by_backend, db_creds_by_backend):
    """Batch insert with skip_duplicates=True: PK duplicates skipped, unique conflicts raise."""
    if db_creds_by_backend["backend"] != "postgresql":
        pytest.skip("PostgreSQL-specific unique constraint enforcement")

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int
        ---
        email : varchar(100)
        unique index (email)
        """

    Item.insert1(dict(item_id=1, email="alice@example.com"))

    # Batch: row 2 is new (OK), row 1 is PK dup (skip), row 3 conflicts on email
    with pytest.raises(DuplicateError):
        Item.insert(
            [
                dict(item_id=2, email="bob@example.com"),
                dict(item_id=1, email="duplicate-pk@example.com"),  # PK dup — skipped
                dict(item_id=3, email="alice@example.com"),  # unique conflict — error
            ],
            skip_duplicates=True,
        )
