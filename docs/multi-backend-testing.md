# Multi-Backend Integration Testing Design

## Current State

DataJoint already has excellent test infrastructure:
- ✅ Testcontainers support (automatic container management)
- ✅ Docker Compose support (DJ_USE_EXTERNAL_CONTAINERS=1)
- ✅ Clean fixture-based credential management
- ✅ Automatic test marking based on fixture usage

## Goal

Run integration tests against both MySQL and PostgreSQL backends to verify:
1. DDL generation is correct for both backends
2. SQL queries work identically
3. Data types map correctly
4. Backward compatibility with MySQL is preserved

## Architecture: Hybrid Testcontainers + Docker Compose

### Strategy

**Support THREE modes**:

1. **Auto mode (default)**: Testcontainers manages both MySQL and PostgreSQL
   ```bash
   pytest tests/
   ```

2. **Docker Compose mode**: External containers for development/debugging
   ```bash
   docker compose up -d
   DJ_USE_EXTERNAL_CONTAINERS=1 pytest tests/
   ```

3. **Single backend mode**: Test only one backend (faster CI)
   ```bash
   pytest -m "mysql"           # MySQL only
   pytest -m "postgresql"      # PostgreSQL only
   pytest -m "not postgresql"  # Skip PostgreSQL tests
   ```

### Benefits

- **Developers**: Run all tests locally with zero setup (`pytest`)
- **CI**: Parallel jobs for MySQL and PostgreSQL (faster feedback)
- **Debugging**: Use docker-compose for persistent containers
- **Flexibility**: Choose backend granularity per test

---

## Implementation Plan

### Phase 1: Update docker-compose.yaml

Add PostgreSQL service alongside MySQL:

```yaml
services:
  db:
    # Existing MySQL service (unchanged)
    image: datajoint/mysql:${MYSQL_VER:-8.0}
    # ... existing config

  postgres:
    image: postgres:${POSTGRES_VER:-15}
    environment:
      - POSTGRES_PASSWORD=${PG_PASS:-password}
      - POSTGRES_USER=${PG_USER:-postgres}
      - POSTGRES_DB=${PG_DB:-test}
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres"]
      timeout: 30s
      retries: 5
      interval: 15s

  minio:
    # Existing MinIO service (unchanged)
    # ...

  app:
    # Existing app service, add PG env vars
    environment:
      # ... existing MySQL env vars
      - DJ_PG_HOST=postgres
      - DJ_PG_USER=postgres
      - DJ_PG_PASS=password
      - DJ_PG_PORT=5432
    depends_on:
      db:
        condition: service_healthy
      postgres:
        condition: service_healthy
      minio:
        condition: service_healthy
```

### Phase 2: Update tests/conftest.py

Add PostgreSQL container and fixtures:

```python
# =============================================================================
# Container Fixtures - MySQL and PostgreSQL
# =============================================================================

@pytest.fixture(scope="session")
def postgres_container():
    """Start PostgreSQL container for the test session (or use external)."""
    if USE_EXTERNAL_CONTAINERS:
        logger.info("Using external PostgreSQL container")
        yield None
        return

    from testcontainers.postgres import PostgresContainer

    container = PostgresContainer(
        image="postgres:15",
        username="postgres",
        password="password",
        dbname="test",
    )
    container.start()

    host = container.get_container_host_ip()
    port = container.get_exposed_port(5432)
    logger.info(f"PostgreSQL container started at {host}:{port}")

    yield container

    container.stop()
    logger.info("PostgreSQL container stopped")


# =============================================================================
# Backend-Parameterized Fixtures
# =============================================================================

@pytest.fixture(scope="session", params=["mysql", "postgresql"])
def backend(request):
    """Parameterize tests to run against both backends."""
    return request.param


@pytest.fixture(scope="session")
def db_creds_by_backend(backend, mysql_container, postgres_container):
    """Get root database credentials for the specified backend."""
    if backend == "mysql":
        if mysql_container is not None:
            host = mysql_container.get_container_host_ip()
            port = mysql_container.get_exposed_port(3306)
            return {
                "backend": "mysql",
                "host": f"{host}:{port}",
                "user": "root",
                "password": "password",
            }
        else:
            # External MySQL container
            host = os.environ.get("DJ_HOST", "localhost")
            port = os.environ.get("DJ_PORT", "3306")
            return {
                "backend": "mysql",
                "host": f"{host}:{port}" if port else host,
                "user": os.environ.get("DJ_USER", "root"),
                "password": os.environ.get("DJ_PASS", "password"),
            }

    elif backend == "postgresql":
        if postgres_container is not None:
            host = postgres_container.get_container_host_ip()
            port = postgres_container.get_exposed_port(5432)
            return {
                "backend": "postgresql",
                "host": f"{host}:{port}",
                "user": "postgres",
                "password": "password",
            }
        else:
            # External PostgreSQL container
            host = os.environ.get("DJ_PG_HOST", "localhost")
            port = os.environ.get("DJ_PG_PORT", "5432")
            return {
                "backend": "postgresql",
                "host": f"{host}:{port}" if port else host,
                "user": os.environ.get("DJ_PG_USER", "postgres"),
                "password": os.environ.get("DJ_PG_PASS", "password"),
            }


@pytest.fixture(scope="session")
def connection_root_by_backend(db_creds_by_backend):
    """Create connection for the specified backend."""
    import datajoint as dj

    # Configure backend
    dj.config["database.backend"] = db_creds_by_backend["backend"]

    # Parse host:port
    host_port = db_creds_by_backend["host"]
    if ":" in host_port:
        host, port = host_port.rsplit(":", 1)
    else:
        host = host_port
        port = "3306" if db_creds_by_backend["backend"] == "mysql" else "5432"

    dj.config["database.host"] = host
    dj.config["database.port"] = int(port)
    dj.config["safemode"] = False

    connection = dj.Connection(
        host=host_port,
        user=db_creds_by_backend["user"],
        password=db_creds_by_backend["password"],
    )

    yield connection
    connection.close()
```

### Phase 3: Backend-Specific Test Markers

Add pytest markers for backend-specific tests:

```python
# In pytest.ini or pyproject.toml
[tool.pytest.ini_options]
markers = [
    "requires_mysql: tests that require MySQL database",
    "requires_minio: tests that require MinIO/S3",
    "mysql: tests that run on MySQL backend",
    "postgresql: tests that run on PostgreSQL backend",
    "backend_agnostic: tests that should pass on all backends (default)",
]
```

Update `tests/conftest.py` to auto-mark backend-specific tests:

```python
def pytest_collection_modifyitems(config, items):
    """Auto-mark integration tests based on their fixtures."""
    # Existing MySQL/MinIO marking logic...

    # Auto-mark backend-parameterized tests
    for item in items:
        try:
            fixturenames = set(item.fixturenames)
        except AttributeError:
            continue

        # If test uses backend-parameterized fixture, add backend markers
        if "backend" in fixturenames or "connection_root_by_backend" in fixturenames:
            # Test will run for both backends
            item.add_marker(pytest.mark.mysql)
            item.add_marker(pytest.mark.postgresql)
            item.add_marker(pytest.mark.backend_agnostic)
```

### Phase 4: Write Multi-Backend Tests

Create `tests/integration/test_multi_backend.py`:

```python
"""
Integration tests that verify backend-agnostic behavior.

These tests run against both MySQL and PostgreSQL to ensure:
1. DDL generation is correct
2. SQL queries work identically
3. Data types map correctly
"""
import pytest
import datajoint as dj


@pytest.mark.backend_agnostic
def test_simple_table_declaration(connection_root_by_backend, backend):
    """Test that simple tables can be declared on both backends."""
    schema = dj.Schema(
        f"test_{backend}_simple",
        connection=connection_root_by_backend,
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
    User.insert1((1, "alice", "2025-01-01"))
    data = User.fetch1()

    assert data["user_id"] == 1
    assert data["username"] == "alice"

    # Cleanup
    schema.drop()


@pytest.mark.backend_agnostic
def test_foreign_keys(connection_root_by_backend, backend):
    """Test foreign key declarations work on both backends."""
    schema = dj.Schema(
        f"test_{backend}_fk",
        connection=connection_root_by_backend,
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

    # Verify FK constraint
    with pytest.raises(dj.DataJointError):
        Observation.insert1((999, 1, "Invalid"))  # FK to non-existent animal

    schema.drop()


@pytest.mark.backend_agnostic
def test_blob_types(connection_root_by_backend, backend):
    """Test that blob types work on both backends."""
    schema = dj.Schema(
        f"test_{backend}_blob",
        connection=connection_root_by_backend,
    )

    @schema
    class BlobTest(dj.Manual):
        definition = """
        id : int
        ---
        data : longblob
        """

    import numpy as np

    # Insert numpy array
    arr = np.random.rand(100, 100)
    BlobTest.insert1((1, arr))

    # Fetch and verify
    fetched = (BlobTest & {"id": 1}).fetch1("data")
    np.testing.assert_array_equal(arr, fetched)

    schema.drop()


@pytest.mark.backend_agnostic
def test_datetime_precision(connection_root_by_backend, backend):
    """Test datetime precision on both backends."""
    schema = dj.Schema(
        f"test_{backend}_datetime",
        connection=connection_root_by_backend,
    )

    @schema
    class TimeTest(dj.Manual):
        definition = """
        id : int
        ---
        timestamp : datetime(3)  # millisecond precision
        """

    from datetime import datetime

    ts = datetime(2025, 1, 17, 12, 30, 45, 123000)
    TimeTest.insert1((1, ts))

    fetched = (TimeTest & {"id": 1}).fetch1("timestamp")

    # Both backends should preserve millisecond precision
    assert fetched.microsecond == 123000

    schema.drop()


@pytest.mark.backend_agnostic
def test_table_comments(connection_root_by_backend, backend):
    """Test that table comments are preserved on both backends."""
    schema = dj.Schema(
        f"test_{backend}_comments",
        connection=connection_root_by_backend,
    )

    @schema
    class Commented(dj.Manual):
        definition = """
        # This is a test table
        id : int  # primary key
        ---
        value : varchar(255)  # some value
        """

    # Fetch table comment from information_schema
    adapter = connection_root_by_backend.adapter

    if backend == "mysql":
        query = """
            SELECT TABLE_COMMENT
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'commented'
        """
    else:  # postgresql
        query = """
            SELECT obj_description(oid)
            FROM pg_class
            WHERE relname = 'commented'
        """

    comment = connection_root_by_backend.query(query, args=(schema.database,)).fetchone()[0]
    assert "This is a test table" in comment

    schema.drop()


@pytest.mark.backend_agnostic
def test_alter_table(connection_root_by_backend, backend):
    """Test ALTER TABLE operations work on both backends."""
    schema = dj.Schema(
        f"test_{backend}_alter",
        connection=connection_root_by_backend,
    )

    @schema
    class AlterTest(dj.Manual):
        definition = """
        id : int
        ---
        field1 : varchar(255)
        """

    AlterTest.insert1((1, "original"))

    # Modify definition (add field)
    AlterTest.definition = """
        id : int
        ---
        field1 : varchar(255)
        field2 : int
        """

    AlterTest.alter(prompt=False)

    # Verify new field exists
    AlterTest.update1((1, "updated", 42))
    data = AlterTest.fetch1()
    assert data["field2"] == 42

    schema.drop()


# =============================================================================
# Backend-Specific Tests (MySQL only)
# =============================================================================

@pytest.mark.mysql
def test_mysql_specific_syntax(connection_root):
    """Test MySQL-specific features that may not exist in PostgreSQL."""
    # Example: MySQL fulltext indexes, specific storage engines, etc.
    pass


# =============================================================================
# Backend-Specific Tests (PostgreSQL only)
# =============================================================================

@pytest.mark.postgresql
def test_postgresql_specific_syntax(connection_root_by_backend):
    """Test PostgreSQL-specific features."""
    if connection_root_by_backend.adapter.backend != "postgresql":
        pytest.skip("PostgreSQL-only test")

    # Example: PostgreSQL arrays, JSON operators, etc.
    pass
```

### Phase 5: CI/CD Configuration

Update GitHub Actions to run tests in parallel:

```yaml
# .github/workflows/test.yml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    name: Unit Tests (No Database)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -e ".[test]"
      - run: pytest -m "not requires_mysql" --cov

  integration-mysql:
    name: Integration Tests (MySQL)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -e ".[test]"
      # Testcontainers automatically manages MySQL
      - run: pytest -m "mysql" --cov

  integration-postgresql:
    name: Integration Tests (PostgreSQL)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -e ".[test]"
      # Testcontainers automatically manages PostgreSQL
      - run: pytest -m "postgresql" --cov

  integration-all:
    name: Integration Tests (Both Backends)
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -e ".[test]"
      # Run all backend-agnostic tests against both backends
      - run: pytest -m "backend_agnostic" --cov
```

---

## Usage Examples

### Developer Workflow

```bash
# Quick: Run all tests with auto-managed containers
pytest tests/

# Fast: Run only unit tests (no Docker)
pytest -m "not requires_mysql"

# Backend-specific: Test only MySQL
pytest -m "mysql"

# Backend-specific: Test only PostgreSQL
pytest -m "postgresql"

# Development: Use docker-compose for persistent containers
docker compose up -d
DJ_USE_EXTERNAL_CONTAINERS=1 pytest tests/
docker compose down
```

### CI Workflow

```bash
# Parallel jobs for speed:
# Job 1: Unit tests (fast, no Docker)
pytest -m "not requires_mysql"

# Job 2: MySQL integration tests
pytest -m "mysql"

# Job 3: PostgreSQL integration tests
pytest -m "postgresql"
```

---

## Testing Strategy

### What to Test

1. **Backend-Agnostic Tests** (run on both):
   - Table declaration (simple, with FKs, with indexes)
   - Data types (int, varchar, datetime, blob, etc.)
   - CRUD operations (insert, update, delete, fetch)
   - Queries (restrictions, projections, joins, aggregations)
   - Foreign key constraints
   - Transactions
   - Schema management (drop, rename)
   - Table alterations (add/drop/rename columns)

2. **Backend-Specific Tests**:
   - MySQL: Fulltext indexes, MyISAM features, MySQL-specific types
   - PostgreSQL: Arrays, JSONB operators, PostgreSQL-specific types

3. **Migration Tests**:
   - Verify MySQL DDL hasn't changed (byte-for-byte comparison)
   - Verify PostgreSQL generates valid DDL

### What NOT to Test

- Performance benchmarks (separate suite)
- Specific DBMS implementation details
- Vendor-specific extensions (unless critical to DataJoint)

---

## File Structure

```
tests/
├── conftest.py                    # Updated with PostgreSQL fixtures
├── unit/                          # No database required
│   ├── test_adapters.py          # Adapter unit tests (existing)
│   └── test_*.py
├── integration/
│   ├── test_multi_backend.py     # NEW: Backend-agnostic tests
│   ├── test_declare.py           # Update to use backend fixture
│   ├── test_alter.py             # Update to use backend fixture
│   ├── test_lineage.py           # Update to use backend fixture
│   ├── test_mysql_specific.py    # NEW: MySQL-only tests
│   └── test_postgres_specific.py # NEW: PostgreSQL-only tests
└── ...

docker-compose.yaml                # Updated with PostgreSQL service
```

---

## Migration Path

### Phase 1: Infrastructure (Week 1)
- ✅ Update docker-compose.yaml with PostgreSQL service
- ✅ Add postgres_container fixture to conftest.py
- ✅ Add backend parameterization fixtures
- ✅ Add pytest markers for backend tests
- ✅ Update CI configuration

### Phase 2: Convert Existing Tests (Week 2)
- Update test_declare.py to use backend fixture
- Update test_alter.py to use backend fixture
- Update test_lineage.py to use backend fixture
- Identify MySQL-specific tests and mark them

### Phase 3: New Multi-Backend Tests (Week 3)
- Write backend-agnostic test suite
- Test all core DataJoint operations
- Verify type mappings
- Test transaction behavior

### Phase 4: Validation (Week 4)
- Run full test suite against both backends
- Fix any backend-specific issues
- Document known differences
- Update contributing guide

---

## Benefits

✅ **Zero-config testing**: `pytest` just works
✅ **Fast CI**: Parallel backend testing
✅ **Flexible debugging**: Use docker-compose when needed
✅ **Selective testing**: Run only MySQL or PostgreSQL tests
✅ **Backward compatible**: Existing tests continue to work
✅ **Comprehensive coverage**: All operations tested on both backends

---

## Next Steps

1. Implement Phase 1 (infrastructure updates)
2. Run existing tests against PostgreSQL to identify failures
3. Fix adapter bugs discovered by tests
4. Gradually convert existing tests to backend-agnostic
5. Add new backend-specific tests where appropriate
