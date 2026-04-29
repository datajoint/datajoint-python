# Contributing to DataJoint

## Development Setup

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (Docker daemon must be running)
- [pixi](https://pixi.sh) (recommended) or Python 3.10+

### Quick Start with pixi

[pixi](https://pixi.sh) manages all dependencies including Python, graphviz, and test tools:

```bash
git clone https://github.com/datajoint/datajoint-python.git
cd datajoint-python

# Run tests (containers managed automatically)
pixi run test

# Run with coverage
pixi run test-cov

# Run pre-commit hooks
pixi run pre-commit run --all-files
```

### Alternative: Using pip

```bash
pip install -e ".[test]"
pytest tests/
```

---

## Running Tests

Tests use [testcontainers](https://testcontainers.com/) to automatically manage MySQL, PostgreSQL, and MinIO containers. No manual `docker-compose up` required.

Integration tests are **backend-parameterized** — tests using the `backend` fixture run automatically against both MySQL and PostgreSQL.

```bash
pixi run test                                    # All tests (both backends)
pixi run test-cov                                # With coverage
pixi run -e test pytest tests/unit/              # Unit tests only
pixi run -e test pytest tests/integration/test_blob.py -v  # Specific file
pixi run -e test pytest -m mysql                 # MySQL tests only
pixi run -e test pytest -m postgresql            # PostgreSQL tests only
```

**macOS Docker Desktop users:** If tests fail to connect:
```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

### PostgreSQL Backend

DataJoint supports MySQL 8.0.13+ and PostgreSQL 15+ as production database backends. To install the PostgreSQL driver:

```bash
pip install -e ".[postgres]"    # Installs psycopg2-binary
```

Tests automatically spin up both MySQL and PostgreSQL containers via testcontainers. Backend-parameterized tests (those using the `backend` fixture in `tests/conftest.py`) run against both backends to ensure feature parity.

### External Containers (for debugging)

```bash
# MySQL + MinIO
docker compose up -d db minio
DJ_USE_EXTERNAL_CONTAINERS=1 pixi run test
docker compose down

# MySQL + PostgreSQL + MinIO
docker compose up -d db postgres minio
DJ_USE_EXTERNAL_CONTAINERS=1 pixi run test
docker compose down
```

### Full Docker

```bash
docker compose --profile test up djtest --build
```

---

## Pre-commit Hooks

Hooks run automatically on `git commit`. All must pass.

```bash
pixi run pre-commit install              # First time only
pixi run pre-commit run --all-files      # Run manually
```

Hooks include: **ruff** (lint/format), **codespell**, YAML/JSON/TOML validation.

---

## Before Submitting a PR

1. `pixi run test` — All tests pass
2. `pixi run pre-commit run --all-files` — Hooks pass
3. `pixi run test-cov` — Coverage maintained

---

## Environment Variables

For `DJ_USE_EXTERNAL_CONTAINERS=1`:

### MySQL

| Variable | Default | Description |
|----------|---------|-------------|
| `DJ_HOST` | `localhost` | MySQL hostname |
| `DJ_PORT` | `3306` | MySQL port |
| `DJ_USER` | `root` | MySQL username |
| `DJ_PASS` | `password` | MySQL password |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `DJ_PG_HOST` | `localhost` | PostgreSQL hostname |
| `DJ_PG_PORT` | `5432` | PostgreSQL port |
| `DJ_PG_USER` | `postgres` | PostgreSQL username |
| `DJ_PG_PASS` | `password` | PostgreSQL password |

### Object Storage

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | `localhost:9000` | MinIO endpoint |

---

## Docstring Style

Use **NumPy-style** docstrings for all public APIs:

```python
def insert(self, rows, *, replace=False):
    """
    Insert rows into the table.

    Parameters
    ----------
    rows : iterable
        Rows to insert. Each row can be a dict, numpy record, or sequence.
    replace : bool, optional
        If True, replace existing rows with matching keys. Default is False.

    Returns
    -------
    None

    Raises
    ------
    DuplicateError
        When inserting a duplicate key without ``replace=True``.

    Examples
    --------
    >>> Mouse.insert1({"mouse_id": 1, "dob": "2024-01-15"})
    """
```

### Section Order

1. Short summary (one line, imperative mood)
2. Extended description
3. Parameters
4. Returns / Yields
5. Raises
6. Examples (strongly encouraged)
7. See Also

### Style Rules

- **Do:** Imperative mood ("Insert rows" not "Inserts rows")
- **Do:** Include examples for public APIs
- **Don't:** Document private methods extensively
- **Don't:** Repeat function signature in description

See [NumPy Docstring Guide](https://numpydoc.readthedocs.io/en/latest/format.html) for full reference.
