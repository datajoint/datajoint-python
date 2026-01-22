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

Tests use [testcontainers](https://testcontainers.com/) to automatically manage MySQL and MinIO containers. No manual `docker-compose up` required.

```bash
pixi run test                                    # All tests
pixi run test-cov                                # With coverage
pixi run -e test pytest tests/unit/              # Unit tests only
pixi run -e test pytest tests/integration/test_blob.py -v  # Specific file
```

**macOS Docker Desktop users:** If tests fail to connect:
```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

### External Containers (for debugging)

```bash
docker compose up -d db minio
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

| Variable | Default | Description |
|----------|---------|-------------|
| `DJ_HOST` | `localhost` | MySQL hostname |
| `DJ_PORT` | `3306` | MySQL port |
| `DJ_USER` | `root` | MySQL username |
| `DJ_PASS` | `password` | MySQL password |
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
