# Contributing Guide

## Quick Start

```bash
# Clone the repository
git clone https://github.com/datajoint/datajoint-python.git
cd datajoint-python

# Create virtual environment (Python 3.10+)
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install with development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Run tests
pytest tests
```

## Development Environment

### Local Setup

Requirements:

- Python 3.10 or higher
- MySQL 8.0+ or Docker (for running tests)

The `[dev]` extras install all development tools: pytest, pre-commit, black, ruff, and documentation builders.

### Using Docker for Database

Tests require a MySQL database. Start one with Docker:

```bash
docker compose up -d db
```

Configure connection (or set environment variables):

```bash
export DJ_HOST=localhost
export DJ_USER=root
export DJ_PASS=password
```

### Alternative: GitHub Codespaces

For a pre-configured environment, use [GitHub Codespaces](https://github.com/features/codespaces):

1. Fork the repository
2. Click "Create codespace on master"
3. Wait for environment to build (~6 minutes first time, ~2 minutes from cache)

## Code Quality

### Pre-commit Hooks

Pre-commit runs automatically on `git commit`. To run manually:

```bash
pre-commit run --all-files
```

Hooks include:

- **ruff** — Linting and import sorting
- **black** — Code formatting
- **mypy** — Type checking (optional)

### Running Tests

```bash
# Full test suite with coverage
pytest -sv --cov=datajoint tests

# Single test file
pytest tests/test_connection.py

# Single test function
pytest tests/test_connection.py::test_dj_conn -v
```

## Submitting Changes

1. Create a feature branch from `master`
2. Make your changes
3. Ensure tests pass and pre-commit is clean
4. Submit a pull request

PRs trigger CI checks automatically. All checks must pass before merge.

## Documentation

Docstrings use NumPy style. See [DOCSTRING_STYLE.md](https://github.com/datajoint/datajoint-python/blob/master/DOCSTRING_STYLE.md) for guidelines.

User documentation is maintained at [docs.datajoint.com](https://docs.datajoint.com).
