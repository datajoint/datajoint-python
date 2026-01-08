# DataJoint for Python

DataJoint is a framework for scientific data pipelines that introduces the **Relational Workflow Model**—a paradigm where your database schema is an executable specification of your workflow.

Traditional databases store data but don't understand how it was computed. DataJoint extends relational databases with native workflow semantics:

- **Tables represent workflow steps** — Each table is a step in your pipeline where entities are created
- **Foreign keys encode dependencies** — Parent tables must be populated before child tables
- **Computations are declarative** — Define *what* to compute; DataJoint determines *when* and tracks *what's done*
- **Results are immutable** — Computed results preserve full provenance and reproducibility

### Object-Augmented Schemas

Scientific data includes both structured metadata and large data objects (time series, images, movies, neural recordings, gene sequences). DataJoint solves this with **Object-Augmented Schemas (OAS)**—a unified architecture where relational tables and object storage are managed as one system with identical guarantees for integrity, transactions, and lifecycle.

### DataJoint 2.0

**DataJoint 2.0** solidifies these core concepts with a modernized API, improved type system, and enhanced object storage integration. Existing users can refer to the [Migration Guide](https://docs.datajoint.com/migration/) for upgrading from earlier versions.

**Documentation:** https://docs.datajoint.com

<table>
<tr>
  <td>PyPI</td>
  <td>
    <a href="https://pypi.org/project/datajoint/">
      <img src="https://img.shields.io/pypi/v/datajoint?color=blue" alt="pypi release" />
    </a>
  </td>
  <td>Conda</td>
  <td>
    <a href="https://anaconda.org/conda-forge/datajoint">
      <img src="https://img.shields.io/conda/vn/conda-forge/datajoint?color=brightgreen" alt="conda-forge release" />
    </a>
  </td>
</tr>
<tr>
  <td>Tests</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml">
      <img src="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml/badge.svg" alt="test status" />
    </a>
  </td>
  <td>Coverage</td>
  <td>
    <a href="https://coveralls.io/github/datajoint/datajoint-python?branch=master">
      <img src="https://coveralls.io/repos/datajoint/datajoint-python/badge.svg?branch=master&service=github" alt="coverage">
    </a>
  </td>
</tr>
<tr>
  <td>License</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/blob/master/LICENSE">
      <img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" alt="Apache-2.0" />
    </a>
  </td>
  <td>Citation</td>
  <td>
    <a href="https://doi.org/10.1101/031658">
      <img src="https://img.shields.io/badge/DOI-10.1101/031658-blue.svg" alt="DOI">
    </a>
  </td>
</tr>
</table>

## Data Pipeline Example

![pipeline](https://raw.githubusercontent.com/datajoint/datajoint-python/master/images/pipeline.png)

[Yatsenko et al., bioRxiv 2021](https://doi.org/10.1101/2021.03.30.437358)

## Getting Started

- Install with Conda

     ```bash
     conda install -c conda-forge datajoint
     ```

- Install with pip

     ```bash
     pip install datajoint
     ```

- [Documentation & Tutorials](https://docs.datajoint.com/core/datajoint-python/)

- [Interactive Tutorials](https://github.com/datajoint/datajoint-tutorials) on GitHub Codespaces

- [DataJoint Elements](https://docs.datajoint.com/elements/) - Catalog of example pipelines for neuroscience experiments

- Contribute
  - [Contribution Guidelines](https://docs.datajoint.com/about/contribute/)

  - [Developer Guide](https://docs.datajoint.com/core/datajoint-python/latest/develop/)

## Developer Guide

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (Docker daemon must be running)
- [pixi](https://pixi.sh) (recommended) or Python 3.10+

### Quick Start with pixi (Recommended)

[pixi](https://pixi.sh) manages all dependencies including Python, graphviz, and test tools:

```bash
# Clone the repo
git clone https://github.com/datajoint/datajoint-python.git
cd datajoint-python

# Install dependencies and run tests (containers managed by testcontainers)
pixi run test

# Run with coverage
pixi run test-cov

# Run pre-commit hooks
pixi run pre-commit run --all-files
```

### Running Tests

Tests use [testcontainers](https://testcontainers.com/) to automatically manage MySQL and MinIO containers.
**No manual `docker-compose up` required** - containers start when tests run and stop afterward.

```bash
# Run all tests (recommended)
pixi run test

# Run with coverage report
pixi run test-cov

# Run only unit tests (no containers needed)
pixi run -e test pytest tests/unit/

# Run specific test file
pixi run -e test pytest tests/integration/test_blob.py -v
```

**macOS Docker Desktop users:** If tests fail to connect to Docker, set `DOCKER_HOST`:
```bash
export DOCKER_HOST=unix://$HOME/.docker/run/docker.sock
```

### Alternative: Using pip

If you prefer pip over pixi:

```bash
pip install -e ".[test]"
pytest tests/
```

### Alternative: External Containers

For development/debugging, you may prefer persistent containers that survive test runs:

```bash
# Start containers manually
docker compose up -d db minio

# Run tests using external containers
DJ_USE_EXTERNAL_CONTAINERS=1 pixi run test
# Or with pip: DJ_USE_EXTERNAL_CONTAINERS=1 pytest tests/

# Stop containers when done
docker compose down
```

### Alternative: Full Docker

Run tests entirely in Docker (no local Python needed):

```bash
docker compose --profile test up djtest --build
```

### Pre-commit Hooks

Pre-commit hooks run automatically on `git commit` to check code quality.
**All hooks must pass before committing.**

```bash
# Install hooks (first time only)
pixi run pre-commit install
# Or with pip: pip install pre-commit && pre-commit install

# Run all checks manually
pixi run pre-commit run --all-files

# Run specific hook
pixi run pre-commit run ruff --all-files
```

Hooks include:
- **ruff**: Python linting and formatting
- **codespell**: Spell checking
- **YAML/JSON/TOML validation**
- **Large file detection**

### Before Submitting a PR

1. **Run all tests**: `pixi run test`
2. **Run pre-commit**: `pixi run pre-commit run --all-files`
3. **Check coverage**: `pixi run test-cov`

### Environment Variables

For external container mode (`DJ_USE_EXTERNAL_CONTAINERS=1`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DJ_HOST` | `localhost` | MySQL hostname |
| `DJ_PORT` | `3306` | MySQL port |
| `DJ_USER` | `root` | MySQL username |
| `DJ_PASS` | `password` | MySQL password |
| `S3_ENDPOINT` | `localhost:9000` | MinIO endpoint |

For Docker-based testing (devcontainer, djtest), set `DJ_HOST=db` and `S3_ENDPOINT=minio:9000`.
