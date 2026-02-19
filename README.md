# DataJoint for Python

DataJoint is a framework for scientific data pipelines based on the **Relational Workflow Model** — a paradigm where your database schema is an executable specification of your workflow.

- **Tables represent workflow steps** — Each table is a step in your pipeline
- **Foreign keys encode dependencies** — Parent tables must be populated before child tables
- **Computations are declarative** — Define *what* to compute; DataJoint handles *when*
- **Results are immutable** — Full provenance and reproducibility

**Documentation:** https://docs.datajoint.com

> **📘 Upgrading from legacy DataJoint (pre-2.0)?**
> See the **[Migration Guide](https://docs.datajoint.com/how-to/migrate-to-v20/)** for a step-by-step upgrade path.

<table>
<tr>
  <td>PyPI</td>
  <td>
    <a href="https://pypi.org/project/datajoint/">
      <img src="https://img.shields.io/pypi/v/datajoint?color=blue" alt="pypi" />
    </a>
  </td>
  <td>Conda</td>
  <td>
    <a href="https://anaconda.org/conda-forge/datajoint">
      <img src="https://img.shields.io/conda/vn/conda-forge/datajoint?color=brightgreen" alt="conda" />
    </a>
  </td>
  <td>Tests</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml">
      <img src="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml/badge.svg" alt="tests" />
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
      <img src="https://img.shields.io/badge/DOI-10.1101/031658-blue.svg" alt="DOI" />
    </a>
  </td>
  <td>Coverage</td>
  <td>
    <a href="https://coveralls.io/github/datajoint/datajoint-python?branch=master">
      <img src="https://coveralls.io/repos/datajoint/datajoint-python/badge.svg?branch=master&service=github" alt="coverage" />
    </a>
  </td>
</tr>
</table>

## Installation

```bash
pip install datajoint
```

or with Conda:

```bash
conda install -c conda-forge datajoint
```

## Example Pipeline

![pipeline](https://raw.githubusercontent.com/datajoint/datajoint-python/master/images/pipeline.png)

[Yatsenko et al., bioRxiv 2021](https://doi.org/10.1101/2021.03.30.437358)

## Resources

- **[Documentation](https://docs.datajoint.com)** — Complete guides and reference
  - [Tutorials](https://docs.datajoint.com/tutorials/) — Learn by example
  - [How-To Guides](https://docs.datajoint.com/how-to/) — Task-oriented guides
  - [API Reference](https://docs.datajoint.com/api/) — Complete API documentation
  - [Migration Guide](https://docs.datajoint.com/how-to/migrate-to-v20/) — Upgrade from legacy versions
- **[DataJoint Elements](https://docs.datajoint.com/elements/)** — Example pipelines for neuroscience
- **[GitHub Discussions](https://github.com/datajoint/datajoint-python/discussions)** — Community support

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup and guidelines.
