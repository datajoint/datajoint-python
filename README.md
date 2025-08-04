# Welcome to DataJoint for Python!

<table>
<!-- Thank Zarr for providing badge insights! -->
<!-- https://github.com/zarr-developers/zarr-python/blob/main/README.md -->
<tr>
  <td>PyPI</td>
  <td>
    <a href="https://pypi.org/project/datajoint/">
      <img src="https://img.shields.io/pypi/v/datajoint?color=blue" alt="pypi release" />
    </a>
    <br>
    <a href="https://pypi.org/project/datajoint/">
      <img src="https://img.shields.io/pypi/dm/datajoint?color=yellow" alt="pypi downloads" />
    </a>
  </td>
</tr>
<tr>
  <td>Conda Forge</td>
  <td>
    <a href="https://anaconda.org/conda-forge/datajoint">
      <img src="https://img.shields.io/conda/vn/conda-forge/datajoint?color=brightgreen" alt="conda-forge release" />
    </a>
    <br>
    <a href="https://anaconda.org/conda-forge/datajoint">
      <img src="https://img.shields.io/conda/dn/conda-forge/datajoint?color=brightgreen" alt="conda-forge downloads" />
    </a>
  </td>
</tr>
<tr>
  <td>Since Release</td>
  <td>
    <a id="commit-since-release-link" href="https://github.com/datajoint/datajoint-python/compare/v0.14.6...master">
      <img id="commit-since-release-img" src="https://img.shields.io/github/commits-since/datajoint/datajoint-python/v0.14.6?color=red" alt="commit since last release" />
    </a>
  </td>
</tr>
<tr>
  <td>Test Status</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml">
      <img src="https://github.com/datajoint/datajoint-python/actions/workflows/test.yaml/badge.svg" alt="test status" />
    </a>
  </td>
</tr>
<tr>
  <td>Release Status</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/actions/workflows/post_draft_release_published.yaml">
      <img src="https://github.com/datajoint/datajoint-python/actions/workflows/post_draft_release_published.yaml/badge.svg" alt="release status" />
    </a>
  </td>
</tr>
<tr>
  <td>Doc Status</td>
  <td>
    <a href="https://docs.datajoint.com">
      <img src="https://github.com/datajoint/datajoint-python/actions/workflows/pages/pages-build-deployment/badge.svg" alt="doc status" />
    </a>
  </td>
</tr>
<tr>
  <td>Coverage</td>
  <td>
    <a href="https://coveralls.io/github/datajoint/datajoint-python?branch=master">
      <img src="https://coveralls.io/repos/datajoint/datajoint-python/badge.svg?branch=master&service=github"/ alt="coverage">
    </a>
  </td>
</tr>
<tr>
 <td>Developer Chat</td>
 <td>
  <a href="https://datajoint.slack.com/">
      <img src="https://img.shields.io/badge/slack-datajoint-purple.svg" alt="datajoint slack"/>
  </a>
 </td>
</tr>
<tr>
  <td>License</td>
  <td>
    <a href="https://github.com/datajoint/datajoint-python/blob/master/LICENSE.txt">
      <img src="https://img.shields.io/github/license/datajoint/datajoint-python" alt="LGPL-2.1" />
    </a>
  </td>
</tr>
<tr>
 <td>Citation</td>
 <td>
  <a href="https://doi.org/10.1101/031658">
   <img src="https://img.shields.io/badge/DOI-10.1101/bioRxiv.031658-B31B1B.svg" alt="bioRxiv">
  </a>
    <br>
    <a href="https://doi.org/10.5281/zenodo.6829062">
      <img src="https://zenodo.org/badge/DOI/10.5281/zenodo.6829062.svg" alt="zenodo">
  </a>
 </td>
</tr>

</table>

DataJoint for Python is a framework for scientific workflow management based on
relational principles. DataJoint is built on the foundation of the relational data
model and prescribes a consistent method for organizing, populating, computing, and
querying data.

DataJoint was initially developed in 2009 by Dimitri Yatsenko in Andreas Tolias' Lab at
Baylor College of Medicine for the distributed processing and management of large
volumes of data streaming from regular experiments. Starting in 2011, DataJoint has
been available as an open-source project adopted by other labs and improved through
contributions from several developers.
Presently, the primary developer of DataJoint open-source software is the company
DataJoint (<https://datajoint.com>).

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
