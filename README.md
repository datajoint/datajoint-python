[![DOI](https://zenodo.org/badge/16774/datajoint/datajoint-python.svg)](https://zenodo.org/badge/latestdoi/16774/datajoint/datajoint-python)
[![Build Status](https://travis-ci.org/datajoint/datajoint-python.svg?branch=master)](https://travis-ci.org/datajoint/datajoint-python)
[![Coverage Status](https://coveralls.io/repos/datajoint/datajoint-python/badge.svg?branch=master&service=github)](https://coveralls.io/github/datajoint/datajoint-python?branch=master)
[![PyPI version](https://badge.fury.io/py/datajoint.svg)](http://badge.fury.io/py/datajoint)
[![Requirements Status](https://requires.io/github/datajoint/datajoint-python/requirements.svg?branch=master)](https://requires.io/github/datajoint/datajoint-python/requirements/?branch=master)
[![Slack](https://img.shields.io/badge/slack-chat-green.svg)](https://datajoint.slack.com/)

# Welcome to DataJoint for Python!
DataJoint for Python is a framework for scientific workflow management based on relational principles. DataJoint is built on the foundation of the relational data model and prescribes a consistent method for organizing, populating, computing, and querying data.

DataJoint was initially developed in 2009 by Dimitri Yatsenko in Andreas Tolias' Lab at Baylor College of Medicine for the distributed processing and management of large volumes of data streaming from regular experiments. Starting in 2011, DataJoint has been available as an open-source project adopted by other labs and improved through contributions from several developers.
Presently, the primary developer of DataJoint open-source software is the company DataJoint (https://datajoint.com). Related resources are listed at https://datajoint.org.

## Installation
```
pip3 install datajoint
```

If you already have an older version of DataJoint installed using `pip`, upgrade with
```bash
pip3 install --upgrade datajoint
```

## Documentation and Tutorials

* https://datajoint.org  -- start page
* https://docs.datajoint.org -- up-to-date documentation
* https://tutorials.datajoint.io -- step-by-step tutorials
* https://elements.datajoint.org -- catalog of example pipelines
* https://codebook.datajoint.io -- interactive online tutorials

## Citation
+ If your work uses DataJoint for Python, please cite the following Research Resource Identifier (RRID) and manuscript.

+ DataJoint ([RRID:SCR_014543](https://scicrunch.org/resolver/SCR_014543)) - DataJoint for Python (version `<Enter version number>`)

+ Yatsenko D, Reimer J, Ecker AS, Walker EY, Sinz F, Berens P, Hoenselaar A, Cotton RJ, Siapas AS, Tolias AS. DataJoint: managing big scientific data using MATLAB or Python. bioRxiv. 2015 Jan 1:031658. doi: https://doi.org/10.1101/031658

## Python Native Blobs
<details>
<summary>Click to expand details</summary>

DataJoint 0.12 adds full support for all native python data types in blobs: tuples, lists, sets, dicts, strings, bytes, `None`, and all their recursive combinations.
The new blobs are a superset of the old functionality and are fully backward compatible.
In previous versions, only MATLAB-style numerical arrays were fully supported.
Some Python datatypes such as dicts were coerced into numpy recarrays and then fetched as such.

However, since some Python types were coerced into MATLAB types, old blobs and new blobs may now be fetched as different types of objects even if they were inserted the same way. 
For example, new `dict` objects will be returned as `dict` while the same types of objects inserted with `datajoint 0.11` will be recarrays.

Since this is a big change, we chose to temporarily disable this feature by default in DataJoint for Python 0.12.x, allowing users to adjust their code if necessary. 
From 13.x, the flag will default to True (on), and will ultimately be removed when corresponding decode support for the new format is added to datajoint-matlab (see: datajoint-matlab #222, datajoint-python #765).

The flag is configured by setting the `enable_python_native_blobs` flag in `dj.config`. 

```python
import datajoint as dj
dj.config["enable_python_native_blobs"] = True
```

You can safely enable this setting if both of the following are true:

  * The only kinds of blobs your pipeline have inserted previously were numerical arrays.
  * You do not need to share blob data between Python and MATLAB.

Otherwise, read the following explanation.

DataJoint v0.12 expands DataJoint's blob serialization mechanism with
improved support for complex native python datatypes, such as dictionaries
and lists of strings.

Prior to DataJoint v0.12, certain python native datatypes such as
dictionaries were 'squashed' into numpy structured arrays when saved into
blob attributes. This facilitated easier data sharing between MATLAB
and Python for certain record types. However, this created a discrepancy
between insert and fetch datatypes which could cause problems in other
portions of users pipelines.

DataJoint v0.12, removes the squashing behavior, instead encoding native python datatypes in blobs directly. 
However, this change creates a compatibility problem for pipelines 
which previously relied on the type squashing behavior since records 
saved via the old squashing format will continue to fetch
as structured arrays, whereas new record inserted in DataJoint 0.12 with
`enable_python_native_blobs` would result in records returned as the
appropriate native python type (dict, etc).  
Furthermore, DataJoint for MATLAB does not yet support unpacking native Python datatypes.

With `dj.config["enable_python_native_blobs"]` set to `False`, 
any attempt to insert any datatype other than a numpy array will result in an exception.
This is meant to get users to read this message in order to allow proper testing
and migration of pre-0.12 pipelines to 0.12 in a safe manner.

The exact process to update a specific pipeline will vary depending on
the situation, but generally the following strategies may apply:

  * Altering code to directly store numpy structured arrays or plain
    multidimensional arrays. This strategy is likely best one for those 
    tables requiring compatibility with MATLAB.
  * Adjust code to deal with both structured array and native fetched data
    for those tables that are populated with `dict`s in blobs in pre-0.12 version. 
    In this case, insert logic is not adjusted, but downstream consumers
    are adjusted to handle records saved under the old and new schemes.
  * Migrate data into a fresh schema, fetching the old data, converting blobs to 
    a uniform data type and re-inserting.
  * Drop/Recompute imported/computed tables to ensure they are in the new
    format.

As always, be sure that your data is safely backed up before modifying any
important DataJoint schema or records.

</details>

### API docs

The API documentation can be built using sphinx by running

``` bash
pip install sphinx sphinx_rtd_theme 
(cd docs-api/sphinx && make html)
```

Generated docs are written to `docs-api/docs/html/index.html`.
More details in [docs-api/README.md](docs-api/README.md).

## Running Tests Locally
<details>
<summary>Click to expand details</summary>

* Create an `.env` with desired development environment values e.g.
``` sh
PY_VER=3.7
ALPINE_VER=3.10
MYSQL_VER=5.7
MINIO_VER=RELEASE.2021-09-03T03-56-13Z
UID=1000
GID=1000
```
* `cp local-docker-compose.yml docker-compose.yml`
* `docker-compose up -d` (Note configured `JUPYTER_PASSWORD`)
* Select a means of running Tests e.g. Docker Terminal, or Local Terminal (see bottom)
* Add entry in `/etc/hosts` for `127.0.0.1 fakeservices.datajoint.io`
* Run desired tests. Some examples are as follows:

| Use Case                     | Shell Code                                                                    |
| ---------------------------- | ------------------------------------------------------------------------------ |
| Run all tests                | `nosetests -vsw tests --with-coverage --cover-package=datajoint`                                                              |
| Run one specific class test       | `nosetests -vs --tests=tests.test_fetch:TestFetch.test_getattribute_for_fetch1`                                                           |
| Run one specific basic test        | `nosetests -vs --tests=tests.test_external_class:test_insert_and_fetch`                                   |


### Launch Docker Terminal
* Shell into `datajoint-python_app_1` i.e. `docker exec -it datajoint-python_app_1 sh`


### Launch Local Terminal
* See `datajoint-python_app` environment variables in `local-docker-compose.yml`
* Launch local terminal
* `export` environment variables in shell
* Add entry in `/etc/hosts` for `127.0.0.1 fakeservices.datajoint.io`


### Launch Jupyter Notebook for Interactive Use
* Navigate to `localhost:8888`
* Input Jupyter password
* Launch a notebook i.e. `New > Python 3`

</details>