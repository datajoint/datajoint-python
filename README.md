[![DOI](https://zenodo.org/badge/16774/datajoint/datajoint-python.svg)](https://zenodo.org/badge/latestdoi/16774/datajoint/datajoint-python)
[![Build Status](https://travis-ci.org/datajoint/datajoint-python.svg?branch=master)](https://travis-ci.org/datajoint/datajoint-python)
[![Coverage Status](https://coveralls.io/repos/datajoint/datajoint-python/badge.svg?branch=master&service=github)](https://coveralls.io/github/datajoint/datajoint-python?branch=master)
[![PyPI version](https://badge.fury.io/py/datajoint.svg)](http://badge.fury.io/py/datajoint)
[![Requirements Status](https://requires.io/github/datajoint/datajoint-python/requirements.svg?branch=master)](https://requires.io/github/datajoint/datajoint-python/requirements/?branch=master)
[![Join the chat at https://gitter.im/datajoint/datajoint-python](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/datajoint/datajoint-python?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

# Welcome to DataJoint for Python!
DataJoint for Python is a framework for scientific workflow management based on relational principles. DataJoint is built on the foundation of the relational data model and prescribes a consistent method for organizing, populating, computing, and querying data.

DataJoint was initially developed in 2009 by Dimitri Yatsenko in Andreas Tolias' Lab for the distributed processing and management of large volumes of data streaming from regular experiments. Starting in 2011, DataJoint has been available as an open-source project adopted by other labs and improved through contributions from several developers.

Vathes LLC supports DataJoint for Python as an open-source project and everyone is welcome to contribute.
Its DataJoint Neuro (https://djneuro.io) business provides support to neuroscience labs for developing and executing custom data pipelines.

## Installation
```
pip3 install datajoint
```

If you already have an older version of DataJoint installed using `pip`, upgrade with
```bash
pip3 install --upgrade datajoint
```
## Python Native Blobs

DataJoint 0.12 adds full support for all native python data types in blobs: tuples, lists, sets, dicts, strings, bytes, `None`, and all their recursive combinations.
The new blobs are a superset of the old functionality and are fully backward compatible.
In previous versions, only MATLAB-style numerical arrays were fully supported.
Some Python datatypes such as dicts were coerced into numpy recarrays and then fetched as such.

However, since some Python types were coerced into MATLAB types, old blobs and new blobs may now be fetched as different types of objects even if they were inserted the same way. 
For example, new `dict` objects will be returned as `dict` while the same types of objects inserted with `datajoint 0.11` will be recarrays.

Since this is a big change, we chose to disable full blob support by default as a temporary precaution, which will be removed in version 0.13.

You may enable it by setting the `enable_python_native_blobs` flag in `dj.config`. 

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

With `dj.config["enable_python_native_blobs"]` set to `False` (default), 
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

## Documentation and Tutorials
A number of labs are currently adopting DataJoint and we are quickly getting the documentation in shape in February 2017.

* https://datajoint.io  -- start page
* https://docs.datajoint.io -- up-to-date documentation
* https://tutorials.datajoint.io -- step-by-step tutorials
* https://catalog.datajoint.io -- catalog of example pipelines

## Running Tests Locally


* Create an `.env` with desired development environment values e.g.
``` sh
PY_VER=3.7
ALPINE_VER=3.10
MYSQL_VER=5.7
MINIO_VER=RELEASE.2019-09-26T19-42-35Z
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
