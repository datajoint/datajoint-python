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

For the v0.12 release, the variable `enable_python_native_blobs` can be
safely enabled for improved blob support of python datatypes if the following
are true:

  * This is a new DataJoint installation / pipeline(s)
  * You have not used DataJoint prior to v0.12 with your pipeline(s)
  * You do not share blob data between Python and Matlab

Otherwise, please read the following carefully:

DataJoint v0.12 expands DataJoint's blob serialization mechanism with
improved support for complex native python datatypes, such as dictionaries
and lists of strings.

Prior to DataJoint v0.12, certain python native datatypes such as
dictionaries were 'squashed' into numpy structured arrays when saved into
blob attributes. This facilitated easier data sharing between Matlab
and Python for certain record types. However, this created a discrepancy
between insert and fetch datatypes which could cause problems in other
portions of users pipelines.

For v0.12, it was decided to remove the type squashing behavior, instead
creating a separate storage encoding which improves support for storing
native python datatypes in blobs without squashing them into numpy
structured arrays. However, this change creates a compatibility problem
for pipelines which previously relied on the type squashing behavior
since records saved via the old squashing format will continue to fetch
as structured arrays, whereas new record inserted in DataJoint 0.12 with
`enable_python_native_blobs` would result in records returned as the
appropriate native python type (dict, etc).  Read support for python
native blobs also not yet implemented in DataJoint for Matlab.

To prevent data from being stored in mixed format within a table across
upgrades from previous versions of DataJoint, the
`enable_python_native_blobs` flag was added as a temporary guard measure
for the 0.12 release. This flag will trigger an exception if any of the
ambiguous cases are encountered during inserts in order to allow testing
and migration of pre-0.12 pipelines to 0.11 in a safe manner.

The exact process to update a specific pipeline will vary depending on
the situation, but generally the following strategies may apply:

  * Altering code to directly store numpy structured arrays or plain
    multidimensional arrays. This strategy is likely best one for those 
    tables requiring compatibility with Matlab.
  * Adjust code to deal with both structured array and native fetched data.
    In this case, insert logic is not adjusted, but downstream consumers
    are adjusted to handle records saved under the old and new schemes.
  * Manually convert data using fetch/insert into a fresh schema.
    In this approach, DataJoint's create_virtual_module functionality would 
    be used in conjunction with a a fetch/convert/insert loop to update 
    the data to the new native_blob functionality.
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
