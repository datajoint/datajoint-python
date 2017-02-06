## Release notes

### 0.4.10 (#286) 
* Removed Vagrant and Readthedocs support 
* Explicit saving of configuration (issue #284)

### 0.4.9 (#285) -- February 2, 2017
* Fixed setup.py for pip install 

### 0.4.7 (#281) -- January 24, 2017
* Fixed issues related to order of attributes in projection.

### 0.4.6 (#277) -- Dec 22, 2016
* Proper handling of interruptions during populate

### 0.4.5 (#274) -- Dec 20, 2016
* Populate reports how many keys remain to be populated at the start.

### 0.4.3  (#271) -- December 6, 2016
* Fixed aggregation issues (#270)
* datajoint no longer attempts to connect to server at import time
* dropped support of view (reversed #257)
* more elegant handling of insufficient privileges (#268)

### 0.4.2 (#267)  -- December 6, 2016
* improved table appearance in Jupyter

### 0.4.1 (#266) -- October 28, 2016
* bugfix for very long error messages

### 0.3.9 -- September 27, 2016
* Added support for datatype `YEAR`
* Fixed issues with `dj.U` and the `aggr` operator (#246, #247)

### 0.3.8  -- August 2, 2016
* added the `_update` method in `base_relation`. It allows updating values in existing tuples.
* bugfix in reading values of type double.  Previously it was cast as float32. 

### 0.3.7  -- July 31, 2016
* added parameter `ignore_extra_fields` in `insert` 
* `insert(..., skip_duplicates=True)` now relies on `SELECT IGNORE`.  Previously it explicitly checked if tuple already exists.
* table previews now include blob attributes displaying the string <BLOB>

### 0.3.6  -- July 30, 2016
* bugfix in `schema.spawn_missing_classes`.  Previously, spawned part classes would not show in ERDs.
* dj.key now causes fetch to return as a list of dicts.  Previously it was a recarray.

### 0.3.5
* `dj.set_password()` now asks for user confirmation before changing the password.
* fixed issue #228

### 0.3.4
* Added method the `ERD.add_parts` method, which adds the part tables of all tables currently in the ERD.
* `ERD() + arg` and `ERD() - arg` can now accept relation classes as arg.

### 0.3.3
* Suppressed warnings (redirected them to logging).  Previoiusly, scipy would throw warnings in ERD, for example.
* Added ERD.from_sequence as a shortcut to combining the ERDs of multiple sources
* ERD() no longer text the context argument.
* ERD.draw() now takes an optional context argument.  By default uses the caller's locals.

### 0.3.2.   
* Fixed issue #223:  `insert` can insert relations without fetching.
* ERD() now takes the `context` argument, which specifies in which context to look for classes. The default is taken from the argument (schema or relation).
* ERD.draw() no longer has the `prefix` argument: class names are shown as found in the context.
