## Release notes
### 0.3.2.   
* Fixed issue #223:  `insert` can insert relations without fetching
* ERD() now takes the `context` argument, which specifies in which context to look for classes. The default is taken from the argument (schema or relation).
* ERD.draw() no longer has the `prefix` argument: class names are shown as found in the context.

### 0.3.3
* Suppressed warnings (redirected them to logging).  Previoiusly, scipy would throw warnings in ERD, for example.
* Added ERD.from_sequence as a shortcut to combining the ERDs of multiple sources
* ERD() no longer text the context argument.
* ERD.draw() now takes an optional context argument.  By default uses the caller's locals.

### 0.3.4
* Added method the `ERD.add_parts` method, which adds the part tables of all tables currently in the ERD.
* `ERD() + arg` and `ERD() - arg` can now accept relation classes as arg.

### 0.3.5
* `dj.set_password()` now asks for user confirmation before changing the password.
* fixed issue #228

### 0.3.6  -- July 30, 2016
* bugfix in `schema.spawn_missing_classes`.  Previously, spawned part classes would not show in ERDs.
* dj.key now causes fetch to return as a list of dicts.  Previously it was a recarray.

### 0.3.7  -- July 31, 2016
* added parameter `ignore_extra_fields` in `insert` 
* `insert(..., skip_duplicates=True)` now relies on `SELECT IGNORE`.  Previously it explicitly checked if tuple already exists.
* table previews now include blob attributes displaying the string <BLOB>

