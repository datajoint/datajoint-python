0.12.7 -- Oct 27, 2020
----------------------
* Fix case sensitivity issues to adapt to MySQL 8+.  PR #819
* Fix pymysql regression bug (#814) PR #816
* Adapted attribute types now have `dtype=object` in all recarray results. PR #811 

0.12.6 -- May 15, 2020
----------------------
* Add `order_by` to `dj.kill` (#668, #779) PR #775, #783
* Add explicit S3 bucket and file storage location existence checks (#748) PR #781
* Modify `_update` to allow nullable updates for strings/date (#664) PR #760
* Avoid logging events on auxiliary tables (#737) PR #753
* Add `kill_quick` and expand display to include host (#740) PR #741
* Bugfix - pandas insert fails due to additional `index` field (#666) PR #776
* Bugfix - `delete_external_files=True` does not remove from S3 (#686) PR #781
* Bugfix - pandas fetch throws error when `fetch_format='frame'` PR #774

0.12.5 -- Feb 24, 2020
----------------------
* Rename module `dj.schema` into `dj.schemas`. `dj.schema` remains an alias for class `dj.Schema`. (#731) PR #732
* `dj.create_virtual_module` is now called `dj.VirtualModule` (#731) PR #732
* Bugfix - SSL `KeyError` on failed connection (#716) PR #725
* Bugfix - Unable to run unit tests using nosetests (#723) PR #724
* Bugfix - `suppress_errors` does not suppress loss of connection error (#720) PR #721

0.12.4 -- Jan 14, 2020
----------------------
* Support for simple scalar datatypes in blobs (#690) PR #709
* Add support for the `serial` data type in declarations: alias for `bigint unsigned auto_increment` PR #713
* Improve the log table to avoid primary key collisions PR #713
* Improve documentation in README PR #713

0.12.3 -- Nov 22, 2019
----------------------
* Bugfix - networkx 2.4 causes error in diagrams (#675) PR #705
* Bugfix - include table definition in doc string and help (#698, #699) PR #706
* Bugfix - job reservation fails when native python datatype support is disabled (#701) PR #702

0.12.2 -- Nov 11, 2019
-------------------------
* Bugfix - Convoluted error thrown if there is a reference to a non-existent table attribute (#691) PR #696
* Bugfix - Insert into external does not trim leading slash if defined in `dj.config['stores']['<store>']['location']` (#692) PR #693

0.12.1 -- Nov 2, 2019
-------------------------
* Bugfix - AttributeAdapter converts into a string (#684) PR #688

0.12.0 -- Oct 31, 2019
-------------------------
* Dropped support for Python 3.4
* Support secure connections with TLS (aka SSL) PR #620
* Convert numpy array from python object to appropriate data type if all elements are of the same type (#587) PR #608
* Remove expression requirement to have additional attributes (#604) PR #604
* Support for filepath datatype (#481) PR #603, #659
* Support file attachment datatype (#480, #592, #637) PR #659
* Fetch return a dict array when specifying `as_dict=True` for specified attributes. (#595) PR #593
* Support of ellipsis in `proj`:  `query_expression.proj(.., '-movie')` (#499) PR #578
* Expand support of blob serialization (#572, #520, #427, #392, #244, #594) PR #577
* Support for alter (#110) PR #573
* Support for `conda install datajoint` via `conda-forge` channel (#293)
* `dj.conn()` accepts a `port` keyword argument (#563) PR #571
* Support for UUID datatype (#562) PR #567
* `query_expr.fetch("KEY", as_dict=False)` returns results as `np.recarray`(#414) PR #574
* `dj.ERD` is now called `dj.Diagram` (#255, #546) PR #565
* `dj.Diagram` underlines "distinguished" classes (#378) PR #557
* Accept alias for supported MySQL datatypes (#544) PR #545
* Support for pandas in `fetch` (#459, #537) PR #534
* Support for ordering by "KEY" in `fetch` (#541) PR #534
* Add config to enable python native blobs PR #672, #676
* Add secure option for external storage (#663) PR #674, #676
* Add blob migration utility from DJ011 to DJ012 PR #673
* Improved external storage - a migration script needed from version 0.11  (#467, #475, #480, #497) PR #532
* Increase default display rows (#523) PR #526
* Bugfixes (#521, #205, #279, #477, #570, #581, #597, #596, #618, #633, #643, #644, #647, #648, #650, #656)
* Minor improvements (#538)

0.11.1 -- Nov 15, 2018
----------------------
* Fix ordering of attributes in proj (#483 and #516)
* Prohibit direct insert into auto-populated tables (#511)

0.11.0 -- Oct 25, 2018
----------------------
* Full support of dependencies with renamed attributes using projection syntax (#300, #345, #436, #506, #507)
* Rename internal class and module names to comply with terminology in documentation (#494, #500)
* Full support of secondary indexes (#498, 500)
* ERD no longer shows numbers in nodes corresponding to derived dependencies (#478, #500)
* Full support of unique and nullable dependencies (#254, #301, #493, #495, #500)
* Improve memory management in ``populate`` (#461, #486)
* Fix query errors and redundancies (#456, #463, #482)

0.10.1  -- Aug 28, 2018
-----------------------
* Fix ERD Tooltip message (#431)
* Networkx 2.0 support (#443)
* Fix insert from query with skip_duplicates=True (#451)
* Sped up queries (#458)
* Bugfix in restriction of the form (A & B) * B (#463)
* Improved error messages (#466)

0.10.0 -- Jan 10, 2018 
----------------------
* Deletes are more efficient (#424)
* ERD shows table definition on tooltip hover in Jupyter (#422) 
* S3 external storage
* Garbage collection for external sorage
* Most operators and methods of tables can be invoked as class methods rather than instance methods (#407)
* The schema decorator object no longer requires locals() to specify the context
* Compatibility with pymysql 0.8.0+
* More efficient loading of dependencies (#403)

0.9.0 -- Nov 17, 2017
---------------------
* Made graphviz installation optional
* Implement file-based external storage
* Implement union operator +
* Implement file-based external storage

0.8.0 -- Jul 26, 2017 
---------------------
Documentation and tutorials available at https://docs.datajoint.io and https://tutorials.datajoint.io
* improved the ERD graphics and features using the graphviz libraries (#207, #333)
* improved password handling logic (#322, #321)
* the use of the ``contents`` property to populate tables now only works in ``dj.Lookup`` classes (#310).
* allow suppressing the display of size of query results through the ``show_tuple_count`` configuration option (#309)
* implemented renamed foreign keys to spec (#333)
* added the ``limit`` keyword argument to populate (#329)
* reduced the number of displayed messages (#308)
* added ``size_on_disk`` property for dj.Schema() objects (#323)
* job keys are entered in the jobs table (#316, #243)
* simplified the ``fetch`` and ``fetch1`` syntax, deprecating the ``fetch[...]`` syntax (#319)
* the jobs tables now store the connection ids to allow identifying abandoned jobs (#288, #317)

0.5.0 (#298) -- Mar 8, 2017
---------------------------
* All fetched integers are now 64-bit long and all fetched floats are double precision.
* Added ``dj.create_virtual_module``

0.4.10 (#286) -- Feb 6, 2017
----------------------------
* Removed Vagrant and Readthedocs support 
* Explicit saving of configuration (issue #284)

0.4.9 (#285) -- Feb 2, 2017
---------------------------
* Fixed setup.py for pip install 

0.4.7 (#281) -- Jan 24, 2017
----------------------------
* Fixed issues related to order of attributes in projection.

0.4.6 (#277) -- Dec 22, 2016
----------------------------
* Proper handling of interruptions during populate

0.4.5 (#274) -- Dec 20, 2016
----------------------------
* Populate reports how many keys remain to be populated at the start.

0.4.3  (#271) -- Dec 6, 2016
----------------------------
* Fixed aggregation issues (#270)
* datajoint no longer attempts to connect to server at import time
* dropped support of view (reversed #257)
* more elegant handling of insufficient privileges (#268)

0.4.2 (#267)  -- Dec 6, 2016
----------------------------
* improved table appearance in Jupyter

0.4.1 (#266) -- Oct 28, 2016
----------------------------
* bugfix for very long error messages

0.3.9 -- Sep 27, 2016
---------------------
* Added support for datatype ``YEAR``
* Fixed issues with ``dj.U`` and the ``aggr`` operator (#246, #247)

0.3.8  -- Aug 2, 2016
---------------------
* added the ``_update`` method in ``base_relation``. It allows updating values in existing tuples.
* bugfix in reading values of type double.  Previously it was cast as float32. 

0.3.7  -- Jul 31, 2016
----------------------
* added parameter ``ignore_extra_fields`` in ``insert`` 
* ``insert(..., skip_duplicates=True)`` now relies on ``SELECT IGNORE``.  Previously it explicitly checked if tuple already exists.
* table previews now include blob attributes displaying the string <BLOB>

0.3.6  -- Jul 30, 2016
----------------------
* bugfix in ``schema.spawn_missing_classes``.  Previously, spawned part classes would not show in ERDs.
* dj.key now causes fetch to return as a list of dicts.  Previously it was a recarray.

0.3.5
-----
* ``dj.set_password()`` now asks for user confirmation before changing the password.
* fixed issue #228

0.3.4
-----
* Added method the ``ERD.add_parts`` method, which adds the part tables of all tables currently in the ERD.
* ``ERD() + arg`` and ``ERD() - arg`` can now accept relation classes as arg.

0.3.3
-----
* Suppressed warnings (redirected them to logging).  Previoiusly, scipy would throw warnings in ERD, for example.
* Added ERD.from_sequence as a shortcut to combining the ERDs of multiple sources
* ERD() no longer text the context argument.
* ERD.draw() now takes an optional context argument.  By default uses the caller's locals.

0.3.2   
-----
* Fixed issue #223:  ``insert`` can insert relations without fetching.
* ERD() now takes the ``context`` argument, which specifies in which context to look for classes. The default is taken from the argument (schema or relation).
* ERD.draw() no longer has the ``prefix`` argument: class names are shown as found in the context.
