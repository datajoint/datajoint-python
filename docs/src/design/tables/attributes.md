# Datatypes

DataJoint supports the following datatypes.
To conserve database resources, use the smallest and most restrictive datatype 
sufficient for your data.
This also ensures that only valid data are entered into the pipeline.

## Most common datatypes

-  `tinyint`: an 8-bit integer number, ranging from -128 to 127.
-  `tinyint unsigned`: an 8-bit positive integer number, ranging from 0 to 255.
-  `smallint`: a 16-bit integer number, ranging from -32,768 to 32,767.
-  `smallint unsigned`: a 16-bit positive integer, ranging from 0 to 65,535.
-  `int`: a 32-bit integer number, ranging from -2,147,483,648 to 2,147,483,647.
-  `int unsigned`: a 32-bit positive integer, ranging from 0 to 4,294,967,295.
-  `enum`: one of several explicitly enumerated values specified as strings.
   Use this datatype instead of text strings to avoid spelling variations and to save 
   storage space.
   For example, the datatype for an anesthesia attribute could be 
   `enum("urethane", "isoflurane", "fentanyl")`.
   Do not use enums in primary keys due to the difficulty of changing their definitions 
   consistently in multiple tables.

-  `date`: date as `'YYYY-MM-DD'`.
-  `time`: time as `'HH:MM:SS'`.
-  `datetime`: Date and time to the second as `'YYYY-MM-DD HH:MM:SS'`
-  `timestamp`: Date and time to the second as `'YYYY-MM-DD HH:MM:SS'`.
   The default value may be set to `CURRENT_TIMESTAMP`.
   Unlike `datetime`, a `timestamp` value will be adjusted to the local time zone.

-  `char(N)`: a character string up to *N* characters (but always takes the entire *N* 
bytes to store).
-  `varchar(N)`: a text string of arbitrary length up to *N* characters that takes 
*M+1* or *M+2* bytes of storage, where *M* is the actual length of each stored string.
-  `float`: a single-precision floating-point number.
   Takes 4 bytes.
   Single precision is sufficient for many measurements.

-  `double`: a double-precision floating-point number.
   Takes 8 bytes.
   Because equality comparisons are error-prone, neither `float` nor `double` should be 
   used in primary keys.
-  `decimal(N,F)`: a fixed-point number with *N* total decimal digits and *F* 
fractional digits.
   This datatype is well suited to represent numbers whose magnitude is well defined 
   and does not warrant the use of floating-point representation or requires precise 
   decimal representations (e.g. dollars and cents).
   Because of its well-defined precision, `decimal` values can be used in equality 
   comparison and be included in primary keys.

-  `longblob`: arbitrary numeric array (e.g. matrix, image, structure), up to 4 
[GiB](http://en.wikipedia.org/wiki/Gibibyte) in size.
   Numeric arrays are compatible between MATLAB and Python (NumPy).
   The `longblob` and other `blob` datatypes can be configured to store data 
   [externally](../../sysadmin/external-store.md) by using the `blob@store` syntax.

## Less common (but supported) datatypes

-  `decimal(N,F) unsigned`: same as `decimal`, but limited to nonnegative values.
-  `mediumint` a 24-bit integer number, ranging from -8,388,608 to 8,388,607.
-  `mediumint unsigned`: a 24-bit positive integer, ranging from 0 to 16,777,216.
-  `mediumblob`: arbitrary numeric array, up to 16 
[MiB](http://en.wikipedia.org/wiki/Mibibyte)
-  `blob`: arbitrary numeric array, up to 64 
[KiB](http://en.wikipedia.org/wiki/Kibibyte)
-  `tinyblob`: arbitrary numeric array, up to 256 bytes (actually smaller due to header 
info).

## Special DataJoint-only datatypes

These types abstract certain kinds of non-database data to facilitate use
together with DataJoint.

- `attach`: a [file attachment](attach.md) similar to email attachments facillitating 
sending/receiving an opaque data file to/from a DataJoint pipeline.

- `filepath@store`: a [filepath](filepath.md) used to link non-DataJoint managed files 
into a DataJoint pipeline.

## Datatypes not (yet) supported

-  `binary`
-  `text`
-  `longtext`
-  `bit`

For additional information about these datatypes, see 
http://dev.mysql.com/doc/refman/5.6/en/data-types.html
