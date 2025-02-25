# Primary Key

## Primary keys in DataJoint

Entities in tables are neither named nor numbered.
DataJoint does not answer questions of the type "What is the 10th element of this table?"
Instead, entities are distinguished by the values of their attributes.
Furthermore, the entire entity is not required for identification.
In each table, a subset of its attributes are designated to be the **primary key**.
Attributes in the primary key alone are sufficient to differentiate any entity from any 
other within the table.

Each table must have exactly one 
[primary key](http://en.wikipedia.org/wiki/Primary_key): a subset of its attributes 
that uniquely identify each entity in the table.
The database uses the primary key to prevent duplicate entries, to relate data across 
tables, and to accelerate data queries.
The choice of the primary key will determine how you identify entities.
Therefore, make the primary key **short**, **expressive**, and **persistent**.

For example, mice in our lab are assigned unique IDs.
The mouse ID number `animal_id` of type `smallint` can serve as the primary key for the 
table `Mice`.
An experiment performed on a mouse may be identified in the table `Experiments` by two 
attributes: `animal_id` and `experiment_number`.

DataJoint takes the concept of primary keys somewhat more seriously than other models 
and query languages.
Even **table expressions**, i.e. those tables produced through operations on other 
tables, have a well-defined primary key.
All operators on tables are designed in such a way that the results always have a 
well-defined primary key.

In all representations of tables in DataJoint, the primary key attributes are always 
listed before other attributes and highlighted for emphasis (e.g. in a **bold** font or 
marked with an asterisk \*)

## Defining a primary key

In table declarations, the primary key attributes always come first and are separated 
from the other attributes with a line containing at least three hyphens.
For example, the following is the definition of a table containing database users where 
`username` is the primary key.

```python
# database users
username : varchar(20)   # unique user name
---
first_name : varchar(30)
last_name  : varchar(30)
role : enum('admin', 'contributor', 'viewer')
```

## Entity integrity

The primary key defines and enforces the desired property of databases known as 
[entity integrity](../integrity.md).
**Entity integrity** ensures that there is a one-to-one and unambiguous mapping between 
real-world entities and their representations in the database system.
The data management process must prevent any duplication or misidentification of 
entities.

To enforce entity integrity, DataJoint implements several rules:

- Every table must have a primary key.
- Primary key attributes cannot have default values (with the exception of 
`auto_increment` and `CURRENT_TIMESTAMP`; see below).
- Operators on tables are defined with respect to the primary key and preserve a 
primary key in their results.

## Datatypes in primary keys

All integer types, dates, timestamps, and short character strings make good primary key 
attributes.
Character strings are somewhat less suitable because they can be long and because they 
may have invisible trailing spaces.
Floating-point numbers should be avoided because rounding errors may lead to 
misidentification of entities.
Enums are okay as long as they do not need to be modified after 
[dependencies](dependencies.md) are already created referencing the table.
Finally, DataJoint does not support blob types in primary keys.

The primary key may be composite, i.e. comprising several attributes.
In DataJoint, hierarchical designs often produce tables whose primary keys comprise 
many attributes.

## Choosing primary key attributes

A primary key comprising real-world attributes is a good choice when such real-world 
attributes are already properly and permanently assigned.
Whatever characteristics are used to uniquely identify the actual entities can be used 
to identify their representations in the database.

If there are no attributes that could readily serve as a primary key, an artificial 
attribute may be created solely for the purpose of distinguishing entities.
In such cases, the primary key created for management in the database must also be used 
to uniquely identify the entities themselves.
If the primary key resides only in the database while entities remain indistinguishable 
in the real world, then the process cannot ensure entity integrity.
When a primary key is created as part of data management rather than based on 
real-world attributes, an institutional process must ensure the uniqueness and 
permanence of such an identifier.

For example, the U.S. government assigns every worker an identifying attribute, the 
social security number.
However, the government must go to great lengths to ensure that this primary key is 
assigned exactly once, by checking against other less convenient candidate keys (i.e. 
the combination of name, parents' names, date of birth, place of birth, etc.).
Just like the SSN, well managed primary keys tend to get institutionalized and find 
multiple uses.

Your lab must maintain a system for uniquely identifying important entities.
For example, experiment subjects and experiment protocols must have unique IDs.
Use these as the primary keys in the corresponding tables in your DataJoint databases.

### Using hashes as primary keys

Some tables include too many attributes in their primary keys.
For example, the stimulus condition in a psychophysics experiment may have a dozen 
parameters such that a change in any one of them makes a different valid stimulus 
condition.
In such a case, all the attributes would need to be included in the primary key to 
ensure entity integrity.
However, long primary keys make it difficult to reference individual entities.
To be most useful, primary keys need to be relatively short.

This problem is effectively solved through the use of a hash of all the identifying 
attributes as the primary key.
For example, MD5 or SHA-1 hash algorithms can be used for this purpose.
To keep their representations human-readable, they may be encoded in base-64 ASCII.
For example, the 128-bit MD5 hash can be represented by 21 base-64 ASCII characters, 
but for many applications, taking the first 8 to 12 characters is sufficient to avoid 
collisions.

### `auto_increment`

Some entities are created by the very action of being entered into the database.
The action of entering them into the database gives them their identity.
It is impossible to duplicate them since entering the same thing twice still means 
creating two distinct entities.

In such cases, the use of an auto-incremented primary key is warranted.
These are declared by adding the word `auto_increment` after the data type in the 
declaration.
The datatype must be an integer.
Then the database will assign incrementing numbers at each insert.

The example definition below defines an auto-incremented primary key

```python
# log entries
entry_id  :  smallint auto_increment
---
entry_text :  varchar(4000)
entry_time = CURRENT_TIMESTAMP : timestamp(3)  # automatic timestamp with millisecond precision
```

DataJoint passes `auto_increment` behavior to the underlying MySQL and therefore it has 
the same limitation: it can only be used for tables with a single attribute in the 
primary key.

If you need to auto-increment an attribute in a composite primary key, you will need to 
do so programmatically within a transaction to avoid collisions.

For example, let’s say that you want to auto-increment `scan_idx` in a table called 
`Scan` whose primary key is `(animal_id, session, scan_idx)`.
You must already have the values for `animal_id` and `session` in the dictionary `key`.
Then you can do the following:

```python
U().aggr(Scan & key, next='max(scan_idx)+1')

# or

Session.aggr(Scan, next='max(scan_idx)+1') & key
```

Note that the first option uses a [universal set](../../query/universals.md).
