# Declaration Syntax

## Creating Tables

### Classes represent tables

To make it easy to work with tables in MATLAB and Python, DataJoint programs create a 
separate class for each table.
Computer programmers refer to this concept as 
[object-relational mapping](https://en.wikipedia.org/wiki/Object-relational_mapping).
For example, the class `experiment.Subject` in the DataJoint client language may 
correspond to the table called `subject` on the database server.
Users never need to see the database directly; they only interact with data in the 
database by creating and interacting with DataJoint classes.

#### Data tiers

The table class must inherit from one of the following superclasses to indicate its 
data tier: `dj.Lookup`, `dj.Manual`, `dj.Imported`, `dj.Computed`, or `dj.Part`.
See [tiers](tiers.md) and [master-part](./master-part.md).

### Defining a table

To define a DataJoint table in Python:

1. Define a class inheriting from the appropriate DataJoint class: `dj.Lookup`, 
`dj.Manual`, `dj.Imported` or `dj.Computed`.

2. Decorate the class with the schema object (see [schema](../schema.md))

3. Define the class property `definition` to define the table heading.

For example, the following code defines the table `Person`:

```python
import datajoint as dj
schema = dj.Schema('alice_experiment')

@schema
class Person(dj.Manual):
     definition = '''
          username : varchar(20)   # unique user name
     ---
     first_name : varchar(30)
     last_name  : varchar(30)
     '''
```

The `@schema` decorator uses the class name and the data tier to check whether an 
appropriate table exists on the database.
If a table does not already exist, the decorator creates one on the database using the 
definition property.
The decorator attaches the information about the table to the class, and then returns 
the class.

The class will become usable after you define the `definition` property as described in 
[Table definition](#table-definition).

#### DataJoint classes in Python

DataJoint for Python is implemented through the use of classes providing access to the 
actual tables stored on the database.
Since only a single table exists on the database for any class, interactions with all 
instances of the class are equivalent.
As such, most methods can be called on the classes themselves rather than on an object, 
for convenience.
Whether calling a DataJoint method on a class or on an instance, the result will only 
depend on or apply to the corresponding table.
All of the basic functionality of DataJoint is built to operate on the classes 
themselves, even when called on an instance.
For example, calling `Person.insert(...)` (on the class) and `Person.insert(...)` (on 
an instance) both have the identical effect of inserting data into the table on the 
database server.
DataJoint does not prevent a user from working with instances, but the workflow is 
complete without the need for instantiation.
It is up to the user whether to implement additional functionality as class methods or 
methods called on instances.

### Valid class names

Note that in both MATLAB and Python, the class names must follow the CamelCase compound 
word notation:

- start with a capital letter and
- contain only alphanumerical characters (no underscores).

Examples of valid class names:

`TwoPhotonScan`, `Scan2P`, `Ephys`, `MembraneVoltage`

Invalid class names:

`Two_photon_Scan`, `twoPhotonScan`, `2PhotonScan`, `membranePotential`, `membrane_potential`

## Table Definition

DataJoint models data as sets of **entities** with shared **attributes**, often 
visualized as tables with rows and columns.
Each row represents a single entity and the values of all of its attributes.
Each column represents a single attribute with a name and a datatype, applicable to 
entity in the table.
Unlike rows in a spreadsheet, entities in DataJoint don't have names or numbers: they 
can only be identified by the values of their attributes.
Defining a table means defining the names and datatypes of the attributes as well as 
the constraints to be applied to those attributes.
Both MATLAB and Python use the same syntax define tables.

For example, the following code in defines the table `User`, that contains users of the 
database:

The table definition is contained in the `definition` property of the class.

```python
@schema
class User(dj.Manual):
     definition = """
     # database users
     username : varchar(20)   # unique user name
     ---
     first_name : varchar(30)
     last_name  : varchar(30)
     role : enum('admin', 'contributor', 'viewer')
     """
```

This defines the class `User` that creates the table in the database and provides all 
its data manipulation functionality.

### Table creation on the database server

Users do not need to do anything special to have a table created in the database.
Tables are created at the time of class definition.
In fact, table creation on the database is one of the jobs performed by the decorator 
`@schema` of the class.

### Changing the definition of an existing table

Once the table is created in the database, the definition string has no further effect.
In other words, changing the definition string in the class of an existing table will 
not actually update the table definition.
To change the table definition, one must first [drop](../drop.md) the existing table.
This means that all the data will be lost, and the new definition will be applied to 
create the new empty table.

Therefore, in the initial phases of designing a DataJoint pipeline, it is common to 
experiment with variations of the design before populating it with substantial amounts 
of data.

It is possible to modify a table without dropping it.
This topic is covered separately.

### Reverse-engineering the table definition

DataJoint objects provide the `describe` method, which displays the table definition 
used to define the table when it was created in the database.
This definition may differ from the definition string of the class if the definition 
string has been edited after creation of the table.

Examples

```python
s = lab.User.describe()
```

## Definition Syntax

The table definition consists of one or more lines.
Each line can be one of the following:

- The optional first line starting with a `#` provides a description of the table's purpose.
  It may also be thought of as the table's long title.
- A new attribute definition in any of the following forms (see 
[Attributes](./attributes.md) for valid datatypes):
  ``name : datatype``
  ``name : datatype # comment``
  ``name = default : datatype``
  ``name = default : datatype  # comment``
- The divider `---` (at least three hyphens) separating primary key attributes above 
from secondary attributes below.
- A foreign key in the format `-> ReferencedTable`.
  (See [Dependencies](dependencies.md).)

For example, the table for Persons may have the following definition:

```python
# Persons in the lab
username :  varchar(16)   #  username in the database
---
full_name  : varchar(255)
start_date :  date   # date when joined the lab
```

This will define the table with attributes `username`, `full_name`, and `start_date`, 
in which `username` is the [primary key](primary.md).

### Attribute names

Attribute names must be in lowercase and must start with a letter.
They can only contain alphanumerical characters and underscores.
The attribute name cannot exceed 64 characters.

Valid attribute names
   `first_name`, `two_photon_scan`, `scan_2p`, `two_photon_scan`

Invalid attribute names
   `firstName`, `first name`, `2photon_scan`, `two-photon_scan`, `TwoPhotonScan`

Ideally, attribute names should be unique across all tables that are likely to be used 
in queries together.
For example, tables often have attributes representing the start times of sessions, 
recordings, etc.
Such attributes must be uniquely named in each table, such as `session_start_time` or 
`recording_start_time`.

### Default values

Secondary attributes can be given default values.
A default value will be used for an attribute if no other value is given at the time 
the entity is [inserted](../../manipulation/insert.md) into the table.
Generally, default values are numerical values or character strings.
Default values for dates must be given as strings as well, contained within quotes 
(with the exception of `CURRENT_TIMESTAMP`).
Note that default values can only be used when inserting as a mapping.
Primary key attributes cannot have default values (with the exceptions of 
`auto_increment` and `CURRENT_TIMESTAMP` attributes; see [primary-key](primary.md)).

An attribute with a default value of `NULL` is called a **nullable attribute**.
A nullable attribute can be thought of as applying to all entities in a table but 
having an optional *value* that may be absent in some entities.
Nullable attributes should *not* be used to indicate that an attribute is inapplicable 
to some entities in a table (see [normalization](../normalization.md)).
Nullable attributes should be used sparingly to indicate optional rather than 
inapplicable attributes that still apply to all entities in the table.
`NULL` is a special literal value and does not need to be enclosed in quotes.

Here are some examples of attributes with default values:

```python
failures = 0 : int
due_date = "2020-05-31" : date
additional_comments = NULL : varchar(256)
```
