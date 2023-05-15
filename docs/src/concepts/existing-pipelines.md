# Existing Pipelines

This section describes how to work with database schemas without access to the original
code that generated the schema. These situations often arise when the database is
created by another user who has not shared the generating code yet or when the database
schema is created from a programming language other than Python.

## Loading Classes

Typically, a DataJoint schema is created as a dedicated Python module. This module
defines a schema object that is used to link classes declared in the module to tables
in the database schema. With the module installed, you can simply import it to interact
with its tables:

``` python
import datajoint as dj
from element_calcium_imaging import scan # (1)
```

1. This and other [DataJoint Elements](https://datajoint.com/docs/elements/) are 
installable via `pip` or downloadable via their respective GitHub repositories.

To visualize an unfamiliar schema, see commands for generating [diagrams](../../getting-started/#diagram).

## Spawning Missing Classes

Now, imagine we do not have access to the 
[Python definition of Scan](https://github.com/datajoint/element-calcium-imaging/blob/main/element_calcium_imaging/scan.py),
or we're unsure if the version on our server matches the definition available. We can
use the `dj.list_schemas` function to list the available database schemas.

``` python
import datajoint as dj
dj.conn() # (1)
dj.list_schemas() # (2)
dj.Schema('schema_name').list_tables() # (3)
```

1. Establish a connection to the server.
2. List the available schemas on the server.
3. List the tables for a given schema from the previous step. These will appear in their
raw database form, with underscores instead of camelcase and special characters for Part
tables.

Just as with a new schema, we can create a schema object to connect to the chosen
database schema. If the schema already exists, `dj.Schema` is initialized as usual.

If a diagram will shows a mixture of class names and database table names, the
`spawn_missing_classes` method will spawn classes into the local namespace for any
tables missing their classes. This will allow us to interact with all tables as if
they were declared in the current namespace.

``` python
schema.spawn_missing_classes()
```

## Virtual Modules

While `spawn_missing_classes` creates the new classes in the local namespace, it is
often more convenient to import a schema with its Python module, equivalent to the
Python command. We can mimmick this import without having access to the schema using
the `VirtualModule` class object:

```python
import datajoint as dj
subject = dj.VirtualModule(module_name='subject', schema_name='db_subject')
```

Now, `subject` behaves as an imported module complete with the schema object and all the
table classes.

The class object `VirtualModule` of the `dj.Schema` class provides access to virtual
modules. It creates a python module with the given name from the name of a schema on
the server, automatically adds classes to it corresponding to the tables in the
schema.

The function can take several parameters:

- `module_name`: displayed module name.

- `schema_name`: name of the database in MySQL.

 `create_schema`: if `True`, create the schema on the database server if it does not
 already exist; if `False` (default), raise an error when the schema is not found.

- `create_tables`: if `True`, `module.schema` can be used as the decorator for declaring
  new classes; if `False`, such use will raise an error stating that the module is
  intend only to work with existing tables.

The function returns the Python module containing classes from the schema object with
all the table classes already declared inside it.

`create_schema=False` may be useful if we want to make sure that the schema already 
exists.  If none exists, `create_schema=True` will create an empty schema.

``` python
dj.VirtualModule('what', 'nonexistent')
```

Returns

``` python
DataJointError: Database named `nonexistent` was not defined. Set argument create_schema=True to create it.
```

`create_tables=False` prevents the use of the schema object of the virtual module for
creating new tables in the existing schema. This is a precautionary measure since
virtual modules are often used for completed schemas. `create_tables=True` will new
tables to the existing schema. A more common approach in this scenario would be to
create a new schema object and to use the `spawn_missing_classes` function to make the
classes available.

However, you if do decide to create new tables in an existing tables using the virtual
module, you may do so by using the schema object from the module as the decorator for
declaring new tables:

``` python
uni = dj.VirtualModule('university.py', 'dimitri_university', create_tables=True)
```

``` python
@uni.schema
class Example(dj.Manual):
    definition = """
    -> uni.Student
    ---
    example : varchar(255)
    """
```

``` python
dj.Diagram(uni)
```
