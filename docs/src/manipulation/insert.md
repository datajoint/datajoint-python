# Common Commands

## Insert

Data entry is as easy as providing the appropriate data structure to a permitted
[table](../reproduce/table-tiers.md).

Given the following [table definition](../getting-started/table-definitions.md), we can
insert data as follows.

```text      
    mouse_id: int            # unique mouse id
    ---
    dob: date                # mouse date of birth
    sex: enum('M', 'F', 'U') # sex of mouse - Male, Female, or Unknown
``` 

```python
mouse.insert1( (0, '2017-03-01', 'M') ) # Single entry
data = [
    (1, '2016-11-19', 'M'),
    (2, '2016-11-20', 'U'),
    (5, '2016-12-25', 'F')
]
mouse.insert(data) # Multi-entry
```

## Make

The `make` method populates automated tables from inserted data. Read more in the
full article [here](../reproduce/make-method.md)

## Fetch

Data queries in DataJoint comprise two distinct steps:

1.  Construct the `query` object to represent the required data using
    tables and [operators](../query/operators).
2.  Fetch the data from `query` into the workspace of the host language.

Note that entities returned by `fetch` methods are not guaranteed to be sorted in any
particular order unless specifically requested. Furthermore, the order is not
guaranteed to be the same in any two queries, and the contents of two identical queries
may change between two sequential invocations unless they are wrapped in a transaction.
Therefore, if you wish to fetch matching pairs of attributes, do so in one `fetch`
call.

``` python
data = query.fetch()
```

## Drop

The `drop` method completely removes a table from the database, including its
definition. It also removes all dependent tables, recursively. DataJoint will first
display the tables being dropped and the number of entities in each before prompting
the user for confirmation to proceed.

The `drop` method is often used during initial design to allow altered
table definitions to take effect.

``` python
# drop the Person table from its schema
Person.drop()
```

## Diagrams

The `Diagram` command can help you visualize your pipeline, or understand
an existing pipeline. 

``` python
import datajoint as dj
schema = dj.Schema('my_database')
dj.Diagram(schema).draw()
```

For more information about diagrams, see [this article](../design/diagrams).
