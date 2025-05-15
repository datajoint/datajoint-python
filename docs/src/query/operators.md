# Operators

[Data queries](principles.md) have the form of expressions using operators to derive
the desired table.
The expressions themselves do not contain any data.
They represent the desired data symbolically.

Once a query is formed, the [fetch](fetch.md) methods are used to bring the data into
the local workspace.
Since the expressions are only symbolic representations, repeated `fetch` calls may
yield different results as the state of the database is modified.

DataJoint implements a complete algebra of operators on tables:

| operator                     | notation       | meaning                                                                 |
|------------------------------|----------------|-------------------------------------------------------------------------|
| [join](#join)                | A * B          | All matching information from A and B                                   |
| [restriction](#restriction)  | A & cond       | The subset of entities from A that meet the condition                   |
| [restriction](#restriction)  | A - cond       | The subset of entities from A that do not meet the condition            |
| [proj](#proj)                | A.proj(...)    | Selects and renames attributes from A or computes new attributes        |
| [aggr](#aggr)                | A.aggr(B, ...) | Same as projection with computations based on matching information in B |
| [union](#union)              | A + B          | All unique entities from both A and B                                   |
| [universal set](#universal-set)\*| dj.U()     | All unique entities from both A and B                                   |
| [top](#top)\*| dj.Top()     | The top rows of A

\*While not technically query operators, it is useful to discuss Universal Set and Top in the
same context.

## Principles of relational algebra

DataJoint's algebra improves upon the classical relational algebra and upon other query
languages to simplify and enhance the construction and interpretation of precise and
efficient data queries.

1. **Entity integrity**: Data are represented and manipulated in the form of tables
representing [well-formed entity sets](../design/integrity.md).
   This applies to the inputs and outputs of query operators.
   The output of a query operator is an entity set with a well-defined entity type, a
   primary key, unique attribute names, etc.
2. **Algebraic closure**: All operators operate on entity sets and yield entity sets.
   Thus query expressions may be used as operands in other expressions or may be
   assigned to variables to be used in other expressions.
3. **Attributes are identified by names**:  All attributes have explicit names.
   This includes results of queries.
   Operators use attribute names to determine how to perform the operation.
   The order of the attributes is not significant.

## Matching entities

Binary operators in DataJoint are based on the concept of **matching entities**; this
phrase will be used throughout the documentation.

    Two entities **match** when they have no common attributes or when their common
    attributes contain the same values.

Here **common attributes** are those that have the same names in both entities.
It is usually assumed that the common attributes are of compatible datatypes to allow
equality comparisons.

Another way to phrase the same definition is

    Two entities match when they have no common attributes whose values differ.

It may be conceptually convenient to imagine that all tables always have an additional
invisible attribute, `omega` whose domain comprises only one value, 1.
Then the definition of matching entities is simplified:

    Two entities match when their common attributes contain the same values.

Matching entities can be **merged** into a single entity without any conflicts of
attribute names and values.

### Examples

This is a matching pair of entities:

![matched_tuples1](../images/matched_tuples1.png){: style="width:366px"}

and so is this one:

![matched_tuples2](../images/matched_tuples2.png){: style="width:366px"}

but these entities do *not* match:

![matched_tuples3](../images/matched_tuples3.png){: style="width:366px"}

## Join compatibility

All binary operators with other tables as their two operands require that the operands
be **join-compatible**, which means that:

1. All common attributes in both operands (attributes with the same name) must be part
of either the primary key or a foreign key.
2. All common attributes in the two relations must be of a compatible datatype for
equality comparisons.

## Restriction

The restriction operator `A & cond` selects the subset of entities from `A` that meet
the condition `cond`. The exclusion operator `A - cond` selects the complement of
restriction, i.e. the subset of entities from `A` that do not meet the condition
`cond`. This means that the restriction and exclusion operators are complementary.
The same query could be constructed using either `A & cond` or `A - Not(cond)`.

<figure markdown>
![Restriction and exclusion.](../../../images/concepts-operators-restriction.png){: style="height:200px"}
</figure>

The condition `cond` may be one of the following:

=== "Python"

    -   another table
    -   a mapping, e.g. `dict`
    -   an expression in a character string
    -   a collection of conditions as a `list`, `tuple`, or Pandas `DataFrame`
    -   a Boolean expression (`True` or `False`)
    -   an `AndList`
    -   a `Not` object
    -   a query expression

??? Warning "Permissive Operators"

    To circumvent compatibility checks, DataJoint offers permissive operators for
    Restriction (`^`) and Join (`@`). Use with Caution.

## Proj

The `proj` operator represents **projection** and is used to select attributes
(columns) from a table, to rename them, or to create new calculated attributes.

1. A simple projection *selects a subset of attributes* of the original
table, which may not include the [primary key](../concepts/glossary#primary-key).

2. A more complex projection *renames an attribute* in another table. This could be
useful when one table should be referenced multiple times in another. A user table,
could contain all personnel. A project table references one person for the lead and
another the coordinator, both referencing the common personnel pool.

3. Projection can also perform calculations (as available in
[MySQL](https://dev.mysql.com/doc/refman/5.7/en/functions.html)) on a single attribute.

## Aggr

**Aggregation** is a special form of `proj` with the added feature of allowing
  aggregation calculations on another table. It has the form `table.aggr
  (other, ...)` where `other` is another table. Aggregation allows adding calculated
  attributes to each entity in `table` based on aggregation functions over attributes
  in the matching entities of `other`.

Aggregation functions include `count`, `sum`, `min`, `max`, `avg`, `std`, `variance`,
and others.

## Union

The result of the union operator `A + B` contains all the entities from both operands.

[Entity normalization](../design/normalization) requires that `A` and `B` are of the same type,
with with the same [primary key](../concepts/glossary#primary-key), using homologous
attributes. Without secondary attributes, the result is the simple set union. With
secondary attributes, they must have the same names and datatypes. The two operands
must also be **disjoint**, without any duplicate primary key values across both inputs.
These requirements prevent ambiguity of attribute values and preserve entity identity.

??? Note "Principles of union"

    1.  As in all operators, the order of the attributes in the operands is not
    significant.

    2.  Operands `A` and `B` must have the same primary key attributes. Otherwise, an
    error will be raised.

    3.  Operands `A` and `B` may not have any common non-key attributes. Otherwise, an
    error will be raised.

    4.  The result `A + B` will have the same primary key as `A` and `B`.

    5.  The result `A + B` will have all the non-key attributes from both `A` and `B`.

    6.  For entities that are found in both `A` and `B` (based on the primary key), the
    secondary attributes will be filled from the corresponding entities in `A` and
    `B`.

    7.  For entities that are only found in either `A` or `B`, the other operand's
    secondary attributes will filled with null values.

For union, order does not matter.

<figure markdown>
![Union Example 1](../../../images/concepts-operators-union1.png){: style="height:200px"}
</figure>
<figure markdown>
![Union Example 2](../../../images/concepts-operators-union2.png){: style="height:200px"}
</figure>

??? Note "Properties of union"

    1.  Commutative: `A + B` is equivalent to `B + A`.
    2.  Associative: `(A + B) + C` is equivalent to `A + (B + C)`.

## Universal Set

All of the above operators are designed to preserve their input type. Some queries may
require creating a new entity type not already represented by existing tables. This
means that the new type must be defined as part of the query.

Universal sets fulfill this role using `dj.U` notation. They denote the set of all
possible entities with given attributes of any possible datatype. Attributes of
universal sets are allowed to be matched to any namesake attributes, even those that do
not come from the same initial source.

Universal sets should be used sparingly when no suitable base tables already exist. In
some cases, defining a new base table can make queries clearer and more semantically
constrained.

The examples below will use the table definitions in [table tiers](../reproduce/table-tiers).

<!-- ## Join appears here in the general docs -->

## Top

Similar to the universal set operator, the top operator uses `dj.Top` notation. It is used to
restrict a query by the given `limit`, `order_by`, and `offset` parameters:

```python
Session & dj.Top(limit=10, order_by='session_date')
```

The result of this expression returns the first 10 rows of `Session` and sorts them
by their `session_date` in ascending order.

### `order_by`

| Example                                   | Description                                                                     |
|-------------------------------------------|---------------------------------------------------------------------------------|
| `order_by="session_date DESC"`            | Sort by `session_date` in *descending* order                                    |
| `order_by="KEY"`                          | Sort by the primary key                                                         |
| `order_by="KEY DESC"`                     | Sort by the primary key in *descending* order                                     |
| `order_by=["subject_id", "session_date"]` | Sort by `subject_id`, then sort matching `subject_id`s by their `session_date`  |

The default values for `dj.Top` parameters are `limit=1`, `order_by="KEY"`, and `offset=0`.

## Restriction

`&` and `-` operators permit restriction.

### By a mapping

For a [Session table](../reproduce/table-tiers#manual-tables), that has the attribute
`session_date`, we can restrict to sessions from January 1st, 2022:

```python
Session & {'session_date': "2022-01-01"}
```

If there were any typos (e.g., using `sess_date` instead of `session_date`), our query
will return all of the entities of `Session`.

### By a string

Conditions may include arithmetic operations, functions, range tests, etc. Restriction
of table `A` by a string containing an attribute not found in table `A` produces an
error.

```python
Session & 'user = "Alice"' # (1)
Session & 'session_date >= "2022-01-01"' # (2)
```

1. All the sessions performed by Alice
2. All of the sessions on or after January 1st, 2022

### By a collection

When `cond` is a collection of conditions, the conditions are applied by logical
disjunction (logical OR). Restricting a table by a collection will return all entities
that meet *any* of the conditions in the collection.

For example, if we restrict the `Session` table by a collection containing two
conditions, one for user and one for date, the query will return any sessions with a
matching user *or* date.

A collection can be a list, a tuple, or a Pandas `DataFrame`.

``` python
cond_list = ['user = "Alice"', 'session_date = "2022-01-01"'] # (1)
cond_tuple = ('user = "Alice"', 'session_date = "2022-01-01"') # (2)
import pandas as pd
cond_frame = pd.DataFrame(data={'user': ['Alice'], 'session_date': ['2022-01-01']}) # (3)

Session() & ['user = "Alice"', 'session_date = "2022-01-01"']
```

1. A list
2. A tuple
3. A data frame

`dj.AndList` represents logical conjunction(logical AND). Restricting a table by an
`AndList` will return all entities that meet *all* of the conditions in the list. `A &
dj.AndList([c1, c2, c3])` is equivalent to `A & c1 & c2 & c3`.

```python
Student() & dj.AndList(['user = "Alice"', 'session_date = "2022-01-01"'])
```

The above will show all the sessions that Alice conducted on the given day.

### By a `Not` object

The special function `dj.Not` represents logical negation, such that `A & dj.Not
(cond)` is equivalent to `A - cond`.

### By a query

Restriction by a query object is a generalization of restriction by a table. The example
below creates a query object corresponding to all the users named Alice. The `Session`
table is then restricted by the query object, returning all the sessions performed by
Alice.

``` python
query = User & 'user = "Alice"'
Session & query
```

## Proj

Renaming an attribute in python can be done via keyword arguments:

```python
table.proj(new_attr='old_attr')
```

This can be done in the context of a table definition:

```python
@schema
class Session(dj.Manual):
    definition = """
    # Experiment Session
    -> Animal
    session             : smallint  # session number for the animal
    ---
    session_datetime    : datetime  # YYYY-MM-DD HH:MM:SS
    session_start_time  : float     # seconds relative to session_datetime
    session_end_time    : float     # seconds relative to session_datetime
    -> User.proj(experimenter='username')
    -> User.proj(supervisor='username')
    """
```

Or to rename multiple values in a table with the following syntax:
`Table.proj(*existing_attributes,*renamed_attributes)`

```python
Session.proj('session','session_date',start='session_start_time',end='session_end_time')
```

Projection can also be used to to compute new attributes from existing ones.

```python
Session.proj(duration='session_end_time-session_start_time') & 'duration > 10'
```

## Aggr

For more complicated calculations, we can use aggregation.

``` python
Subject.aggr(Session,n="count(*)") # (1)
Subject.aggr(Session,average_start="avg(session_start_time)") # (2)
```

1. Number of sessions per subject.
2. Average `session_start_time` for each subject

<!-- ## Union appears here in the general docs -->

## Universal set

Universal sets offer the complete list of combinations of attributes.

``` python
# All home cities of students
dj.U('laser_wavelength', 'laser_power') & Scan # (1)
dj.U('laser_wavelength', 'laser_power').aggr(Scan, n="count(*)") # (2)
dj.U().aggr(Session, n="max(session)") # (3)
```

1. All combinations of wavelength and power.
2. Total number of scans for each combination.
3. Largest session number.

`dj.U()`, as shown in the last example above, is often useful for integer IDs.
For an example of this process, see the source code for
[Element Array Electrophysiology's `insert_new_params`](https://docs.datajoint.com/elements/element-array-ephys/latest/api/element_array_ephys/ephys_acute/#element_array_ephys.ephys_acute.ClusteringParamSet.insert_new_params).
