# Operators

The examples below will use the table definitions in [table tiers](../../reproduce/table-tiers).

<!-- ## Join appears here in the general docs -->

## Restriction

`&` and `-` operators permit restriction.

### By a mapping

For a [Session table](../../reproduce/table-tiers#manual-tables), that has the attribute
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
[Element Array Electrophysiology's `insert_new_params`](https://datajoint.com/docs/elements/element-array-ephys/latest/api/element_array_ephys/ephys_acute/#element_array_ephys.ephys_acute.ClusteringParamSet.insert_new_params).
