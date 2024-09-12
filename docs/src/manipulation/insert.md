# Insert

The `insert` method of DataJoint table objects inserts entities into the table.

In Python there is a separate method `insert1` to insert one entity at a time.
The entity may have the form of a Python dictionary with key names matching the 
attribute names in the table.

```python
lab.Person.insert1(
          dict(username='alice',
               first_name='Alice',
               last_name='Cooper'))
```

The entity also may take the form of a sequence of values in the same order as the 
attributes in the table.

```python
lab.Person.insert1(['alice', 'Alice', 'Cooper'])
```

Additionally, the entity may be inserted as a 
[NumPy record array](https://docs.scipy.org/doc/numpy/reference/generated/numpy.record.html#numpy.record)
 or [Pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html).

The `insert` method accepts a sequence or a generator of multiple entities and is used 
to insert multiple entities at once.

```python
lab.Person.insert([
          ['alice',   'Alice',   'Cooper'],
          ['bob',     'Bob',     'Dylan'],
          ['carol',   'Carol',   'Douglas']])
```

Several optional parameters can be used with `insert`:

  `replace` If `True`, replaces the existing entity.
  (Default `False`.)

  `skip_duplicates` If `True`, silently skip duplicate inserts.
  (Default `False`.)

  `ignore_extra_fields` If `False`, fields that are not in the heading raise an error.
  (Default `False`.)

  `allow_direct_insert` If `True`, allows inserts outside of populate calls.
  Applies only in auto-populated tables.
  (Default `None`.)

## Batched inserts

Inserting a set of entities in a single `insert` differs from inserting the same set of 
entities one-by-one in a `for` loop in two ways:

1. Network overhead is reduced.
   Network overhead can be tens of milliseconds per query.
   Inserting 1000 entities in a single `insert` call may save a few seconds over 
   inserting them individually.
2. The insert is performed as an all-or-nothing transaction.
   If even one insert fails because it violates any constraint, then none of the 
   entities in the set are inserted.

However, inserting too many entities in a single query may run against buffer size or 
packet size limits of the database server.
Due to these limitations, performing inserts of very large numbers of entities should 
be broken up into moderately sized batches, such as a few hundred at a time.

## Server-side inserts

Data inserted into a table often come from other tables already present on the database server.
In such cases, data can be [fetched](../query/fetch.md) from the first table and then 
inserted into another table, but this results in transfers back and forth between the 
database and the local system.
Instead, data can be inserted from one table into another without transfers between the 
database and the local system using [queries](../query/principles.md).

In the example below, a new schema has been created in preparation for phase two of a 
project.
Experimental protocols from the first phase of the project will be reused in the second 
phase.
Since the entities are already present on the database in the `Protocol` table of the 
`phase_one` schema, we can perform a server-side insert into `phase_two.Protocol` 
without fetching a local copy.

```python
# Server-side inserts are faster...
phase_two.Protocol.insert(phase_one.Protocol)

# ...than fetching before inserting
protocols = phase_one.Protocol.fetch()
phase_two.Protocol.insert(protocols)
```
