# Fetch

Data queries in DataJoint comprise two distinct steps:

1. Construct the `query` object to represent the required data using tables and 
[operators](operators.md).
2. Fetch the data from `query` into the workspace of the host language -- described in 
this section.

Note that entities returned by `fetch` methods are not guaranteed to be sorted in any 
particular order unless specifically requested.
Furthermore, the order is not guaranteed to be the same in any two queries, and the 
contents of two identical queries may change between two sequential invocations unless 
they are wrapped in a transaction.
Therefore, if you wish to fetch matching pairs of attributes, do so in one `fetch` call.

The examples below are based on the [example schema](example-schema.md) for this part 
of the documentation.

## Entire table

The following statement retrieves the entire table as a NumPy 
[recarray](https://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html).

```python
data = query.fetch()
```

To retrieve the data as a list of `dict`:

```python
data = query.fetch(as_dict=True)
```

In some cases, the amount of data returned by fetch can be quite large; in these cases 
it can be useful to use the `size_on_disk` attribute to determine if running a bare 
fetch would be wise.
Please note that it is only currently possible to query the size of entire tables 
stored directly in the database at this time.

## As separate variables

```python
name, img = query.fetch1('name', 'image')  # when query has exactly one entity
name, img = query.fetch('name', 'image')  # [name, ...] [image, ...]
```

## Primary key values

```python
keydict = tab.fetch1("KEY")  # single key dict when tab has exactly one entity
keylist = tab.fetch("KEY")  # list of key dictionaries [{}, ...]
```

`KEY` can also used when returning attribute values as separate variables, such that 
one of the returned variables contains the entire primary keys.

## Sorting and limiting the results

To sort the result, use the `order_by` keyword argument.

```python
# ascending order:
data = query.fetch(order_by='name')
# descending order:
data = query.fetch(order_by='name desc')  
# by name first, year second:
data = query.fetch(order_by=('name desc', 'year'))
# sort by the primary key:
data = query.fetch(order_by='KEY')
# sort by name but for same names order by primary key:
data = query.fetch(order_by=('name', 'KEY desc'))
```

The `order_by` argument can be a string specifying the attribute to sort by. By default 
the sort is in ascending order. Use `'attr desc'` to sort in descending order by 
attribute `attr`.  The value can also be a sequence of strings, in which case, the sort 
performed on all the attributes jointly in the order specified.

The special attribute name `'KEY'` represents the primary key attributes in order that 
they appear in the index. Otherwise, this name can be used as any other argument.

If an attribute happens to be a SQL reserved word, it needs to be enclosed in 
backquotes.  For example:

```python
data = query.fetch(order_by='`select` desc')
```

The `order_by` value is eventually passed to the `ORDER BY` 
[clause](https://dev.mysql.com/doc/refman/5.7/en/order-by-optimization.html).

Similarly, the `limit` and `offset` arguments can be used to limit the result to a 
subset of entities.

For example, one could do the following:

```python
data = query.fetch(order_by='name', limit=10, offset=5)
```

Note that an `offset` cannot be used without specifying a `limit` as well. 

## Usage with Pandas

The [pandas library](http://pandas.pydata.org/) is a popular library for data analysis 
in Python which can easily be used with DataJoint query results.
Since the records returned by `fetch()` are contained within a `numpy.recarray`, they 
can be easily converted to `pandas.DataFrame` objects by passing them into the 
`pandas.DataFrame` constructor.
For example:

```python
import pandas as pd
frame = pd.DataFrame(tab.fetch())
```

Calling `fetch()` with the argument `format="frame"` returns results as 
`pandas.DataFrame` objects indexed by the table's primary key attributes.

```python
frame = tab.fetch(format="frame")
```

Returning results as a `DataFrame` is not possible when fetching a particular subset of 
attributes or when `as_dict` is set to `True`.
