
## Insert

Data entry is as easy as providing the appropriate data structure to a permitted table.
Given the following table definition, we can insert data as tuples, dicts, pandas
dataframes, or pathlib `Path` relative paths to local CSV files.

```text      
    mouse_id: int            # unique mouse id
    ---
    dob: date                # mouse date of birth
    sex: enum('M', 'F', 'U') # sex of mouse - Male, Female, or Unknown
``` 

=== "Tuple"

    ```python
    mouse.insert1( (0, '2017-03-01', 'M') ) # Single entry
    data = [
        (1, '2016-11-19', 'M'),
        (2, '2016-11-20', 'U'),
        (5, '2016-12-25', 'F')
    ]
    mouse.insert(data) # Multi-entry
    ```

=== "Dict"

    ```python
    mouse.insert1( dict(mouse_id=0, dob='2017-03-01', sex='M') ) # Single entry
    data = [
        {'mouse_id':1, 'dob':'2016-11-19', 'sex':'M'},
        {'mouse_id':2, 'dob':'2016-11-20', 'sex':'U'},
        {'mouse_id':5, 'dob':'2016-12-25', 'sex':'F'}
    ]
    mouse.insert(data) # Multi-entry
    ```

=== "Pandas"

    ```python
    import pandas as pd
    data = pd.DataFrame(
        [[1, "2016-11-19", "M"], [2, "2016-11-20", "U"], [5, "2016-12-25", "F"]],
        columns=["mouse_id", "dob", "sex"],
    )
    mouse.insert(data)
    ```

=== "CSV"

    Given the following CSV in the current working directory as `mice.csv`
    
    ```console
    mouse_id,dob,sex
    1,2016-11-19,M
    2,2016-11-20,U
    5,2016-12-25,F
    ```
    
    We can import as follows:
    
    ```python
    from pathlib import Path
    mouse.insert(Path('./mice.csv'))
    ```
    
## Make

See the article on [`make` methods](../../reproduce/make-method/)

## Fetch

### Entire table

A `fetch` command can either retrieve table data as a NumPy
[recarray](https://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html)
or a as a list of `dict`

``` python
data = query.fetch() # (1)
data = query.fetch(as_dict=True) # (2)
```

1. NumPy recarray
2. List of `dict`:

??? Note "For very large tables..."

    In some cases, the amount of data returned by fetch can be quite large; it can be
    useful to use the `size_on_disk` attribute to determine if running a bare fetch
    would be wise. Please note that it is only currently possible to query the size of
    entire tables stored directly in the database at this time.

### Separate variables

``` python
name, img = query.fetch1('mouse_id', 'dob')  # when query has exactly one entity
name, img = query.fetch('mouse_id', 'dob')   # [mouse_id, ...] [dob, ...]
```

### Primary key values

``` python
keydict = tab.fetch1("KEY")  # single key dict when tab has exactly one entity
keylist = tab.fetch("KEY")   # list of key dictionaries [{}, ...]
```

`KEY` can also used when returning attribute values as separate
variables, such that one of the returned variables contains the entire
primary keys.

### Sorting results

To sort the result, use the `order_by` keyword argument.

``` python
data = query.fetch(order_by='mouse_id')                # ascending order
data = query.fetch(order_by='mouse_id desc')           # descending order
data = query.fetch(order_by=('mouse_id', 'dob'))       # by ID first, dob second
data = query.fetch(order_by='KEY')                     # sort by the primary key
```

The `order_by` argument can be a string specifying the attribute to sort by. By default
the sort is in ascending order. Use `'attr desc'` to sort in descending order by
attribute `attr`. The value can also be a sequence of strings, in which case, the sort
performed on all the attributes jointly in the order specified.

The special attribute named `'KEY'` represents the primary key attributes in order that
they appear in the index. Otherwise, this name can be used as any other argument.

If an attribute happens to be a SQL reserved word, it needs to be enclosed in
backquotes. For example:

``` python
data = query.fetch(order_by='`select` desc')
```

The `order_by` value is eventually passed to the `ORDER BY`
[clause](https://dev.mysql.com/doc/refman/5.7/en/order-by-optimization.html).

### Limiting results

Similar to sorting, the `limit` and `offset` arguments can be used to limit the result
to a subset of entities.

``` python
data = query.fetch(order_by='mouse_id', limit=10, offset=5)
```

Note that an `offset` cannot be used without specifying a `limit` as
well.

### Usage with Pandas

The `pandas` [library](http://pandas.pydata.org/) is a popular library for data analysis
in Python which can easily be used with DataJoint query results. Since the records
returned by `fetch()` are contained within a `numpy.recarray`, they can be easily
converted to `pandas.DataFrame` objects by passing them into the `pandas.DataFrame`
constructor. For example:

``` python
import pandas as pd
frame = pd.DataFrame(tab.fetch())
```

Calling `fetch()` with the argument `format="frame"` returns results as
`pandas.DataFrame` objects indexed by the table's primary key attributes.

``` python
frame = tab.fetch(format="frame")
```

Returning results as a `DataFrame` is not possible when fetching a particular subset of
attributes or when `as_dict` is set to `True`.

<!-- ## Drop and ## Diagrams are mentioned in general docs here -->
