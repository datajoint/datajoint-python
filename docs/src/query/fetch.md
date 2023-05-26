# Query Objects

**Data queries** retrieve data from the database. A data query is performed with the
  help of a **query object**, which is a symbolic representation of the query that does
  not in itself contain any actual data. The simplest query object is an instance of
  a **table class**, representing the contents of an entire table.

## Querying a database

For example, if given a `Session` table, you can
create a query object to retrieve its entire contents as follows:

``` python
query  = Session()
```

More generally, a query object may be formed as a **query expression**
constructed by applying [operators](./operators.md) to other query objects.

For example, the following query retrieves information about all
experiments and scans for mouse 001:

``` python
query = Session * Scan & 'animal_id = 001'
```

Note that for brevity, query operators can be applied directly to class, as
`Session` instead of `Session()`.

Alternatively, we could query all scans with a sample rate over 1000, and preview the
contents of the query simply displaying the object. 

``` python
Scan & 'sample_rate > 1000'
```

The above command shows the following table:

    ```text
    | id* |    start_time*      | sample_rate | signal |  times | duration |
    |-----|---------------------|-------------|--------|--------|----------| 
    |  1  | 2020-01-02 22:15:00 |   1893.00   | =BLOB= | =BLOB= |  1981.29 |
    |  2  | 2020-01-03 00:15:00 |   4800.00   | =BLOB= | =BLOB= |   548.0  |
    |  3  | 2020-01-19 14:03:03 |   4800.00   | =BLOB= | =BLOB= |   336.0  |
    |  4  | 2020-01-19 14:13:03 |   4800.00   | =BLOB= | =BLOB= |  2501.0  |
    |  5  | 2020-01-23 11:05:23 |   4800.00   | =BLOB= | =BLOB= |  1800.0  |
    |  6  | 2020-01-27 14:03:03 |   4800.00   | =BLOB= | =BLOB= |   600.0  |
    |  7  | 2020-01-31 20:15:00 |   4800.00   | =BLOB= | =BLOB= |   600.0  |
    ...
    11 tuples
    ```

Note that this preview (a) only lists a few of the entities that will be returned and 
(b) does not contain any data for attributes of datatype `blob`.

Once the desired query object is formed, the query can be executed using its [fetch]
(./fetch) methods. To **fetch** means to transfer the data represented by the query
object from the database server into the workspace of the host language.

```python
query = Scan & 'sample_rate > 1000'
s = query.fetch()
```

Here fetching from the `query` object produces the NumPy record array
`s` of the queried data.

## Checking for entities

The preview of the query object shown above displayed only a few of the entities
returned by the query but also displayed the total number of entities that would be
returned. It can be useful to know the number of entities returned by a query, or even
whether a query will return any entities at all, without having to fetch all the data
themselves.

The `bool` function applied to a query object evaluates to `True` if the
query returns any entities and to `False` if the query result is empty.

The `len` function applied to a query object determines the number of
entities returned by the query.

``` python
# number of sessions since the start of 2018.
n = len(Session & 'session_date >= "2018-01-01"')
```

## Normalization in queries

Query objects adhere to entity [entity normalization](../design/normalization). The result of a
query will include the uniquely defining attributes jointly distinguish any two
entities from each other. The query [operators](./operators) are designed to keep the
result normalized even in complex query expressions.
