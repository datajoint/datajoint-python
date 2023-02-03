# Query Caching

Query caching allows avoiding repeated queries to the database by caching the results
locally for faster retrieval.

To enable queries, set the query cache local path in `dj.config`, create the directory,
and activate the query caching.

``` python
dj.config['query_cache'] = os.path.expanduser('~/dj_query_cache') # (1)
# (2)
conn = dj.conn()                # if queries co-located with tables
conn = module.schema.connection # if schema co-located with tables
conn = module.table.connection  # most flexible

conn.set_query_cache(query_cache='main') # (3)
```

1. Set the query cache path
2. Access the active connection object for the tables
3. Activate query caching for a namespace called 'main'

The `query_cache` argument is an arbitrary string serving to differentiate cache states;
setting a new value will effectively start a new cache, triggering retrieval of new
values once.

To turn off query caching, use the following:

``` python
conn.set_query_cache(query_cache=None)
## OR
conn.set_query_cache()
```

While query caching is enabled, any insert or delete calls and any transactions are
disabled and will raise an error. This ensures that stale data are not used for
updating the database in violation of data integrity.

To clear and remove the query cache, use the following:

``` python
conn.purge_query_cache() # Purge the cached queries
```
