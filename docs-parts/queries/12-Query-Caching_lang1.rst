
.. code-block:: python

  # set the query cache path
  dj.config['query_cache'] = os.path.expanduser('~/dj_query_cache')

  # access the active connection object for the tables
  conn = dj.conn() # if queries co-located with tables
  conn = module.schema.connection # if schema co-located with tables
  conn = module.table.connection # most flexible

  # activate query caching for a namespace called 'main'
  conn.set_query_cache(query_cache='main')
