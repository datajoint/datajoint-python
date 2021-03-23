
.. code-block:: python

  # set the query cache path
  dj.config['query_cache'] = os.path.expanduser('~/dj_query_cache')

  # access the currently active connection object
  conn = dj.conn()
  ## OR
  conn = schema.connection
  ## OR
  conn = table.connection

  # activate query caching for a namespace called 'main'
  conn.set_query_cache(query_cache='main')
