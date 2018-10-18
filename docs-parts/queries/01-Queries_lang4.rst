
The ``bool`` function applied to a query object evaluates to ``True`` if the query returns any entities and to ``False`` if the query result is empty.

The ``len`` function applied to a query object determines the number of entities returned by the query.

.. code-block:: python

    # number of sessions since the start of 2018.
    n = len(Session & 'session_date >= "2018-01-01"')

