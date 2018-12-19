
.. code-block:: python

  # Server-side inserts are faster...
  phase_two.Protocol.insert(phase_one.Protocol)

  # ...than fetching before inserting
  protocols = phase_one.Protocol.fetch()
  phase_two.Protocol.insert(protocols)
