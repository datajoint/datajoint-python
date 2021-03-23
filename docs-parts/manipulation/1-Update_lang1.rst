
.. code-block:: python

  # with record as a dict specifying the primary and
  # secondary attribute values
  table.update1(record)

  # update value in record with id as primary key
  table.update1({'id': 1, 'value': 3})

  # reset value to default with id as primary key
  table.update1({'id': 1, 'value': None})
  ## OR
  table.update1({'id': 1})
