
|python| Python
---------------

In Python there is a separate method ``insert1`` to insert one entity at a time.
The entity may have the form of a Python dictionary with key names matching the attribute names in the table.

.. code-block:: python

    lab.Person.insert1(
           dict(username='alice',
                first_name='Alice',
                last_name='Cooper'))

The entity also may take the form of a sequence of values in the same order as the attributes in the table.

.. code-block:: python

    lab.Person.insert1(['alice', 'Alice', 'Cooper'])

Additionally, the entity may be inserted as a `numpy.record <https://docs.scipy.org/doc/numpy/reference/generated/numpy.record.html#numpy.record>`_.

The ``insert`` method accepts a sequence or a generator of multiple entities and is used to insert multiple entities at once.

.. code-block:: python

    lab.Person.insert([
           ['alice',   'Alice',   'Cooper'],
           ['bob',     'Bob',     'Dylan'],
           ['carol',   'Carol',   'Douglas']])
