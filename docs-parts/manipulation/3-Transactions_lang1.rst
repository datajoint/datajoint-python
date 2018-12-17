Transactions are formed using the ``transaction`` property of the connection object. 
The connection object may be obtained from any table object.
The ``transaction`` property can then be used as a context manager in Python's ``with`` statement.

For example, the following code inserts matching entries for the master table ``Session`` and its part table ``Session.Experimenter``.

.. code-block:: python

    # get the connection object 
    connection = Session.connection

    # insert Session and Session.Experimenter entries in a transaction
    with connection.transaction:
        key = {'subject_id': animal_id, 'session_time': session_time}
        Session.insert1({**key, 'brain_region':region, 'cortical_layer':layer})
        Session.Experimenter.insert1({**key, 'experimenter': username})

Here, to external observers, both inserts will take effect together upon exiting from the ``with`` block or will not have any effect at all.
For example, if the second insert fails due to an error, the first insert will be rolled back. 

