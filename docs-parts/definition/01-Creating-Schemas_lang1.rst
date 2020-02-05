
.. note:: By convention, the ``datajoint`` package is imported as ``dj``.
  The documentation refers to the package as ``dj`` throughout.

Create a new schema using the ``dj.Schema`` class object:

.. code-block:: python

    import datajoint as dj
    schema = dj.Schema('alice_experiment')

This statement creates the database schema ``alice_experiment`` on the server.

The returned object ``schema`` will then serve as a decorator for DataJoint classes, as described in :ref:`table`.

It is a common practice to have a separate Python module for each schema.
Therefore, each such module has only one ``dj.Schema`` object defined and is usually named ``schema``.

The ``dj.Schema`` constructor can take a number of optional parameters after the schema name.

- ``context`` - Dictionary for looking up foreign key references.
  Defaults to ``None`` to use local context.
- ``connection`` - Specifies the DataJoint connection object.
  Defaults to ``dj.conn()``.
- ``create_schema`` - When ``False``, the schema object will not create a schema on the database and will raise an error if one does not already exist.
  Defaults to ``True``.
- ``create_tables`` - When ``False``, the schema object will not create tables on the database and will raise errors when accessing missing tables.
  Defaults to ``True``.
