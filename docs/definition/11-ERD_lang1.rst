
|python| Python
+++++++++++++++

To plot the ERD for an entire schema in Python, an ERD object can be initialized with the schema object (which is normally used to decorate table objects)

.. code-block:: python

    import datajoint as dj
    schema = dj.schema('my_database')
    dj.ERD(schema).draw()

or, alternatively an object that has the schema object as an attribute, such as the module defining a schema:

.. code-block:: python

    import datajoint as dj
    import seq    # import the sequence module defining the seq database
    dj.ERD(seq).draw()   # draw the ERD

