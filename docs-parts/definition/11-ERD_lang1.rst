
To plot the ERD for an entire schema, an ERD object can be initialized with the schema object (which is normally used to decorate table objects)

.. code-block:: python

    import datajoint as dj
    schema = dj.Schema('my_database')
    dj.ERD(schema).draw()

or alternatively an object that has the schema object as an attribute, such as the module defining a schema:

.. code-block:: python

    import datajoint as dj
    import seq    # import the sequence module defining the seq database
    dj.ERD(seq).draw()   # draw the ERD

Note that calling the ``.draw()`` method is not necessary when working in a Jupyter notebook.
The preferred workflow is to simply let the object display itself, for example by writing ``dj.ERD(seq)``.
The ERD will then render in the notebook using its ``_repr_html_`` method.
An ERD displayed without ``.draw()`` will be rendered as an SVG, and hovering the mouse over a table will reveal a compact version of the output of the ``.describe()`` method.
