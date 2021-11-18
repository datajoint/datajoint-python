
To plot the Diagram for an entire schema, an Diagram object can be initialized with the schema object (which is normally used to decorate table objects)

.. code-block:: python

    import datajoint as dj
    schema = dj.Schema('my_database')
    dj.Diagram(schema).draw()

or alternatively an object that has the schema object as an attribute, such as the module defining a schema:

.. code-block:: python

    import datajoint as dj
    import seq    # import the sequence module defining the seq database
    dj.Diagram(seq).draw()   # draw the Diagram

Note that calling the ``.draw()`` method is not necessary when working in a Jupyter notebook.
The preferred workflow is to simply let the object display itself, for example by writing ``dj.Diagram(seq)``.
The Diagram will then render in the notebook using its ``_repr_html_`` method.
An Diagram displayed without ``.draw()`` will be rendered as an SVG, and hovering the mouse over a table will reveal a compact version of the output of the ``.describe()`` method.
