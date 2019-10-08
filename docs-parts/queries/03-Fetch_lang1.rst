
Entire table
~~~~~~~~~~~~

The following statement retrieves the entire table as a NumPy `recarray <https://docs.scipy.org/doc/numpy/reference/generated/numpy.recarray.html>`_.

.. code-block:: python

    data = query.fetch()

To retrieve the data as a list of ``dict``:

.. code-block:: python

    data = query.fetch(as_dict=True)

In some cases, the amount of data returned by fetch can be quite large; in these cases it can be useful to use the ``size_on_disk`` attribute to determine if running a bare fetch would be wise.
Please note that it is only currently possible to query the size of entire tables stored directly in the database at this time.

As separate variables
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    name, img = query.fetch1('name', 'image')  # when query has exactly one entity
    name, img = query.fetch('name', 'image')  # [name, ...] [image, ...]

Primary key values
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    keydict = tab.fetch1("KEY")  # single key dict when tab has exactly one entity
    keylist = tab.fetch("KEY")  # list of key dictionaries [{}, ...]

``KEY`` can also used when returning attribute values as separate variables, such that one of the returned variables contains the entire primary keys.

Usage with Pandas
~~~~~~~~~~~~~~~~~

The ``pandas`` `library <http://pandas.pydata.org/>`_ is a popular library for data analysis in Python which can easily be used with DataJoint query results.
Since the records returned by ``fetch()`` are contained within a ``numpy.recarray``, they can be easily converted to ``pandas.DataFrame`` objects by passing them into the ``pandas.DataFrame`` constructor.
For example:

.. code-block:: python

    import pandas as pd
    frame = pd.DataFrame(tab.fetch())

Calling ``fetch()`` with the argument ``format="frame"`` returns results as ``pandas.DataFrame`` objects with no need for conversion.

.. code-block:: python

  frame = tab.fetch(format="frame")

Returning results as a ``DataFrame`` is not possible when fetching a particular subset of attributes or when ``as_dict`` is set to ``True``.
