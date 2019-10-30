
To remove only the tracking entries in the external table, call ``delete``
on the ``~external_<storename>`` table for the external configuration with the argument 
``delete_external_files=False``.

.. note::

  Currently, cleanup operations on a schema's external table are not 100% 
  transaction safe and so must be run when there is no write activity occurring 
  in tables which use a given schema / external store pairing.

.. code-block:: python

    >>> schema.external['external_raw'].delete(delete_external_files=False)

To remove the tracking entries as well as the underlying files, call `delete`
on the external table for the external configuration with the argument
`delete_external_files=True`.

.. code-block:: python

    >>> schema.external['external_raw'].delete(delete_external_files=True)

.. note::

  Setting ``delete_external_files=True`` will always attempt to delete
  the underlying data file, and so should not typically be used with 
  the ``filepath`` datatype.

