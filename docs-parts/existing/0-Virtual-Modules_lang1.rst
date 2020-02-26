The class object ``VirtualModule`` of the ``dj.Schema`` class provides access to virtual modules.
It creates a python module with the given name from the name of a schema on the server, automatically adds classes to it corresponding to the tables in the schema.

The function can take several parameters:

  ``module_name``: displayed module name.

  ``schema_name``: name of the database in MySQL.

  ``create_schema``: if ``True``, create the schema on the database server if it does not already exist; if ``False`` (default), raise an error when the schema is not found.

  ``create_tables``: if ``True``, ``module.schema`` can be used as the decorator for declaring new classes; if ``False``, such use will raise an error stating that the module is intend only to work with existing tables.

The function returns the Python module containing classes from the schema object with all the table classes already declared inside it.
