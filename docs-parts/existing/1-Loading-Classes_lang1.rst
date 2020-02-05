
This section describes how to work with database schemas without access to the
original code that generated the schema. These situations often arise when the
database is created by another user who has not shared the generating code yet
or when the database schema is created from a programming language other than
Python.

.. code-block:: python

    import datajoint as dj


Working with schemas and their modules
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Typically a DataJoint schema is created as a dedicated Python module. This
module defines a schema object that is used to link classes declared in the
module to tables in the database schema. As an example, examine the university
module: `university.py <https://github.com/vathes/db-programming-with-datajoint/blob/master/notebooks/university.py>`_.

You may then import the module to interact with its tables:

.. code-block:: python

    import university as uni

*Connecting dimitri\@localhost:3306*

.. code-block:: python

    dj.Diagram(uni)

.. figure:: virtual-module-ERD.svg
   :align: center
   :alt: query object preview

.. .. raw:: html
..     :file: virtual-module-ERD.svg

Note that dj.Diagram can extract the diagram from a schema object or from a
Python module containing its schema object, lending further support to the
convention of one-to-one correspondence between database schemas and Python
modules in a DataJoint project:

``dj.Diagram(uni)``

is equvalent to

``dj.Diagram(uni.schema)``

.. code-block:: python

    # students without majors
    uni.Student - uni.StudentMajor

.. figure:: StudentTable.png
   :align: center
   :alt: query object preview

.. .. csv-table::
..    :file: Student_Table.csv
..    :widths: 5, 5, 5, 5, 5, 5, 5, 5, 5, 5
..    :header-rows: 1

Spawning missing classes
~~~~~~~~~~~~~~~~~~~~~~~~
Now imagine that you do not have access to ``university.py`` or you do not have
its latest version. You can still connect to the database schema but you will
not have classes declared to interact with it.

So let's start over in this scenario.

You can may use the ``dj.list_schemas`` function (new in DataJoint 0.12.0) to
list the names of database schemas available to you.

.. code-block:: python

    import datajoint as dj
    dj.list_schemas()

*Connecting dimitri@localhost:3306*

*['dimitri_alter','dimitri_attach','dimitri_blob','dimitri_blobs',
'dimitri_nphoton','dimitri_schema','dimitri_university','dimitri_uuid',
'university']*

Just as with a new schema, we start by creating a schema object to connect to
the chosen database schema:

.. code-block:: python

    schema = dj.Schema('dimitri_university')

If the schema already exists, dj.Schema is initialized as usual and you may plot
the schema diagram. But instead of seeing class names, you will see the raw
table names as they appear in the database.

.. code-block:: python

    # let's plot its diagram
    dj.Diagram(schema)

.. figure:: dimitri-ERD.svg
   :align: center
   :alt: query object preview

.. .. raw:: html
..    :file: dimitri-ERD.svg

You may view the diagram but, at this point, there is no way to interact with
these tables. A similar situation arises when another developer has added new
tables to the schema but has not yet shared the updated module code with you.
Then the diagram will show a mixture of class names and database table names.

Now you may use the ``schema.spawn_missing_classes`` method to spawn classes into
the local namespace for any tables missing their classes:

.. code-block:: python

    schema.spawn_missing_classes()
    dj.Di(schema)

.. figure:: spawned-classes-ERD.svg
   :align: center
   :alt: query object preview

.. .. raw:: html
..    :file: spawned-classes-ERD.svg

Now you may interact with these tables as if they were declared right here in
this namespace:

.. code-block:: python

    # students without majors
    Student - StudentMajor

.. figure:: StudentTable.png
   :align: center
   :alt: query object preview

Creating a virtual module
~~~~~~~~~~~~~~~~~~~~~~~~~
Now ``spawn_missing_classes`` creates the new classes in the local namespace.
However, it is often more convenient to import a schema with its Python module,
equivalent to the Python command

.. code-block:: python

    import university as uni

We can mimick this import without having access to ``university.py`` using the
``VirtualModule`` class object:

.. code-block:: python

    import datajoint as dj

    uni = dj.VirtualModule('university.py', 'dimitri_university')

*Connecting dimitri@localhost:3306*

Now ``uni`` behaves as an imported module complete with the schema object and all
the table classes.

.. code-block:: python

    dj.Di(uni)

.. figure:: added-example-ERD.svg
   :align: center
   :alt: query object preview

.. .. raw:: html
..    :file: added-example-ERD.svg

.. code-block:: python

    uni.Student - uni.StudentMajor

.. figure:: StudentTable.png
   :align: center
   :alt: query object preview

``dj.VirtualModule`` takes optional arguments.

First, ``create_schema=False`` assures that an error is raised when the schema
does not already exist. Set it to ``True`` if you want to create an empty schema.

.. code-block:: python

    dj.VirtualModule('what', 'nonexistent')

.. code-block:: python

    ---------------------------------------------------------------------------
    DataJointError                            Traceback (most recent call last)
    .
    .
    .
    DataJointError: Database named `nonexistent` was not defined. Set argument create_schema=True to create it.


The other optional argument, ``create_tables=False`` is passed to the schema
object. It prevents the use of the schema obect of the virtual module for
creating new tables in the existing schema. This is a precautionary measure
since virtual modules are often used for completed schemas. You may set this
argument to ``True`` if you wish to add new tables to the existing schema. A
more common approach in this scenario would be to create a new schema object and
to use the ``spawn_missing_classes`` function to make the classes available.

However, you if do decide to create new tables in an existing tables using the
virtual module, you may do so by using the schema object from the module as the
decorator for declaring new tables:

.. code-block:: python

    uni = dj.VirtualModule('university.py', 'dimitri_university', create_tables=True)

.. code-block:: python

    @uni.schema
    class Example(dj.Manual):
        definition = """
        -> uni.Student
        ---
        example : varchar(255)
        """

.. code-block:: python

    dj.Di(uni)

.. figure:: added-example-ERD.svg
   :align: center
   :alt: query object preview

.. .. raw:: html
..    :file: added-example-ERD.svg
