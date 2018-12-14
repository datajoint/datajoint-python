
To define a DataJoint table in Python:

1. Define a class inheriting from the appropriate DataJoint class: ``dj.Lookup``, ``dj.Manual``, ``dj.Imported`` or ``dj.Computed``.

2. Decorate the class with the schema object (see :ref:`schema`)

3. Define the class property ``definition`` to define the table heading.

For example, the following code defines the table ``Person``:

.. code-block:: python

	import datajoint as dj
	schema = dj.schema('alice_experiment')

	@schema
	class Person(dj.Manual):
	    definition = '''
			username : varchar(20)   # unique user name
	    ---
	    first_name : varchar(30)
	    last_name  : varchar(30)
	    '''


The ``@schema`` decorator uses the class name and the data tier to check whether an appropriate table exists on the database.
If a table does not already exist, the decorator creates one on the database using the definition property.
The decorator attaches the information about the table to the class, and then returns the class.

The class will become usable after you define the ``definition`` property as described in :ref:`definitions`.

DataJoint classes in Python
^^^^^^^^^^^^^^^^^^^^^^^^^^^

DataJoint for Python is implemented through the use of classes.
Working with classes usually implies that one might create different class instances with various internal states.
However, DataJoint classes only serve as interfaces to data that actually reside within tables on the database server.
Whether calling a DataJoint method on a class or on an instance, the result will only depend on or apply to the corresponding table.
All of the basic functionality of DataJoint is built to operate on the classes themselves, even when called on an instance.
For example, calling ``Person.insert(...)`` (on the class) and ``Person.insert(...)`` (on an instance) both have the identical effect of inserting data into the table on the database server.
DataJoint does not prevent a user from working with instances, but the workflow is complete without the need for instantiation.
It is up to the user whether to implement additional functionality as class methods or methods called on instances.
