
The table definition is contained in the ``definition`` property of the class.

.. code-block:: python

	@schema
	class User(dj.Manual):
	    definition = """
	    # database users
	    username : varchar(20)   # unique user name
	    ---
	    first_name : varchar(30)
	    last_name  : varchar(30)
	    role : enum('admin', 'contributor', 'viewer')
	    """
