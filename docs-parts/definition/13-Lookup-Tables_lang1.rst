
.. code-block:: python

    @schema
    class User(dj.Lookup):
        definition = """
        # users in the lab
        username : varchar(20)   # user in the lab
        ---
        first_name  : varchar(20)   # user first name
        last_name   : varchar(20)   # user last name
        """
        contents = [
            ['cajal', 'Santiago', 'Cajal'],
            ['hubel', 'David', 'Hubel'],
            ['wiesel', 'Torsten', 'Wiesel']
    ]
