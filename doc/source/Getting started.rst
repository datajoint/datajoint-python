
Create a database object
========================

First, import datajoint, set environmental variables and define the
database. The definition of the database is usually done in another
file.

This tests some latex :math:`x \mapsto y`.

.. code:: python

    from os import environ
    environ.get('DJ_HOST', 'localhost')
    environ.get('DJ_USER', 'datajoint')
    environ.get('DJ_PASSW', 'datajoint')
    import datajoint as dj
    
    class Subjects(dj.Relation):
        _table_def = """
        Subjects (manual)     # Basic subject info
    
        subject_id       : int      # unique subject id
        ---
        real_id                     :  varchar(40)    #  real-world name
        species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
        """
