.. code-block:: python

  @schema
  class Mouse(dj.Manual):
    definition = """
    mouse_name : varchar(64)
    ---
    mouse_dob : datetime
    """

  @schema
  class MouseDeath(dj.Manual):
    definition = """
    -> Mouse
    ---
    death_date : datetime
    """
