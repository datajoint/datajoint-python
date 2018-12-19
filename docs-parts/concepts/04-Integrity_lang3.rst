.. code-block:: python

  @schema
  class Mouse(dj.Manual):
    definition = """
    mouse_name : varchar(64)
    ---
    mouse_dob : datetime
    """

  @schema
  class SubjectGroup(dj.Manual):
    definition = """
    group_number : int
    ---
    group_name : varchar(64)
    """

    class GroupMember(dj.Part):
      definition = """
      -> master
      -> Mouse
      """
