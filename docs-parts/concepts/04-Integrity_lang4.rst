.. code-block:: python

  @schema
  class RecordingModality(dj.Lookup):
    definition = """
    modality : varchar(64)
    """

  @schema
  class MultimodalSession(dj.Manual):
    definition = """
    -> Session
    modes : int
    """

    class SessionMode(dj.Part):
      definition = """
      -> master
      -> RecordingModality
      """
