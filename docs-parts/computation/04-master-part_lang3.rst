
.. code-block:: python

  @schema
  class ArrayResponse(dj.Computed):
  definition = """
  array_id: int
  """

    class ElectrodeResponse(dj.Part):
    definition = """
    -> master
    electrode_id: int
    """

    class ChannelResponse(dj.Part):
    definition = """
    -> ElectrodeResponse
    channel_id: int
    ---
    response: longblob  # response of a channel
    """
