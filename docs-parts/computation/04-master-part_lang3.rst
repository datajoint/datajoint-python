
.. code-block:: python

  @schema
  class ArrayResponse(dj.Computed):
  definition = """
  array: int
  """

    class ElectrodeResponse(dj.Part):
    definition = """
    -> master
    electrode: int
    """

    class ChannelResponse(dj.Part):
    definition = """
    -> ElectrodeResponse
    channel: int
    ---
    response: longblob  # response of a channel
    """
