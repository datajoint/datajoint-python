.. code-block:: python

  @schema
  class EEGRecording(dj.Manual):
    definition = """
    -> Session
    eeg_recording_id : int
    ---
    eeg_system : varchar(64)
    num_channels : int
    """

  @schema
  class ChannelData(dj.Imported):
    definition = """
    -> EEGRecording
    channel_idx : int
    ---
    channel_data : longblob
    """
