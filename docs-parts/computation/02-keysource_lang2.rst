.. code-block:: python

  @schema
  class EEG(dj.Imported):
    definition = """
    -> Recording
    ---
    sample_rate : float
    eeg_data : longblob
    """
    key_source = Recording & 'recording_type = "EEG"'
