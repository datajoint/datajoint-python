
For example, to sort the output by hostname in descending order:

.. code-block:: text

    In [3]: dj.kill(None, None, 'host desc')
    Out[3]:
      ID USER         HOST          STATE         TIME    INFO
    +--+ +----------+ +-----------+ +-----------+ +-----+
      33 chris        localhost:54840                 1261  None
      17 chris        localhost:54587                 3246  None
       4 event_scheduler localhost    Waiting on empty queue  187180  None
    process to kill or "q" to quit > q

