
For example, if a Python process is interrupted via the keyboard, a KeyboardError will be logged to the database as follows:

.. code-block:: text

    In [2]: schema.jobs
    Out[2]:
    *table_name    *key_hash      status     error_message  user           host           pid       connection_id  timestamp      key        error_stack
    +------------+ +------------+ +--------+ +------------+ +------------+ +------------+ +-------+ +------------+ +------------+ +--------+ +------------+
    __job_results  3416a75f4cea91 error      KeyboardInterr datajoint@localhos localhost     15571     59             2017-09-04 14: <BLOB>     <BLOB>
     (1 tuples)
