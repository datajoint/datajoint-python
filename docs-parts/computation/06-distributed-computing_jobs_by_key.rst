
This can be done by using `dj.key_hash` to convert the key as follows:

.. code-block:: python

    In [4]: jk = {'table_name': JobResults.table_name, 'key_hash' : dj.key_hash({'id': 2})}
    In [5]: schema.jobs & jk
    Out[5]: 
    *table_name    *key_hash      status     key        error_message  error_stac user           host      pid        connection_id  timestamp     
    +------------+ +------------+ +--------+ +--------+ +------------+ +--------+ +------------+ +-------+ +--------+ +------------+ +------------+
    __job_results  c81e728d9d4c2f error      =BLOB=     KeyboardInterr =BLOB=     datajoint@localhost  localhost     15571     59             2017-09-04 14:
     (Total: 1)
    
    In [6]: (schema.jobs & jk).delete()     
    
    In [7]: schema.jobs & jk
    Out[7]: 
    *table_name    *key_hash    status     key        error_message  error_stac user     host     pid     connection_id  timestamp    
    +------------+ +----------+ +--------+ +--------+ +------------+ +--------+ +------+ +------+ +-----+ +------------+ +-----------+
    
     (Total: 0)

