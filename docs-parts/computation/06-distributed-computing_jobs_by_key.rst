
This can be done by using `dj.key_hash` to convert the key as follows:

.. code-block:: python

    In [4]: schema.jobs & {'key_hash' : dj.key_hash({'id': 2})}                
    Out[4]: 
    *table_name    *key_hash      status     key        error_message  error_stac user           host      pid        connection_id  timestamp     
    +------------+ +------------+ +--------+ +--------+ +------------+ +--------+ +------------+ +-------+ +--------+ +------------+ +------------+
    __job_results  c81e728d9d4c2f error      =BLOB=     KeyboardInterr =BLOB=     datajoint@localhost  localhost     15571     59             2017-09-04 14:
     (Total: 1)
    
    In [5]: (schema.jobs & {'key_hash' : dj.key_hash({'id': 2})}).delete()     
    
    In [6]: schema.jobs & {'key_hash' : dj.key_hash({'id': 2})}                
    Out[6]: 
    *table_name    *key_hash    status     key        error_message  error_stac user     host     pid     connection_id  timestamp    
    +------------+ +----------+ +--------+ +--------+ +------------+ +--------+ +------+ +------+ +-----+ +------------+ +-----------+
    
     (Total: 0)

