.. code-block:: python

   # default external storage
   dj.config['external'] = dict(
                 protocol='s3',
                 endpoint='https://s3.amazonaws.com',
                 bucket = 'testbucket',
                 location = '/datajoint-projects/myschema',
                 access_key='1234567',
                 secret_key='foaf1234')

   # raw data storage
   dj.config['extnernal-raw'] = dict(
                 protocol='file',
                 location='/net/djblobs/myschema')

   # external object cache - see fetch operation below for details.
   dj.config['cache'] = dict(
                 protocol='file',
                 location='/net/djcache')
