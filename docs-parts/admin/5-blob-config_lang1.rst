.. code-block:: python

  dj.config['stores'] = {
    'external': dict(  # 'regular' external storage for this pipeline
                  protocol='s3',
                  endpoint='s3.amazonaws.com:9000',
                  bucket = 'testbucket',
                  location = 'datajoint-projects/lab1',
                  access_key='1234567',
                  secret_key='foaf1234'),
    'external-raw': dict( # 'raw' storage for this pipeline
                  protocol='file',
                  location='/net/djblobs/myschema')
  }
  # external object cache - see fetch operation below for details.
  dj.config['cache'] = '/net/djcache'

