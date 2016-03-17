"""
Sample schema with realistic tables for testing
"""

import datajoint as dj
from . import PREFIX, CONN_INFO
from . import schema as _ # make sure that the other tables are defined

schema = dj.schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))

