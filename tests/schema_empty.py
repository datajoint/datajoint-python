"""
Sample schema with realistic tables for testing
"""

import datajoint as dj
from . import PREFIX, CONN_INFO
from . import schema as _ # make sure that the other tables are defined

schema = dj.Schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))


@schema
class Ephys(dj.Imported):
    definition = """  # This is already declare in ./schema.py
    """

schema.spawn_missing_classes()    # load the rest of the classes