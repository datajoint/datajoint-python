__author__ = 'eywalker'
import datajoint as dj
conn = dj.conn()
conn.bind(__name__, 'dj_test_schema1')
print(__name__)
from .test1 import *
from .test2 import *
from .test3 import *