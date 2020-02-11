from nose.tools import assert_true
import datajoint as dj
from datajoint.user_tables import UserTable
from . import schema
from . import CONN_INFO


def test_virtual_module():
    module = dj.VirtualModule('module', schema.schema.database, connection=dj.conn(**CONN_INFO))
    assert_true(issubclass(module.Experiment, UserTable))