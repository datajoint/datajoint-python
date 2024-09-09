import datajoint as dj
from datajoint.user_tables import UserTable


def test_virtual_module(schema_any, connection_test):
    module = dj.VirtualModule("module", schema_any.database, connection=connection_test)
    assert issubclass(module.Experiment, UserTable)
