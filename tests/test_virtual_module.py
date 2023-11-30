import datajoint as dj
from datajoint.user_tables import UserTable


def test_virtual_module(schema_obj, connection_test):
    module = dj.VirtualModule(
        "module", schema_obj.schema.database, connection=dj.conn(connection_test)
    )
    assert issubclass(module.Experiment, UserTable)
