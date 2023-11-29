import datajoint as dj
from datajoint.user_tables import UserTable
from . import CONN_INFO


def test_virtual_module(schema_obj):
    module = dj.VirtualModule(
        "module", schema_obj.schema.database, connection=dj.conn(**CONN_INFO)
    )
    assert issubclass(module.Experiment, UserTable)
