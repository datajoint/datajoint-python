import os
from .base_relation import BaseRelation
from . import __version__ as version


class Log(BaseRelation):
    """
    A base relation with no definition. Allows reserving jobs
    """

    def __init__(self, arg, database=None):
        super().__init__()

        if isinstance(arg, Log):
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            self._definition = arg._definition
            self._user = arg._user
            return

        self.database = database
        self._connection = arg
        self._definition = """    # job reservation table for `{database}`
        timestamp  : timestamp(3)
        ---
        version  :varchar(12)   # datajoint version
        user     :varchar(255)  # user@host
        host=""  :varchar(255)  # system hostname
        event="" :varchar(255)  # custom message
        """.format(database=database)

        if not self.is_declared:
            self.declare()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    @property
    def table_name(self):
        return '~log'

    def __call__(self, event):
        self.insert1(dict(
            user=self._user,
            version=version+'py',
            host=os.uname().nodename,
            event=event), replace=True, ignore_extra_fields=True)

    def delete(self):
        """bypass interactive prompts and dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and dependencies"""
        self.drop_quick()


