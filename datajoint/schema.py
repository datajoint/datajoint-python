import conn


class Schema:
    """
    datajoint.Schema objects link a python module with a database schema.

    """
    def __init__(self, dbName, conn=conn.conn()):
        conn.activateSchema(self,dbName)

