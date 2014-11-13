__author__ = 'eywalker'
from . import CONN_INFO, PREFIX, BASE_CONN
from .schemas import test1
import datajoint as dj

def test_establish_connection():
    "Should be able to establish a connection"
    c = dj.conn(**CONN_INFO)
    assert c.isConnected


class TestConnection(object):
    def __init__(self):
        self.conn = dj.conn(**CONN_INFO)

    def check_binding(self, dbname, module):
        "Check if database-module pairing exists"
        assert self.conn.modules[dbname] == module
        assert self.conn.dbnames[module] == dbname

    def test_bind_to_existing_database(self):
        "Should be able to bind a module to an existing database"
        dbname= PREFIX + '_test1'
        module = test1.__name__
        self.conn.bind(module, dbname)
        self.check_binding(dbname, module)

    def test_bind_to_non_existing_database(self):
        "Should be able to bind a module to a non-existing database by creating target"
        dbname = PREFIX + '_test3'
        module = test1.__name__
        cur = BASE_CONN.cursor()

        # Ensure target database doesn't exist
        cur.execute("DROP DATABASE IF EXISTS `{}`".format(dbname))
        # Bind module to non-existing database
        self.conn.bind(module, dbname)
        # Check that target database was created
        assert cur.execute("SHOW DATABASES LIKE '{}'".format(dbname)) == 1
        self.check_binding(dbname, module)
        # Remove the target database
        cur.execute("DROP DATABASE IF EXISTS `{}`".format(dbname))







