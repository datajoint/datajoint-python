import os
import connection


connObj = None

def conn(host=None, user=None, passwd=None, initFun=None):
    """
    Manage a persistent connection object. 
    This is one of several ways to configure and access a datajoint connection. 
    Users may customize their own connection manager.
    """

    global connObj
    if not connObj:
        host = host or os.getenv('DJ_HOST') or raw_input('Enter datajoint server address >>')
        user = user or os.getenv('DJ_USER') or raw_input('Enter datajoint user name >>')
        passwd = passwd or os.getenv('DJ_PASS') or raw_input('Enter datajoint password >>')
        initFun = initFun or os.getenv('DJ_INIT')
        connObj = connection.Connection(host, user, passwd, initFun)

    return connObj
