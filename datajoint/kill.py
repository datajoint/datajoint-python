import pymysql
from . import conn


def kill(restriction=None, connection=None):
    """
    view and kill database connections.
    :param restriction: restriction to be applied to processlist
    :param connection: a datajoint.Connection object. Default calls datajoint.conn()

    Restrictions are specified as strings and can involve any of the attributes of
    information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.

    Examples:
        dj.kill('HOST LIKE "%compute%"') lists only connections from hosts containing "compute".
        dj.kill('TIME > 600') lists only connections older than 10 minutes.
    """

    if connection is None:
        connection = conn()

    query = 'SELECT * FROM information_schema.processlist WHERE id <> CONNECTION_ID()' + (
        "" if restriction is None else ' AND (%s)' % restriction)

    while True:
        print('  ID USER         STATE         TIME  INFO')
        print('+--+ +----------+ +-----------+ +--+')
        for process in connection.query(query, as_dict=True).fetchall():
            try:
                print('{ID:>4d} {USER:<12s} {STATE:<12s} {TIME:>5d}  {INFO}'.format(**process))
            except TypeError:
                print(process)
        response = input('process to kill or "q" to quit > ')
        if response == 'q':
            break
        if response:
            try:
                pid = int(response)
            except ValueError:
                pass  # ignore non-numeric input
            else:
                try:
                    connection.query('kill %d' % pid)
                except pymysql.err.InternalError:
                    print('Process not found')
