import pymysql
from . import conn, config
from getpass import getpass
from .utils import user_choice


def set_password(new_password=None, connection=None, update_config=None):   # pragma: no cover
    connection = conn() if connection is None else connection
    if new_password is None:
        new_password = getpass('New password: ')
        confirm_password = getpass('Confirm password: ')
        if new_password != confirm_password:
            print('Failed to confirm the password! Aborting password change.')
            return
    connection.query("SET PASSWORD = PASSWORD('%s')" % new_password)
    print('Password updated.')

    if update_config or (update_config is None and user_choice('Update local setting?') == 'yes'):
        config['database.password'] = new_password
        config.save_local(verbose=True)


def kill(restriction=None, connection=None):  # pragma: no cover
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
        cur = connection.query(query, as_dict=True)
        for process in cur:
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
