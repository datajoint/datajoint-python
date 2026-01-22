import logging

import pymysql

from .connection import conn

logger = logging.getLogger(__name__.split(".")[0])


def kill(restriction=None, connection=None, order_by=None):
    """
    View and kill database connections interactively.

    Displays a list of active connections and prompts for connections to kill.

    Parameters
    ----------
    restriction : str, optional
        SQL WHERE clause to filter connections. Can use any attribute from
        information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.
    connection : Connection, optional
        A datajoint.Connection object. Defaults to datajoint.conn().
    order_by : str or list[str], optional
        Attribute(s) to order results by. Defaults to 'id'.

    Examples
    --------
    >>> dj.kill('HOST LIKE "%compute%"')  # List connections from hosts containing "compute"
    >>> dj.kill('TIME > 600')  # List connections idle for more than 10 minutes
    """

    if connection is None:
        connection = conn()

    if order_by is not None and not isinstance(order_by, str):
        order_by = ",".join(order_by)

    query = (
        "SELECT * FROM information_schema.processlist WHERE id <> CONNECTION_ID()"
        + ("" if restriction is None else " AND (%s)" % restriction)
        + (" ORDER BY %s" % (order_by or "id"))
    )

    while True:
        print("  ID USER         HOST          STATE         TIME    INFO")
        print("+--+ +----------+ +-----------+ +-----------+ +-----+")
        cur = ({k.lower(): v for k, v in elem.items()} for elem in connection.query(query, as_dict=True))
        for process in cur:
            try:
                print("{id:>4d} {user:<12s} {host:<12s} {state:<12s} {time:>7d}  {info}".format(**process))
            except TypeError:
                print(process)
        response = input('process to kill or "q" to quit > ')
        if response == "q":
            break
        if response:
            try:
                pid = int(response)
            except ValueError:
                pass  # ignore non-numeric input
            else:
                try:
                    connection.query("kill %d" % pid)
                except pymysql.err.InternalError:
                    logger.warn("Process not found")


def kill_quick(restriction=None, connection=None):
    """
    Kill database connections without prompting.

    Parameters
    ----------
    restriction : str, optional
        SQL WHERE clause to filter connections. Can use any attribute from
        information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.
    connection : Connection, optional
        A datajoint.Connection object. Defaults to datajoint.conn().

    Returns
    -------
    int
        Number of terminated connections.

    Examples
    --------
    >>> dj.kill_quick('HOST LIKE "%compute%"')  # Kill connections from hosts with "compute"
    """
    if connection is None:
        connection = conn()

    query = "SELECT * FROM information_schema.processlist WHERE id <> CONNECTION_ID()" + (
        "" if restriction is None else " AND (%s)" % restriction
    )

    cur = ({k.lower(): v for k, v in elem.items()} for elem in connection.query(query, as_dict=True))
    nkill = 0
    for process in cur:
        connection.query("kill %d" % process["id"])
        nkill += 1
    return nkill
