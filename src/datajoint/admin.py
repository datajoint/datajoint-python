"""
Administrative utilities for DataJoint database management.

This module provides functions for administrative tasks such as setting passwords
and managing database connections. These operations typically require appropriate
database privileges.
"""

from __future__ import annotations

import logging

import pymysql

from .connection import conn

logger = logging.getLogger(__name__.split(".")[0])


def kill(restriction=None, connection=None, order_by=None):
    """
    view and kill database connections.

    :param restriction: restriction to be applied to processlist
    :param connection: a datajoint.Connection object. Default calls datajoint.conn()
    :param order_by: order by a single attribute or the list of attributes. defaults to 'id'.

    Restrictions are specified as strings and can involve any of the attributes of
    information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.

    Examples:
        dj.kill('HOST LIKE "%compute%"') lists only connections from hosts containing "compute".
        dj.kill('TIME > 600') lists only connections in their current state for more than 10 minutes
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
    Kill database connections without prompting. Returns number of terminated connections.

    :param restriction: restriction to be applied to processlist
    :param connection: a datajoint.Connection object. Default calls datajoint.conn()

    Restrictions are specified as strings and can involve any of the attributes of
    information_schema.processlist: ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.

    Examples:
        dj.kill('HOST LIKE "%compute%"') terminates connections from hosts containing "compute".
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
