"""
Administrative utilities for managing database connections and passwords.

This module provides functions for viewing and terminating database connections
through the MySQL processlist interface, as well as password management.
"""

from __future__ import annotations

import logging
from getpass import getpass

import pymysql
from packaging import version

from .connection import Connection, conn
from .settings import config
from .utils import user_choice

logger = logging.getLogger(__name__.split(".")[0])


def set_password(
    new_password: str | None = None,
    connection: Connection | None = None,
    update_config: bool | None = None,
) -> None:
    """
    Change the database password for the current user.

    Args:
        new_password: The new password. If None, prompts for input.
        connection: A datajoint.Connection object. If None, uses datajoint.conn().
        update_config: If True, save the new password to local config.
            If None, prompts the user.
    """
    connection = conn() if connection is None else connection
    if new_password is None:
        new_password = getpass("New password: ")
        confirm_password = getpass("Confirm password: ")
        if new_password != confirm_password:
            logger.warning("Failed to confirm the password! Aborting password change.")
            return

    if version.parse(
        connection.query("select @@version;").fetchone()[0]
    ) >= version.parse("5.7"):
        # SET PASSWORD is deprecated as of MySQL 5.7 and removed in 8+
        connection.query("ALTER USER user() IDENTIFIED BY '%s';" % new_password)
    else:
        connection.query("SET PASSWORD = PASSWORD('%s')" % new_password)
    logger.info("Password updated.")

    if update_config or (
        update_config is None and user_choice("Update local setting?") == "yes"
    ):
        config["database.password"] = new_password
        config.save_local(verbose=True)


def kill(
    restriction: str | None = None,
    connection: Connection | None = None,
    order_by: str | list[str] | None = None,
) -> None:
    """
    View and interactively kill database connections.

    Displays active database connections matching the optional restriction and
    prompts the user to select connections to terminate.

    Args:
        restriction: SQL WHERE clause condition to filter the processlist.
            Can reference any column from information_schema.processlist:
            ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.
        connection: A datajoint.Connection object. If None, uses datajoint.conn().
        order_by: Column name(s) to sort results by. Defaults to 'id'.

    Examples:
        >>> dj.kill('HOST LIKE "%compute%"')  # connections from hosts containing "compute"
        >>> dj.kill('TIME > 600')  # connections idle for more than 10 minutes
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
        cur = (
            {k.lower(): v for k, v in elem.items()}
            for elem in connection.query(query, as_dict=True)
        )
        for process in cur:
            try:
                print(
                    "{id:>4d} {user:<12s} {host:<12s} {state:<12s} {time:>7d}  {info}".format(
                        **process
                    )
                )
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


def kill_quick(
    restriction: str | None = None,
    connection: Connection | None = None,
) -> int:
    """
    Kill database connections without prompting.

    Terminates all database connections matching the optional restriction
    without user confirmation.

    Args:
        restriction: SQL WHERE clause condition to filter the processlist.
            Can reference any column from information_schema.processlist:
            ID, USER, HOST, DB, COMMAND, TIME, STATE, INFO.
        connection: A datajoint.Connection object. If None, uses datajoint.conn().

    Returns:
        Number of connections terminated.

    Examples:
        >>> dj.kill_quick('HOST LIKE "%compute%"')  # kill connections from "compute" hosts
        >>> dj.kill_quick('TIME > 600')  # kill connections idle for more than 10 minutes
    """
    if connection is None:
        connection = conn()

    query = (
        "SELECT * FROM information_schema.processlist WHERE id <> CONNECTION_ID()"
        + ("" if restriction is None else " AND (%s)" % restriction)
    )

    cur = (
        {k.lower(): v for k, v in elem.items()}
        for elem in connection.query(query, as_dict=True)
    )
    nkill = 0
    for process in cur:
        connection.query("kill %d" % process["id"])
        nkill += 1
    return nkill
