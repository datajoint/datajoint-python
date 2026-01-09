"""
DataJoint command-line interface.

Provides a Python REPL with DataJoint pre-loaded and optional schema access.

Usage::

    # Start REPL with database credentials
    dj --user root --password secret --host localhost:3306

    # Load schemas as virtual modules
    dj -s my_lab:lab -s my_analysis:analysis

    # In the REPL
    >>> lab.Subject.to_dicts()
    >>> dj.Diagram(lab.schema)
"""

from __future__ import annotations

import argparse
from code import interact
from collections import ChainMap

import datajoint as dj


def cli(args: list[str] | None = None) -> None:
    """
    DataJoint command-line interface.

    Starts an interactive Python REPL with DataJoint imported and configured.
    Optionally loads database schemas as virtual modules for quick exploration.

    Parameters
    ----------
    args : list[str], optional
        Command-line arguments. If None, reads from sys.argv.

    Examples
    --------
    From the command line::

        $ dj --host localhost:3306 --user root --password secret
        $ dj -s my_lab:lab -s my_analysis:analysis

    Programmatically::

        >>> from datajoint.cli import cli
        >>> cli(["--version"])
    """
    parser = argparse.ArgumentParser(
        prog="dj",
        description="DataJoint interactive console. Start a Python REPL with DataJoint pre-loaded.",
        epilog="Example: dj -s my_lab:lab --host localhost:3306",
    )
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version=f"{dj.__name__} {dj.__version__}",
    )
    parser.add_argument(
        "-u",
        "--user",
        type=str,
        default=None,
        help="Database username (default: from config)",
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        default=None,
        help="Database password (default: from config)",
    )
    parser.add_argument(
        "--host",
        type=str,
        default=None,
        help="Database host as host:port (default: from config)",
    )
    parser.add_argument(
        "-s",
        "--schemas",
        nargs="+",
        type=str,
        metavar="DB:ALIAS",
        help="Load schemas as virtual modules. Format: schema_name:alias",
    )

    kwargs = vars(parser.parse_args(args))

    # Apply credentials to config
    if kwargs["user"]:
        dj.config["database.user"] = kwargs["user"]
    if kwargs["password"]:
        dj.config["database.password"] = kwargs["password"]
    if kwargs["host"]:
        dj.config["database.host"] = kwargs["host"]

    # Load requested schemas
    mods: dict[str, dj.VirtualModule] = {}
    if kwargs["schemas"]:
        for vm in kwargs["schemas"]:
            if ":" not in vm:
                parser.error(f"Invalid schema format '{vm}'. Use schema_name:alias")
            schema_name, alias = vm.split(":", 1)
            mods[alias] = dj.VirtualModule(alias, schema_name)

    # Build banner
    banner = f"DataJoint {dj.__version__} REPL\n"
    banner += "Type 'dj.' and press Tab for available functions.\n"
    if mods:
        banner += "\nLoaded schemas:\n"
        for alias in mods:
            banner += f"  {alias} -> {mods[alias].schema.database}\n"

    # Start interactive session
    interact(banner, local=dict(ChainMap(mods, {"dj": dj}, globals())))
    raise SystemExit


if __name__ == "__main__":
    cli()
