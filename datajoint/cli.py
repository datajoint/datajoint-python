import argparse
from code import interact
from collections import ChainMap

import datajoint as dj


def cli(args: list = None):
    """
    Console interface for DataJoint Python

    :param args: List of arguments to be passed in, defaults to reading stdin
    :type args: list, optional
    """
    parser = argparse.ArgumentParser(
        prog="datajoint",
        description="DataJoint console interface.",
        conflict_handler="resolve",
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"{dj.__name__} {dj.__version__}"
    )
    parser.add_argument(
        "-u",
        "--user",
        type=str,
        default=dj.config["database.user"],
        required=False,
        help="Datajoint username",
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        default=dj.config["database.password"],
        required=False,
        help="Datajoint password",
    )
    parser.add_argument(
        "-h",
        "--host",
        type=str,
        default=dj.config["database.host"],
        required=False,
        help="Datajoint host",
    )
    parser.add_argument(
        "-s",
        "--schemas",
        nargs="+",
        type=str,
        required=False,
        help="A list of virtual module mappings in `db:schema ...` format",
    )
    kwargs = vars(parser.parse_args(args))
    mods = {}
    if kwargs["user"]:
        dj.config["database.user"] = kwargs["user"]
    if kwargs["password"]:
        dj.config["database.password"] = kwargs["password"]
    if kwargs["host"]:
        dj.config["database.host"] = kwargs["host"]
    if kwargs["schemas"]:
        for vm in kwargs["schemas"]:
            d, m = vm.split(":")
            mods[m] = dj.create_virtual_module(m, d)

    banner = "dj repl\n"
    if mods:
        modstr = "\n".join("  - {}".format(m) for m in mods)
        banner += "\nschema modules:\n\n" + modstr + "\n"
    interact(banner, local=dict(ChainMap(mods, locals(), globals())))

    raise SystemExit


if __name__ == "__main__":
    cli()
