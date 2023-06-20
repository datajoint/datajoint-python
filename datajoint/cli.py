import argparse
import sys
from code import interact
from collections import ChainMap
from datajoint import __version__ as version, config, create_virtual_module


def dj_cli(args: list = None):
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
        "-V", "--version", action="version", version=f"datajoint {version}"
    )
    parser.add_argument(
        "-u",
        "--user",
        type=str,
        default=config["database.user"],
        required=False,
        help="Datajoint username",
    )
    parser.add_argument(
        "-p",
        "--password",
        type=str,
        default=config["database.password"],
        required=False,
        help="Datajoint password",
    )
    parser.add_argument(
        "-h",
        "--host",
        type=str,
        default=config["database.host"],
        required=False,
        help="Datajoint host",
    )
    parser.add_argument(
        "-s",
        "--schemas",
        nargs="+",
        type=[str],
        default=[],
        required=False,
        help="A list of virtual module mappings in `db:schema ...` format",
    )
    kwargs = vars(parser.parse_args(args if sys.argv[1:] else ["--help"]))
    mods = {}
    if kwargs["user"]:
        config["database.user"] = kwargs["user"]
    if kwargs["password"]:
        config["database.password"] = kwargs["password"]
    if kwargs["host"]:
        config["database.host"] = kwargs["host"]
    if kwargs["schemas"]:
        d, m = kwargs["schemas"].split(":")
        mods[m] = create_virtual_module(m, d)

    banner = "dj repl\n"
    if mods:
        modstr = "\n".join("  - {}".format(m) for m in mods)
        banner += "\nschema modules:\n\n" + modstr + "\n"
    interact(banner, local=dict(ChainMap(mods, locals(), globals())))

    raise SystemExit


if __name__ == "__main__":
    dj_cli()
