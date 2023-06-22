"""
Collection of test cases to test the dj cli
"""

import json
import subprocess
import pytest
import datajoint as dj
from .schema_simple import *


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        dj.cli(args=["-V"])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0

    captured_output = capsys.readouterr().out
    assert captured_output == f"{dj.__name__} {dj.__version__}\n"


def test_cli_help(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        dj.cli(args=["--help"])
    assert pytest_wrapped_e.type == SystemExit
    assert pytest_wrapped_e.value.code == 0

    captured_output = capsys.readouterr().out

    assert (
        "\
usage: datajoint [--help] [-V] [-u USER] [-p PASSWORD] [-h HOST]\n\
                 [-s SCHEMAS [SCHEMAS ...]]\n\n\
\
DataJoint console interface.\n\n\
\
optional arguments:\n\
  --help                show this help message and exit\n\
  -V, --version         show program's version number and exit\n\
  -u USER, --user USER  Datajoint username\n\
  -p PASSWORD, --password PASSWORD\n\
                        Datajoint password\n\
  -h HOST, --host HOST  Datajoint host\n\
  -s SCHEMAS [SCHEMAS ...], --schemas SCHEMAS [SCHEMAS ...]\n\
                        A list of virtual module mappings in `db:schema ...`\n\
                        format\n"
        == captured_output
    )


def test_cli_config():
    process = subprocess.Popen(
        ["dj"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    process.stdin.write("dj.config\n")
    process.stdin.flush()

    stdout, stderr = process.communicate()

    assert dj.config == json.loads(
        stdout[4:519]
        .replace("'", '"')
        .replace("None", "null")
        .replace("True", "true")
        .replace("False", "false")
    )


def test_cli_args():
    process = subprocess.Popen(
        ["dj", "-utest_user", "-ptest_pass", "-htest_host"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    process.stdin.write("dj.config['database.user']\n")
    process.stdin.write("dj.config['database.password']\n")
    process.stdin.write("dj.config['database.host']\n")
    process.stdin.flush()

    stdout, stderr = process.communicate()
    assert "test_user" == stdout[5:14]
    assert "test_pass" == stdout[21:30]
    assert "test_host" == stdout[37:46]


def test_cli_schemas():
    process = subprocess.Popen(
        ["dj", "-s", "djtest_test1:test_schema1", "djtest_relational:test_schema2"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    process.stdin.write("test_schema1.__dict__['__name__']\n")
    process.stdin.write("test_schema1.__dict__['schema']\n")
    process.stdin.write("test_schema1.TTest.fetch(as_dict=True)\n")
    process.stdin.flush()

    stdout, stderr = process.communicate()
    fetch_res = [
        {"key": 0, "value": 0},
        {"key": 1, "value": 2},
        {"key": 2, "value": 4},
        {"key": 3, "value": 6},
        {"key": 4, "value": 8},
        {"key": 5, "value": 10},
        {"key": 6, "value": 12},
        {"key": 7, "value": 14},
        {"key": 8, "value": 16},
        {"key": 9, "value": 18},
    ]
    assert (
        "dj repl\n\nschema modules:\n\n  - test_schema1\n  - test_schema2"
        == stderr[159:218]
    )
    assert "'test_schema1'" == stdout[4:18]
    assert "Schema `djtest_test1`" == stdout[23:44]
    assert fetch_res == json.loads(stdout[50:295].replace("'", '"'))
