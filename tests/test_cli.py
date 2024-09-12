"""
Collection of test cases to test the dj cli
"""

import json
import ast
import subprocess
import pytest
import datajoint as dj


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
    cleaned = stdout.strip(" >\t\n\r")
    assert dj.config.keys() == ast.literal_eval(cleaned).keys()


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


def test_cli_schemas(prefix, connection_root):
    schema = dj.Schema(prefix + "_cli", locals(), connection=connection_root)

    @schema
    class IJ(dj.Lookup):
        definition = """  # tests restrictions
        i  : int
        j  : int
        """
        contents = list(dict(i=i, j=j + 2) for i in range(3) for j in range(3))

    process = subprocess.Popen(
        ["dj", "-s", "djtest_cli:test_schema"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    process.stdin.write("test_schema.__dict__['__name__']\n")
    process.stdin.write("test_schema.__dict__['schema']\n")
    process.stdin.write("test_schema.IJ.fetch(as_dict=True)\n")
    process.stdin.flush()

    stdout, stderr = process.communicate()
    fetch_res = [
        {"i": 0, "j": 2},
        {"i": 0, "j": 3},
        {"i": 0, "j": 4},
        {"i": 1, "j": 2},
        {"i": 1, "j": 3},
        {"i": 1, "j": 4},
        {"i": 2, "j": 2},
        {"i": 2, "j": 3},
        {"i": 2, "j": 4},
    ]
    assert (
        "\
dj repl\n\n\
\
schema modules:\n\n\
  - test_schema"
        == stderr[159:200]
    )
    assert "'test_schema'" == stdout[4:17]
    assert "Schema `djtest_cli`" == stdout[22:41]
    assert fetch_res == json.loads(stdout[47:209].replace("'", '"'))
