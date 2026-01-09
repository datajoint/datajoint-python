"""
Collection of test cases to test the dj cli
"""

import subprocess

import pytest

import datajoint as dj


def test_cli_version(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        dj.cli(args=["-V"])
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    captured_output = capsys.readouterr().out
    assert captured_output == f"{dj.__name__} {dj.__version__}\n"


def test_cli_help(capsys):
    with pytest.raises(SystemExit) as pytest_wrapped_e:
        dj.cli(args=["--help"])
    assert pytest_wrapped_e.type is SystemExit
    assert pytest_wrapped_e.value.code == 0

    captured_output = capsys.readouterr().out
    assert captured_output.strip()


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
    # Config now uses pydantic format: Config(database=DatabaseSettings(host=..., user=..., ...))
    for key in ("host=", "user=", "password="):
        assert key in cleaned, f"Key {key} not found in config from stdout: {cleaned}"


def test_cli_args():
    process = subprocess.Popen(
        ["dj", "-u", "test_user", "-p", "test_pass", "--host", "test_host"],
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
    assert "test_user" in stdout
    assert "test_pass" in stdout
    assert "test_host" in stdout


def test_cli_schemas(prefix, connection_root, db_creds_root):
    schema = dj.Schema(prefix + "_cli", locals(), connection=connection_root)

    @schema
    class IJ(dj.Lookup):
        definition = """  # tests restrictions
        i  : int
        j  : int
        """
        contents = list(dict(i=i, j=j + 2) for i in range(3) for j in range(3))

    # Pass credentials via CLI args to avoid prompting for username
    process = subprocess.Popen(
        [
            "dj",
            "-u", db_creds_root['user'],
            "-p", db_creds_root['password'],
            "--host", db_creds_root['host'],
            "-s",
            f"{prefix}_cli:test_schema",
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    process.stdin.write("test_schema.__dict__['__name__']\n")
    process.stdin.write("test_schema.__dict__['schema']\n")
    process.stdin.write("test_schema.IJ.to_dicts()\n")
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

    cleaned = stdout.strip(" >\t\n\r")
    for key in (
        "test_schema",
        f"Schema `{prefix}_cli`",
    ):
        assert key in cleaned, f"Key {key} not found in stdout: {cleaned}"
