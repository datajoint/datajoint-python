import pprint
import random
import string
import pytest
from datajoint import DataJointError, settings
import datajoint as dj
import os

__author__ = "Fabian Sinz"


def test_load_save():
    """Testing load and save"""
    dj.config.save("tmp.json")
    conf = settings.Config()
    conf.load("tmp.json")
    assert conf == dj.config
    os.remove("tmp.json")


def test_singleton():
    """Testing singleton property"""
    dj.config.save("tmp.json")
    conf = settings.Config()
    conf.load("tmp.json")
    conf["dummy.val"] = 2

    assert conf == dj.config
    os.remove("tmp.json")


def test_singleton2():
    """Testing singleton property"""
    conf = settings.Config()
    conf["dummy.val"] = 2
    _ = settings.Config()  # a new instance should not delete dummy.val
    assert conf["dummy.val"] == 2


def test_validator():
    """Testing validator"""
    with pytest.raises(DataJointError):
        dj.config["database.port"] = "harbor"


def test_del():
    """Testing del"""
    dj.config["peter"] = 2
    assert "peter" in dj.config
    del dj.config["peter"]
    assert "peter" not in dj.config


def test_len():
    """Testing len"""
    len(dj.config) == len(dj.config._conf)


def test_str():
    """Testing str"""
    str(dj.config) == pprint.pformat(dj.config._conf, indent=4)


def test_repr():
    """Testing repr"""
    repr(dj.config) == pprint.pformat(dj.config._conf, indent=4)


def test_save():
    """Testing save of config"""
    tmpfile = "".join(
        random.choice(string.ascii_uppercase + string.digits) for _ in range(20)
    )
    moved = False
    if os.path.isfile(settings.LOCALCONFIG):
        os.rename(settings.LOCALCONFIG, tmpfile)
        moved = True
    dj.config.save_local()
    assert os.path.isfile(settings.LOCALCONFIG)
    if moved:
        os.rename(tmpfile, settings.LOCALCONFIG)


def test_load_save():
    """Testing load and save of config"""
    filename_old = dj.settings.LOCALCONFIG
    filename = (
        "".join(
            random.choice(string.ascii_uppercase + string.digits) for _ in range(50)
        )
        + ".json"
    )
    dj.settings.LOCALCONFIG = filename
    dj.config.save_local()
    dj.config.load(filename=filename)
    dj.settings.LOCALCONFIG = filename_old
    os.remove(filename)


def test_contextmanager():
    """Testing context manager"""
    dj.config["arbitrary.stuff"] = 7
    with dj.config(arbitrary__stuff=10):
        assert dj.config["arbitrary.stuff"] == 10
    assert dj.config["arbitrary.stuff"] == 7
