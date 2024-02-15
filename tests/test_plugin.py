import pytest
import datajoint.errors as djerr
import datajoint.plugin as p
import pkg_resources
from os import path


def test_check_pubkey():
    base_name = "datajoint"
    base_meta = pkg_resources.get_distribution(base_name)
    pubkey_meta = base_meta.get_metadata("{}.pub".format(base_name))

    with open(
        path.join(path.abspath(path.dirname(__file__)), "..", "datajoint.pub"), "r"
    ) as f:
        assert f.read() == pubkey_meta


def test_normal_djerror():
    try:
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        assert e.__cause__ is None


def test_verified_djerror(category="connection"):
    try:
        curr_plugins = getattr(p, "{}_plugins".format(category))
        setattr(
            p,
            "{}_plugins".format(category),
            dict(test_plugin_id=dict(verified=True, object="example")),
        )
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        setattr(p, "{}_plugins".format(category), curr_plugins)
        assert e.__cause__ is None


def test_verified_djerror_type():
    test_verified_djerror(category="type")


def test_unverified_djerror(category="connection"):
    try:
        curr_plugins = getattr(p, "{}_plugins".format(category))
        setattr(
            p,
            "{}_plugins".format(category),
            dict(test_plugin_id=dict(verified=False, object="example")),
        )
        raise djerr.DataJointError("hello")
    except djerr.DataJointError as e:
        setattr(p, "{}_plugins".format(category), curr_plugins)
        assert isinstance(e.__cause__, djerr.PluginWarning)


def test_unverified_djerror_type():
    test_unverified_djerror(category="type")
