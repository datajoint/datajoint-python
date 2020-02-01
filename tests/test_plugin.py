import datajoint.errors as djerr
import datajoint.plugin as p
import importlib


def test_normal_djerror():
    try:
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        assert(e.__cause__ is None)


def test_unverified_djerror():
    try:
        curr_plugins = p.discovered_plugins
        p.discovered_plugins = dict(test_plugin_module=dict(verified=False, plugon='example'))
        importlib.reload(djerr)
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        p.discovered_plugins = curr_plugins
        plugwarn = djerr.PluginWarning
        importlib.reload(djerr)
        assert(isinstance(e.__cause__, plugwarn))


def test_verified_djerror():
    try:
        curr_plugins = p.discovered_plugins
        p.discovered_plugins = dict(test_plugin_module=dict(verified=True, plugon='example'))
        importlib.reload(djerr)
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        p.discovered_plugins = curr_plugins
        importlib.reload(djerr)
        assert(e.__cause__ is None)
