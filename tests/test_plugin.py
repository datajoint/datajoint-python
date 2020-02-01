import datajoint.errors as djerr
import datajoint.plugin as p
import pkg_resources


def test_check_pubkey():
    base_name = 'datajoint'
    base_meta = pkg_resources.get_distribution(base_name)
    pubkey_meta = base_meta.get_metadata('{}.pub'.format(base_name))

    with open('./datajoint.pub', "r") as f:
        assert(f.read() == pubkey_meta)


def test_normal_djerror():
    try:
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        assert(e.__cause__ is None)


def test_verified_djerror():
    try:
        curr_plugins = p.discovered_plugins
        p.discovered_plugins = dict(test_plugin_module=dict(verified=True, plugon='example'))
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        p.discovered_plugins = curr_plugins
        assert(e.__cause__ is None)


def test_unverified_djerror():
    try:
        curr_plugins = p.discovered_plugins
        p.discovered_plugins = dict(test_plugin_module=dict(verified=False, plugon='example'))
        raise djerr.DataJointError("hello")
    except djerr.DataJointError as e:
        p.discovered_plugins = curr_plugins
        assert(isinstance(e.__cause__, djerr.PluginWarning))
