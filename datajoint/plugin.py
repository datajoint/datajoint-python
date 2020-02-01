import pkg_resources
from pathlib import Path
from cryptography.exceptions import InvalidSignature
from raphael_python_metadata import hash_pkg, verify

discovered_plugins = {
    entry_point.module_name: dict(plugon=entry_point.name, verified=False)
    for entry_point
    in pkg_resources.iter_entry_points('datajoint.plugins')
}


def _update_error_stack(plugin_name):
    try:
        base_name = 'datajoint'
        base_meta = pkg_resources.get_distribution(base_name)
        plugin_meta = pkg_resources.get_distribution(plugin_name)

        data = hash_pkg(str(Path(plugin_meta.module_path, plugin_name)))
        signature = plugin_meta.get_metadata('{}.sig'.format(plugin_name))
        pubkey_path = str(Path(base_meta.egg_info, '{}.pub'.format(base_name)))
        verify(pubkey_path, data, signature)
        discovered_plugins[plugin_name]['verified'] = True
        print('DataJoint verified plugin `{}` introduced.'.format(plugin_name))
    except (FileNotFoundError, InvalidSignature):
        print('Unverified plugin `{}` introduced.'.format(plugin_name))


def override(plugin_type, context, method_list=None):
    relevant_plugins = {
        k: v for k, v in discovered_plugins.items() if v['plugon'] == plugin_type}
    if relevant_plugins:
        for module_name in relevant_plugins:
            # import plugin
            module = __import__(module_name)
            module_dict = module.__dict__
            # update error stack (if applicable)
            _update_error_stack(module.__name__)
            # override based on plugon preference
            if method_list is not None:
                new_methods = []
                for v in method_list:
                    try:
                        new_methods.append(getattr(module, v))
                    except AttributeError:
                        pass
                context.update(dict(zip(method_list, new_methods)))
            else:
                try:
                    new_methods = module.__all__
                except AttributeError:
                    new_methods = [name for name in module_dict if not name.startswith('_')]
                context.update({name: module_dict[name] for name in new_methods})
