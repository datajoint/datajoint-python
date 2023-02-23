from .settings import config
import pkg_resources
from pathlib import Path
from cryptography.exceptions import InvalidSignature
from otumat import hash_pkg, verify
import logging

logger = logging.getLogger(__name__.split(".")[0])


def _update_error_stack(plugin_name):
    try:
        base_name = "datajoint"
        base_meta = pkg_resources.get_distribution(base_name)
        plugin_meta = pkg_resources.get_distribution(plugin_name)

        data = hash_pkg(pkgpath=str(Path(plugin_meta.module_path, plugin_name)))
        signature = plugin_meta.get_metadata(f"{plugin_name}.sig")
        pubkey_path = str(Path(base_meta.egg_info, f"{base_name}.pub"))
        verify(pubkey_path=pubkey_path, data=data, signature=signature)
        logger.info(f"DataJoint verified plugin `{plugin_name}` detected.")
        return True
    except (FileNotFoundError, InvalidSignature):
        logger.warning(f"Unverified plugin `{plugin_name}` detected.")
        return False


def _import_plugins(category):
    return {
        entry_point.name: dict(
            object=entry_point,
            verified=_update_error_stack(entry_point.module_name.split(".")[0]),
        )
        for entry_point in pkg_resources.iter_entry_points(
            "datajoint_plugins.{}".format(category)
        )
        if "plugin" not in config
        or category not in config["plugin"]
        or entry_point.module_name.split(".")[0] in config["plugin"][category]
    }


connection_plugins = _import_plugins("connection")
type_plugins = _import_plugins("datatype")
