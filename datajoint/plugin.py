import logging
from importlib.metadata import distribution, entry_points
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from otumat import hash_pkg, verify

from .settings import config

logger = logging.getLogger(__name__.split(".")[0])


def _update_error_stack(plugin_name):
    try:
        base_name = "datajoint"
        base_meta = distribution(base_name)
        plugin_meta = distribution(plugin_name)

        # Get the package location - equivalent to module_path
        plugin_location = plugin_meta.locate_file("")
        data = hash_pkg(pkgpath=str(Path(plugin_location.parent, plugin_name)))

        # Get signature metadata - equivalent to get_metadata()
        signature = plugin_meta.read_text(f"{plugin_name}.sig")

        # Get public key path - equivalent to egg_info
        pubkey_path = str(Path(base_meta.locate_file("").parent, f"{base_name}.pub"))
        verify(pubkey_path=pubkey_path, data=data, signature=signature)
        logger.info(f"DataJoint verified plugin `{plugin_name}` detected.")
        return True
    except (FileNotFoundError, InvalidSignature):
        logger.warning(f"Unverified plugin `{plugin_name}` detected.")
        return False


def _import_plugins(category):
    # Get entry points for the group - equivalent to iter_entry_points()
    group_name = f"datajoint_plugins.{category}"
    eps = entry_points()

    # Handle both Python 3.9 and 3.10+ entry points API
    if hasattr(eps, "select"):
        group_eps = eps.select(group=group_name)
    else:
        group_eps = eps.get(group_name, [])

    return {
        entry_point.name: dict(
            object=entry_point,
            verified=_update_error_stack(entry_point.module.split(".")[0]),
        )
        for entry_point in group_eps
        if "plugin" not in config
        or category not in config["plugin"]
        or entry_point.module.split(".")[0] in config["plugin"][category]
    }


connection_plugins = _import_plugins("connection")
type_plugins = _import_plugins("datatype")
