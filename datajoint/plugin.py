import base64
import hashlib
import logging
from importlib.metadata import distribution, entry_points
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

from .settings import config

logger = logging.getLogger(__name__.split(".")[0])


def _git_hash(text):
    return hashlib.sha1(f"blob {len(text)}\0{text}".encode()).hexdigest()


def hash_pkg(*, pkgpath):
    root = Path(pkgpath).absolute()
    return _git_hash(
        "".join(
            f"100644 {_git_hash(path.read_text())} 0\t{path.relative_to(root.parent)}\n"
            for path in sorted(root.rglob("*"))
            if path.is_file() and "pycache" not in str(path)
        )
    )


def verify(*, pubkey_path, data, signature):
    serialization.load_pem_public_key(Path(pubkey_path).read_bytes()).verify(
        base64.b64decode(signature.encode()),
        data.encode(),
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH
        ),
        hashes.SHA256(),
    )


def _update_error_stack(plugin_name):
    try:
        base_name = "datajoint"
        base_meta = distribution(base_name)
        plugin_meta = distribution(plugin_name)

        data = hash_pkg(pkgpath=str(Path(plugin_meta.locate_file(""), plugin_name)))
        signature = plugin_meta.read_text(f"{plugin_name}.sig")
        if signature is None:
            raise FileNotFoundError(f"{plugin_name}.sig")
        pubkey_path = str(Path(base_meta._path).resolve() / f"{base_name}.pub")
        verify(pubkey_path=pubkey_path, data=data, signature=signature)
        logger.info(f"DataJoint verified plugin `{plugin_name}` detected.")
        return True
    except (FileNotFoundError, InvalidSignature):
        logger.warning(f"Unverified plugin `{plugin_name}` detected.")
        return False


def _import_plugins(category):
    eps = entry_points()
    group = f"datajoint_plugins.{category}"
    eps = eps.select(group=group) if hasattr(eps, "select") else eps.get(group, [])
    return {
        entry_point.name: dict(
            object=entry_point,
            verified=_update_error_stack(entry_point.module.split(".")[0]),
        )
        for entry_point in eps
        if "plugin" not in config
        or category not in config["plugin"]
        or entry_point.module.split(".")[0] in config["plugin"][category]
    }


connection_plugins = _import_plugins("connection")
type_plugins = _import_plugins("datatype")
