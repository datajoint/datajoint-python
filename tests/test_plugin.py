import base64
import os
import subprocess
import sys
from importlib.metadata import distribution
from os import path

import pytest
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

import datajoint.errors as djerr
import datajoint.plugin as p


@pytest.mark.skip(reason="marked for deprecation")
def test_check_pubkey():
    base_name = "datajoint"
    base_meta = distribution(base_name)
    pubkey_meta = base_meta.read_text("{}.pub".format(base_name))

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


def _fake_plugin(root, signature):
    (root / "djfake").mkdir()
    (root / "djfake" / "__init__.py").write_text("x = 1\n")
    (root / "djfake-1.0.dist-info").mkdir()
    (root / "djfake-1.0.dist-info" / "METADATA").write_text(
        "Name: djfake\nVersion: 1.0\n"
    )
    (root / "djfake-1.0.dist-info" / "entry_points.txt").write_text(
        "[datajoint_plugins.connection]\nfake = djfake\n"
    )
    if signature is not None:
        (root / "djfake-1.0.dist-info" / "djfake.sig").write_text(signature)


@pytest.mark.parametrize("signature, verified", [("SIG", True), (None, False)])
def test_import_plugins(tmp_path, monkeypatch, signature, verified):
    _fake_plugin(tmp_path, signature)
    monkeypatch.syspath_prepend(str(tmp_path))
    calls = {}
    monkeypatch.setattr(p, "hash_pkg", lambda pkgpath: pkgpath)
    monkeypatch.setattr(p, "verify", lambda **kwargs: calls.update(kwargs))
    plugins = p._import_plugins("connection")
    assert plugins["fake"]["verified"] is verified
    assert plugins["fake"]["object"].load().x == 1
    assert p._import_plugins("datatype") == {}
    if verified:
        assert calls["data"] == str(tmp_path / "djfake")
        assert calls["signature"] == "SIG"
        assert calls["pubkey_path"].endswith("datajoint.pub")


def test_import_without_pkg_resources_or_otumat(tmp_path):
    _fake_plugin(tmp_path, "SIG")
    code = (
        "import sys\n"
        "class Block:\n"
        "    def find_spec(self, name, *args):\n"
        "        if name.split('.')[0] in ('pkg_resources', 'otumat', 'distutils'):\n"
        "            raise ImportError(name)\n"
        "sys.meta_path.insert(0, Block())\n"
        "import datajoint.plugin as p\n"
        "print(p.connection_plugins['fake']['verified'])\n"
    )
    env = dict(
        os.environ,
        PYTHONPATH=os.pathsep.join(
            [str(tmp_path), path.dirname(path.dirname(__file__))]
        ),
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.stdout.splitlines() == ["False"], result.stderr


def test_hash_pkg(tmp_path):
    for name, text in {
        "__init__.py": "",
        "sub/data.txt": "café\n",
        "__pycache__/a.pyc": "x",
    }.items():
        (tmp_path / "pkg" / name).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "pkg" / name).write_text(text)
    assert (
        p.hash_pkg(pkgpath=str(tmp_path / "pkg"))
        == "51ad1acd96ead2654ef409bb708c9c3b4b8c6333"
    )
    assert (
        p.hash_pkg(pkgpath=str(tmp_path / "pkg" / "sub" / "missing"))
        == "e69de29bb2d1d6434b8b29ae775ad8c2e48c5391"
    )


def test_verify(tmp_path):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    (tmp_path / "datajoint.pub").write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    pss = padding.PSS(
        mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH
    )
    signature = (
        base64.b64encode(key.sign(b"digest", pss, hashes.SHA256())).decode() + "\n"
    )
    p.verify(
        pubkey_path=str(tmp_path / "datajoint.pub"), data="digest", signature=signature
    )
    with pytest.raises(InvalidSignature):
        p.verify(
            pubkey_path=str(tmp_path / "datajoint.pub"), data="", signature=signature
        )
