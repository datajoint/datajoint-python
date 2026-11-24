"""
The Docker image build sets the package version (#1723).

The version comes from the git tag through hatch-vcs, and the Docker build
context has no ``.git`` directory, so the Dockerfile must give it explicitly.
``docker compose build app`` failed with a LookupError without it. Building the
image is too slow for the unit tests; these checks keep the build files in step.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]

pytestmark = pytest.mark.skipif(not (ROOT / "Dockerfile").exists(), reason="needs the source tree")


def test_dockerfile_builds_wheel_with_pretend_version():
    text = (ROOT / "Dockerfile").read_text()
    assert re.search(r"^ARG DJ_BUILD_VERSION=\S+", text, re.MULTILINE)
    build = re.search(r"SETUPTOOLS_SCM_PRETEND_VERSION=\$\{DJ_BUILD_VERSION\}\s*\\\s*\n\s*(.+)", text)
    assert build, "the datajoint wheel must be built with SETUPTOOLS_SCM_PRETEND_VERSION set"
    # Only the datajoint wheel gets the pretend version, not dependencies built from source
    assert "pip wheel" in build.group(1) and "--no-deps" in build.group(1)
    assert "pip install -q --no-cache-dir /main" not in text


def test_compose_passes_build_version():
    text = (ROOT / "docker-compose.yaml").read_text()
    assert re.search(r"^\s+DJ_BUILD_VERSION: \$\{DJ_BUILD_VERSION:-[^}]+\}$", text, re.MULTILINE)
