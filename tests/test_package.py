from __future__ import annotations

import importlib.metadata

import datajoint as m


def test_version():
    assert importlib.metadata.version("datajoint") == m.__version__
