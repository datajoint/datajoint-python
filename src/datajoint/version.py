# Version is derived from the git tag at build time by hatch-vcs (see
# pyproject.toml). The build writes the resolved version to the generated,
# git-ignored ``_version.py``; there is no version string to bump by hand.
# This module re-exports it, falling back to installed package metadata so
# ``datajoint.__version__`` also works when running from a source tree.
try:
    from ._version import __version__
except ImportError:  # not built yet (e.g. a bare git checkout on sys.path)
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("datajoint")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"

__all__ = ["__version__"]
