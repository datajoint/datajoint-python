"""Fixtures for unit tests."""

import logging
import sys

import pytest


def _datajoint_modules():
    return [name for name in sys.modules if name == "datajoint" or name.startswith("datajoint.")]


@pytest.fixture
def fresh_datajoint_import():
    """
    Remove ``datajoint`` from ``sys.modules`` so the test can import it afresh,
    and put the original modules back afterwards.

    Without the restore, later tests in the same session would mix two copies
    of the package: the copy their module imported at collection time, and the
    fresh copy that function-level imports inside the package then resolve to.
    Settings changed through one copy's ``dj.config`` would not reach code that
    reads the other copy's config.
    """
    saved = {name: sys.modules.pop(name) for name in _datajoint_modules()}
    logger = logging.getLogger("datajoint")
    handlers, level, propagate = list(logger.handlers), logger.level, logger.propagate
    try:
        yield
    finally:
        for name in _datajoint_modules():
            del sys.modules[name]
        sys.modules.update(saved)
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate
