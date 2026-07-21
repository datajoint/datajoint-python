"""
Tests for datajoint.logging.

Regression coverage for #1516: importing datajoint must not install a
process-wide sys.excepthook, and LevelAwareFormatter must not discard
exc_info/stack_info.
"""

import logging
import sys


def test_import_does_not_replace_excepthook():
    """Importing datajoint must leave sys.excepthook untouched (#1516)."""
    original = sys.excepthook
    try:
        modules_to_remove = [key for key in sys.modules if key.startswith("datajoint")]
        for mod in modules_to_remove:
            del sys.modules[mod]

        import datajoint  # noqa: F401

        assert sys.excepthook is original, "importing datajoint replaced sys.excepthook"
    finally:
        sys.excepthook = original


def test_formatter_renders_exception_info():
    """LevelAwareFormatter must append the traceback when exc_info is set (#1516)."""
    from datajoint.logging import LevelAwareFormatter

    formatter = LevelAwareFormatter()
    try:
        raise ValueError("something specific and diagnosable went wrong")
    except ValueError:
        record = logging.LogRecord(
            name="datajoint",
            level=logging.ERROR,
            pathname=__file__,
            lineno=0,
            msg="Uncaught exception",
            args=(),
            exc_info=sys.exc_info(),
        )

    output = formatter.format(record)
    assert "Uncaught exception" in output
    assert "ValueError: something specific and diagnosable went wrong" in output
    assert "Traceback (most recent call last)" in output


def test_formatter_renders_stack_info():
    """LevelAwareFormatter must append stack_info when present (#1516)."""
    from datajoint.logging import LevelAwareFormatter

    formatter = LevelAwareFormatter()
    record = logging.LogRecord(
        name="datajoint",
        level=logging.WARNING,
        pathname=__file__,
        lineno=0,
        msg="with stack",
        args=(),
        exc_info=None,
        sinfo="Stack (most recent call last):\n  fake stack frame",
    )

    output = formatter.format(record)
    assert "with stack" in output
    assert "fake stack frame" in output
