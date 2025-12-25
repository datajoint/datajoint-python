"""
Logging configuration for the DataJoint package.

This module sets up the default logging handler and format for DataJoint,
and provides a custom exception hook to log uncaught exceptions.

The log level can be configured via the DJ_LOG_LEVEL environment variable.
"""

from __future__ import annotations

import logging
import os
import sys
from types import TracebackType

logger = logging.getLogger(__name__.split(".")[0])

log_level = os.getenv("DJ_LOG_LEVEL", "info").upper()

log_format = logging.Formatter("[%(asctime)s][%(levelname)s]: %(message)s")

stream_handler = logging.StreamHandler()  # default handler
stream_handler.setFormatter(log_format)

logger.setLevel(level=log_level)
logger.handlers = [stream_handler]


def excepthook(
    exc_type: type[BaseException],
    exc_value: BaseException,
    exc_traceback: TracebackType | None,
) -> None:
    """
    Custom exception hook that logs uncaught exceptions.

    Keyboard interrupts are passed to the default handler; all other exceptions
    are logged as errors with full traceback information.

    Args:
        exc_type: The exception class.
        exc_value: The exception instance.
        exc_traceback: The traceback object.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = excepthook
