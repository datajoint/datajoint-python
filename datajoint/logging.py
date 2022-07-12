import logging
import os
import sys
import io

logger = logging.getLogger(__name__.split(".")[0])

log_level = os.getenv("DJ_LOG_LEVEL", "info").upper()

log_format = logging.Formatter("[%(asctime)s][%(levelname)s]: %(message)s")

stream_handler = logging.StreamHandler()  # default handler
stream_handler.setFormatter(log_format)

logger.setLevel(level=log_level)
logger.handlers = [stream_handler]


def excepthook(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = excepthook
