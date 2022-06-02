import logging
import os
import sys
import io

logger = logging.getLogger(__name__.split(".")[0])

log_level = os.environ.get("DJ_LOG_LEVEL", "warning").upper()

log_format = logging.Formatter(
    "[%(asctime)s][%(funcName)s][%(levelname)s]: %(message)s"
)

stream_handler = logging.StreamHandler()  # default handler
stream_handler.setFormatter(log_format)

logger.setLevel(level=log_level)
logger.handlers = [stream_handler]


def excepthook(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    if logger.getEffectiveLevel() == 10:
        logger.debug(
            "Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback)
        )
    else:
        logger.error(f"Uncaught exception: {exc_value}")


sys.excepthook = excepthook


# https://github.com/tqdm/tqdm/issues/313#issuecomment-267959111
class TqdmToLogger(io.StringIO):
    """
    Output stream for TQDM which will output to logger module instead of
    the StdOut.
    """

    logger = None
    level = None
    buf = ""

    def __init__(self, logger, level=None):
        super(TqdmToLogger, self).__init__()
        self.logger = logger
        self.level = level or logging.INFO

    def write(self, buf):
        self.buf = buf.strip("\r\n\t ")

    def flush(self):
        self.logger.log(self.level, self.buf)
