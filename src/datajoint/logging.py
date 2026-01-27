import logging
import os
import sys

# Custom log level for job/populate status messages
# DEBUG (10) < JOBS (15) < INFO (20) < WARNING (30) < ERROR (40)
JOBS = 15
logging.addLevelName(JOBS, "JOBS")


def jobs(self, message, *args, **kwargs):
    """Log job status messages (make start/success/error)."""
    if self.isEnabledFor(JOBS):
        self._log(JOBS, message, args, **kwargs)


logging.Logger.jobs = jobs

logger = logging.getLogger(__name__.split(".")[0])

log_level = os.getenv("DJ_LOG_LEVEL", "info").upper()
log_stream = os.getenv("DJ_LOG_STREAM", "stdout").lower()


class LevelAwareFormatter(logging.Formatter):
    """Format INFO messages cleanly, show level for warnings/errors and JOBS."""

    def format(self, record):
        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        if record.levelno >= logging.WARNING:
            return f"[{timestamp}][{record.levelname}]: {record.getMessage()}"
        elif record.levelno == JOBS:
            return f"[{timestamp}][JOBS]: {record.getMessage()}"
        else:
            return f"[{timestamp}] {record.getMessage()}"


# Select output stream: stdout (default, no red highlighting) or stderr
# Configurable via DJ_LOG_STREAM=stdout|stderr
output_stream = sys.stderr if log_stream == "stderr" else sys.stdout
stream_handler = logging.StreamHandler(output_stream)
stream_handler.setFormatter(LevelAwareFormatter())

logger.setLevel(level=log_level)
logger.handlers = [stream_handler]


def excepthook(exc_type, exc_value, exc_traceback):
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    logger.error("Uncaught exception", exc_info=(exc_type, exc_value, exc_traceback))


sys.excepthook = excepthook
