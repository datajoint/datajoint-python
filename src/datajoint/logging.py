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
            message = f"[{timestamp}][{record.levelname}]: {record.getMessage()}"
        elif record.levelno == JOBS:
            message = f"[{timestamp}][JOBS]: {record.getMessage()}"
        else:
            message = f"[{timestamp}] {record.getMessage()}"

        # Render exception/stack info like the base logging.Formatter does, so
        # that a logger.exception(...)/exc_info=... call never silently drops
        # the traceback (see #1516).
        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            message = f"{message}\n{record.exc_text}"
        if record.stack_info:
            message = f"{message}\n{self.formatStack(record.stack_info)}"
        return message


# Select output stream: stdout (default, no red highlighting) or stderr
# Configurable via DJ_LOG_STREAM=stdout|stderr
output_stream = sys.stderr if log_stream == "stderr" else sys.stdout
stream_handler = logging.StreamHandler(output_stream)
stream_handler.setFormatter(LevelAwareFormatter())

logger.setLevel(level=log_level)
logger.handlers = [stream_handler]

# NOTE: DataJoint intentionally does NOT install a process-wide sys.excepthook.
# Importing a library must not change how *unrelated* uncaught exceptions are
# reported in the host process. Uncaught exceptions are left to Python's default
# handler, which prints the full type/message/traceback to stderr. (See #1516.)
