"""Logging and error reporting functionality for the Deepnote runtime environment.

This module provides a logger manager for creating and configuring loggers
in the Deepnote environment, with support for file-based logging and
error reporting to the webapp.
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional
from urllib.error import URLError
from urllib.request import Request, urlopen

from deepnote_core.config.xdg_paths import XDGPaths

from .config import get_config
from .get_webapp_url import get_absolute_userpod_api_url, get_project_auth_headers


def report_error_to_webapp(
    error_type: str,
    error_message: str,
    extra_context: dict = None,
    retries: int = 2,
    retry_delay_seconds: float = 0.5,
) -> None:
    """
    Report toolkit errors to the Deepnote webapp via userpod-api with retry logic.

    Args:
        error_type: The type identifier for the error.
        error_message: The detailed error message.
        extra_context: An optional dictionary with additional context.
        retries: The number of times to retry sending the error after the initial attempt fails. Defaults to 0 (no retries).
        retry_delay_seconds: The delay in seconds between retry attempts. Defaults to 1,0.
    """

    # Always log locally as a fallback or for immediate visibility (original behavior)
    log_message = f"[{error_type}] {error_message}"
    if extra_context:
        log_message += f" | Context: {extra_context}"
    logging.warning(log_message)

    data = {"type": error_type, "message": error_message}
    if extra_context:
        data["context"] = extra_context

    error_url = None
    try:
        error_url = get_absolute_userpod_api_url("toolkit/errors")
    except Exception as e:
        # If URL generation fails, we can't attempt to send.
        logging.error(f"Failed to construct error report URL: {e}")
        return

    encoded_data = json.dumps(data).encode()
    headers = {"Content-Type": "application/json", **get_project_auth_headers()}

    for attempt in range(retries + 1):
        is_last_attempt = attempt == retries
        failure_log_level = logging.WARNING if is_last_attempt else logging.INFO

        try:
            if attempt > 0:
                logging.info(
                    f"Retrying error report to {error_url} (Attempt {attempt + 1}/{retries + 1})..."
                )
                time.sleep(retry_delay_seconds)

            request = Request(
                error_url, data=encoded_data, headers=headers, method="POST"
            )

            with urlopen(request, timeout=5) as response:
                if response.status == 200:
                    return

                # Handle non-200 status codes
                log_msg = f"Failed attempt {attempt + 1}/{retries + 1} to report error via API: {response.status} {response.reason}"
                logging.log(failure_log_level, log_msg)

        except URLError as e:
            log_msg = f"Failed attempt {attempt + 1}/{retries + 1} to report error via API (URLError): {e}"
            logging.log(failure_log_level, log_msg)
        except Exception as e:
            log_msg = f"Failed attempt {attempt + 1}/{retries + 1} to report error via API (Unexpected Error): {e}"
            logging.log(failure_log_level, log_msg)

    if retries > 0:
        logging.warning(
            f"Failed to report error '{error_type}' to {error_url} after {retries + 1} attempts."
        )


class WebappErrorHandler(logging.Handler):
    """Custom logging handler that reports ERROR level messages to the webapp.

    This handler captures ERROR level log messages and sends them to the
    Deepnote webapp using the report_error_to_webapp function.
    """

    def __init__(self) -> None:
        """Initialize the WebappErrorHandler with ERROR level."""
        super().__init__(level=logging.ERROR)

    def emit(self, record: logging.LogRecord) -> None:
        """Report the error message to the webapp when an ERROR level log is emitted.

        Args:
            record: The log record to process and report.
        """
        try:
            # Only report ERROR and above
            if record.levelno >= logging.ERROR:
                error_message = self.format(record)
                error_type = "TOOLKIT_RUNTIME_ERROR"

                # Extract extra context from the record if available
                # LogRecord always has __dict__, so we don't need hasattr
                extra_context = None

                # Try to get the 'extra' attribute first (if it was explicitly set)
                extra = getattr(record, "extra", None)
                if extra is not None:
                    extra_context = extra
                else:
                    # Extract any attributes that don't belong to the standard LogRecord
                    standard_attrs = {
                        "args",
                        "asctime",
                        "created",
                        "exc_info",
                        "exc_text",
                        "filename",
                        "funcName",
                        "levelname",
                        "levelno",
                        "lineno",
                        "module",
                        "msecs",
                        "message",
                        "msg",
                        "name",
                        "pathname",
                        "process",
                        "processName",
                        "relativeCreated",
                        "stack_info",
                        "thread",
                        "threadName",
                    }
                    extra_context = {
                        k: v
                        for k, v in record.__dict__.items()
                        if k not in standard_attrs and not k.startswith("_")
                    }
                    if not extra_context:  # Don't send empty dict
                        extra_context = None

                report_error_to_webapp(error_type, error_message, extra_context)
        except Exception:  # pylint: disable=broad-except
            # Avoid recursion by not logging anything here
            pass


class LoggerManager:
    """Manager for creating and configuring loggers in the Deepnote environment.

    This singleton class is responsible for creating and managing a logger instance
    that writes logs to a specified file from where they can be scraped by Loki,
    and also reports errors to the webapp via report_error_to_webapp.

    Attributes:
        log_file: The file path where logs will be written.
        level: The logging level (default is logging.DEBUG).
        logger: The configured logger instance.
    """

    _instance: Optional["LoggerManager"] = None
    level: int
    log_file: str
    logger: Optional[logging.Logger]
    initialized: bool
    _force_file_handler: bool

    def __new__(cls, *args, **kwargs) -> "LoggerManager":
        """Implement singleton pattern for LoggerManager.

        Returns:
            LoggerManager: The singleton instance of LoggerManager.
        """
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton logger and detach handlers (for tests)."""
        if cls._instance is not None:
            # Check if logger attribute exists and is initialized (not None)
            # Using hasattr here is appropriate since we're checking for existence
            # after a potentially partial initialization
            if hasattr(cls._instance, "logger") and cls._instance.logger is not None:
                for h in list(cls._instance.logger.handlers):
                    try:
                        h.flush()
                        cls._instance.logger.removeHandler(h)
                        h.close()
                    except Exception:
                        pass
        cls._instance = None

    def __init__(
        self,
        log_file: Optional[str] = None,
        level: int = logging.DEBUG,
    ) -> None:
        """Initialize the LoggerManager with the specified file and level.

        Args:
            log_file: The file path where logs will be written.
            level: The logging level (default is logging.DEBUG).
        """
        # Check if already initialized (singleton pattern)
        # We need to check for the attribute existence since __new__ doesn't initialize them
        if not hasattr(self, "initialized") or self.initialized is None:
            self.initialized = False
            self.logger = None
            self.level = logging.DEBUG
            self.log_file = ""
            self._force_file_handler = False

        # If we've already been initialized, allow updating the log level dynamically
        if self.initialized and level != self.level:
            self.level = level
            if self.logger is not None:
                self.logger.setLevel(level)
                for h in self.logger.handlers:
                    try:
                        h.setLevel(level)
                    except Exception:
                        pass
            # Do not re-create handlers or change paths if already initialized
            return

        # Use config (preferred). Avoid reading process env directly here.
        self._force_file_handler = False
        if log_file is None:
            log_dir = get_config().paths.log_dir

            # Config validation ensures log_dir is not None, defaulting if needed
            if not log_dir:
                # Align default with XDG specification
                log_dir_default = XDGPaths().log_dir
            else:
                log_dir_default = Path(log_dir)
                self._force_file_handler = True

            log_file = str((log_dir_default / "helpers.log"))

        # First-time initialization
        self.log_file = log_file
        self.level = level

        self.logger = self._create_logger()
        self._configure_error_handler()

        self.initialized = True

    def _configure_error_handler(self) -> None:
        """Configure custom error handling to report errors to the webapp.

        Adds the WebappErrorHandler to the logger if not in CI environment.
        """
        # Add a custom handler for ERROR level logs to report them to the webapp
        try:
            if not get_config().runtime.ci:
                self.logger.addHandler(WebappErrorHandler())
        except Exception:
            # Fallback to env behavior for CI when config unavailable
            if not bool(os.environ.get("CI")):
                self.logger.addHandler(WebappErrorHandler())

    def _create_logger(self) -> logging.Logger:
        """Create and configure a logger instance.

        Creates a logger that writes to stdout in CI environments
        and to a file in non-CI environments.

        Returns:
            The configured logger instance.
        """
        logger_instance = logging.getLogger("file_logger")
        logger_instance.setLevel(self.level)
        logger_instance.propagate = False

        is_ci = bool(get_config().runtime.ci)

        if is_ci and not self._force_file_handler:
            stdout_handler = logging.StreamHandler(sys.stdout)
            stdout_handler.setLevel(self.level)
            formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
            stdout_handler.setFormatter(formatter)
            logger_instance.addHandler(stdout_handler)
            return logger_instance

        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        file_handler = logging.FileHandler(self.log_file)
        file_handler.setLevel(self.level)

        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        file_handler.setFormatter(formatter)

        logger_instance.addHandler(file_handler)
        return logger_instance

    def get_logger(self) -> logging.Logger:
        """Get the configured logger instance.

        Returns:
            The configured logger instance.
        """
        return self.logger


def get_logger(
    log_file: Optional[str] = None, level: int = logging.DEBUG
) -> logging.Logger:
    """Convenience function to get a configured logger instance.

    This is a shorthand for LoggerManager().get_logger().

    Args:
        log_file: The file path where logs will be written.
        level: The logging level (default is logging.DEBUG).

    Returns:
        The configured logger instance.
    """
    return LoggerManager(log_file=log_file, level=level).get_logger()
