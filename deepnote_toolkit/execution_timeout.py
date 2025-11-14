"""
This module provides execution timeout monitoring for Jupyter notebook cells.
It can detect long-running executions and optionally send warnings or interrupt them.
"""

import os
import signal
import threading
from typing import Optional

import requests
from IPython.core.interactiveshell import ExecutionInfo, ExecutionResult

from .get_webapp_url import get_webapp_url
from .logging import LoggerManager


class ExecutionTimeoutMonitor:
    """
    Monitors execution duration and can send warnings or interrupt stuck executions.
    """

    def __init__(
        self,
        warning_threshold_seconds: int = 240,
        timeout_seconds: int = 300,
        enable_auto_interrupt: bool = False,
    ):
        """
        Initialize the execution timeout monitor.

        Args:
            warning_threshold_seconds: Seconds after which to send a warning (default: 240s = 4min)
            timeout_seconds: Seconds after which to consider execution stuck (default: 300s = 5min)
            enable_auto_interrupt: Whether to automatically interrupt stuck executions (default: False)
        """
        self.logger = LoggerManager.get_logger("execution_timeout")
        self.warning_threshold = warning_threshold_seconds
        self.timeout_threshold = timeout_seconds
        self.enable_auto_interrupt = enable_auto_interrupt
        self.current_execution: Optional[dict] = None
        self.warning_timer: Optional[threading.Timer] = None
        self.timeout_timer: Optional[threading.Timer] = None

    def on_pre_execute(self, info: ExecutionInfo) -> None:
        """
        Called before executing a cell.
        Starts timers for warning and timeout.
        """
        import time

        cell_preview = info.raw_cell[:100] if info.raw_cell else "<empty>"

        self.current_execution = {
            "code": cell_preview,
            "start": time.time(),
        }

        # Start warning timer
        if self.warning_threshold > 0:
            self.warning_timer = threading.Timer(
                self.warning_threshold, self._send_warning
            )
            self.warning_timer.daemon = True
            self.warning_timer.start()

        # Start timeout timer
        if self.enable_auto_interrupt and self.timeout_threshold > 0:
            self.timeout_timer = threading.Timer(
                self.timeout_threshold, self._interrupt_execution
            )
            self.timeout_timer.daemon = True
            self.timeout_timer.start()

        self.logger.debug(
            "Timeout monitoring started: warning=%ds, timeout=%ds, auto_interrupt=%s",
            self.warning_threshold,
            self.timeout_threshold,
            self.enable_auto_interrupt,
        )

    def on_post_execute(self, result: ExecutionResult) -> None:
        """
        Called after executing a cell.
        Cancels any pending timers.
        """
        self._cancel_timers()
        self.current_execution = None

    def _cancel_timers(self) -> None:
        """Cancel all active timers."""
        if self.warning_timer:
            self.warning_timer.cancel()
            self.warning_timer = None
        if self.timeout_timer:
            self.timeout_timer.cancel()
            self.timeout_timer = None

    def _send_warning(self) -> None:
        """Send warning when execution is running longer than threshold."""
        if not self.current_execution:
            return

        import time

        duration = time.time() - self.current_execution["start"]
        code_preview = self.current_execution["code"][:50]

        self.logger.warning(
            "LONG_EXECUTION | duration=%.1fs | preview=%s",
            duration,
            code_preview.replace("\n", "\\n"),
        )

        # Try to report to webapp
        self._report_to_webapp(duration, code_preview, warning=True)

    def _interrupt_execution(self) -> None:
        """Interrupt execution after timeout threshold is exceeded."""
        if not self.current_execution:
            return

        import time

        duration = time.time() - self.current_execution["start"]

        self.logger.error(
            "TIMEOUT_INTERRUPT | duration=%.1fs | Sending SIGINT to interrupt execution",
            duration,
        )

        # Report to webapp before interrupting
        self._report_to_webapp(
            duration, self.current_execution["code"][:50], warning=False
        )

        # Send SIGINT to interrupt the execution (simulates Ctrl+C)
        try:
            os.kill(os.getpid(), signal.SIGINT)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error("Failed to send SIGINT: %s", e)

    def _report_to_webapp(
        self, duration: float, code_preview: str, warning: bool
    ) -> None:
        """
        Report execution warning/timeout to webapp.

        Args:
            duration: Execution duration in seconds
            code_preview: Preview of the code being executed
            warning: Whether this is a warning (True) or timeout (False)
        """
        try:
            webapp_url = get_webapp_url()
            project_id = os.getenv("DEEPNOTE_PROJECT_ID")

            if not webapp_url or not project_id:
                self.logger.debug(
                    "Webapp URL or project ID not available, skipping report"
                )
                return

            endpoint = (
                "warning" if warning else "timeout"
            )
            url = f"{webapp_url}/userpod-api/{project_id}/execution/{endpoint}"

            payload = {
                "duration": duration,
                "code_preview": code_preview,
                "threshold": (
                    self.warning_threshold if warning else self.timeout_threshold
                ),
            }

            response = requests.post(url, json=payload, timeout=2)
            response.raise_for_status()

            self.logger.debug("Successfully reported %s to webapp", endpoint)

        except Exception as e:  # pylint: disable=broad-exception-caught
            self.logger.error("Failed to report to webapp: %s", e)


# Global instance
_timeout_monitor: Optional[ExecutionTimeoutMonitor] = None


def setup_execution_timeout_monitor(
    warning_threshold_seconds: int = 240,
    timeout_seconds: int = 300,
    enable_auto_interrupt: bool = False,
) -> None:
    """
    Set up execution timeout monitoring.

    This is optional and should be called during runtime initialization if needed.

    Args:
        warning_threshold_seconds: Seconds after which to send a warning (default: 240s = 4min)
        timeout_seconds: Seconds after which to consider execution stuck (default: 300s = 5min)
        enable_auto_interrupt: Whether to automatically interrupt stuck executions (default: False)
    """
    global _timeout_monitor  # pylint: disable=global-statement

    try:
        from IPython import get_ipython

        ip = get_ipython()
        if ip is None:
            LoggerManager.get_logger("execution_timeout").warning(
                "IPython instance not available, skipping timeout monitor setup"
            )
            return

        _timeout_monitor = ExecutionTimeoutMonitor(
            warning_threshold_seconds=warning_threshold_seconds,
            timeout_seconds=timeout_seconds,
            enable_auto_interrupt=enable_auto_interrupt,
        )

        # Register event handlers
        ip.events.register("pre_execute", _timeout_monitor.on_pre_execute)
        ip.events.register("post_execute", _timeout_monitor.on_post_execute)

        LoggerManager.get_logger("execution_timeout").info(
            "Execution timeout monitor initialized: warning=%ds, timeout=%ds, auto_interrupt=%s",
            warning_threshold_seconds,
            timeout_seconds,
            enable_auto_interrupt,
        )

    except Exception as e:  # pylint: disable=broad-exception-caught
        LoggerManager.get_logger("execution_timeout").error(
            "Failed to set up timeout monitor: %s", e
        )
