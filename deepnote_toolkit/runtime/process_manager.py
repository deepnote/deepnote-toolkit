"""Process lifecycle management with proper cleanup."""

from __future__ import annotations

import atexit
import logging
import signal
import subprocess
import sys
from contextlib import contextmanager
from types import FrameType, TracebackType
from typing import Iterator, List, Literal, Optional, Protocol, Type, runtime_checkable

logger = logging.getLogger(__name__)


@runtime_checkable
class SupportsProcess(Protocol):
    """Protocol for process-like objects that can be managed."""

    pid: Optional[int]

    def poll(self) -> Optional[int]:
        """Check if process has terminated."""
        ...

    def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for process to terminate."""
        ...

    def terminate(self) -> None:
        """Terminate the process."""
        ...

    def kill(self) -> None:
        """Kill the process."""
        ...


class ProcessManager:
    """Manages subprocess lifecycle with proper cleanup."""

    def __init__(self):
        self.processes: List[SupportsProcess] = []
        self._cleanup_registered = False
        self._original_sigterm_handler: Optional[object] = None

    def add_process(self, proc: SupportsProcess) -> None:
        """Add a process to be managed.

        Args:
            proc: Process object conforming to SupportsProcess protocol
        """
        self.processes.append(proc)
        logger.info(f"Managing process {proc.pid}")

        # Register cleanup on first process
        if not self._cleanup_registered:
            atexit.register(self.cleanup_all)
            # Also register SIGTERM handler for graceful shutdown
            self._register_signal_handlers()
            self._cleanup_registered = True

    def cleanup_all(self) -> None:
        """Clean up all managed processes."""
        if not self.processes:
            return

        logger.info(f"Cleaning up {len(self.processes)} processes")
        for proc in self.processes:
            self._terminate_process(proc)

        # Clear the list after cleanup
        self.processes.clear()

    def _register_signal_handlers(self) -> None:
        """Register SIGTERM handler for graceful shutdown."""
        try:
            # Save the original handler
            self._original_sigterm_handler = signal.signal(
                signal.SIGTERM, self._handle_sigterm
            )
            logger.debug("Registered SIGTERM handler for process cleanup")
        except (OSError, ValueError) as e:
            # Can't register signal handlers in some environments (e.g., threads)
            logger.debug(f"Could not register SIGTERM handler: {e}")

    def _handle_sigterm(self, signum: int, frame: Optional[FrameType]) -> None:
        """Handle SIGTERM signal by cleaning up processes."""
        logger.info("Received SIGTERM, cleaning up processes...")
        self.cleanup_all()

        # Restore and call original handler if it exists
        if self._original_sigterm_handler is not None:
            signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            if callable(self._original_sigterm_handler):
                self._original_sigterm_handler(signum, frame)

        sys.exit(128 + signum)

    def _terminate_process(self, proc: SupportsProcess) -> None:
        """Safely terminate a single process.

        Args:
            proc: Process object conforming to SupportsProcess protocol
        """
        if proc.poll() is not None:
            # Process already terminated
            return

        try:
            logger.info(f"Terminating process {proc.pid}")
            proc.terminate()

            try:
                proc.wait(timeout=5)
                logger.info(f"Process {proc.pid} terminated gracefully")
            except subprocess.TimeoutExpired:
                logger.warning(f"Process {proc.pid} did not terminate, forcing kill")
                proc.kill()

                try:
                    proc.wait(timeout=2)
                    logger.info(f"Process {proc.pid} killed successfully")
                except subprocess.TimeoutExpired:
                    logger.error(f"Failed to kill process {proc.pid} - may be zombied")

        except ProcessLookupError:
            # Process already gone
            logger.debug(f"Process {proc.pid} already terminated")
        except Exception as e:
            logger.error(f"Error terminating process {proc.pid}: {e}", exc_info=True)

    def check_processes(self) -> List[SupportsProcess]:
        """Check for terminated processes and return list of dead ones.

        Returns:
            List of process objects that have terminated
        """
        alive_processes = []
        dead_processes = []

        for proc in self.processes:
            poll_result = proc.poll()
            if poll_result is not None:
                dead_processes.append(proc)
                returncode = getattr(proc, "returncode", poll_result)
                logger.warning(f"Process {proc.pid} exited with code {returncode}")
            else:
                alive_processes.append(proc)

        # Update our list to only contain alive processes
        self.processes = alive_processes

        return dead_processes

    def __enter__(self) -> ProcessManager:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        self.cleanup_all()

        # Unregister atexit hook to avoid double cleanup
        if self._cleanup_registered:
            try:
                atexit.unregister(self.cleanup_all)
            except (AttributeError, ValueError):
                # AttributeError: unregister not available in older Python
                # ValueError: function not registered
                pass

        # Restore original signal handler if we changed it
        if self._cleanup_registered and self._original_sigterm_handler is not None:
            try:
                signal.signal(signal.SIGTERM, self._original_sigterm_handler)
            except (OSError, ValueError):
                pass  # Can't restore in some environments

        return False  # Don't suppress exceptions


@contextmanager
def managed_processes() -> Iterator[ProcessManager]:
    """Context manager for process lifecycle management."""
    with ProcessManager() as manager:
        yield manager
