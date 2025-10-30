"""Module to manage a server process."""

import logging
import os
import subprocess
import sys
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class ServerProcess:
    """A class to manage a server process."""

    def __init__(self, command: str, cwd: Optional[str] = None):
        """
        Initialize the ServerProcess with the given command.

        :param command: The command to start the server.
        """
        self.command = command
        self.cwd = cwd
        self.process = None
        self.stdout_thread = None
        self.stderr_thread = None
        self._pid: Optional[int] = None

    def start(self, retries: int = 3, delay: float = 0.2) -> subprocess.Popen:
        """
        Start the server process.

        This method is called by VirtualEnvironment.start_server() to actually
        start the process. It should not be called by external code using the
        ServerProcess through the ProcessManager protocol.

        :param retries: Number of times to retry starting the process on failure (default: 3).
        :param delay: Delay in seconds between retries (default: 0.2).
        :return: The started process.
        :raises Exception: If the process fails to start after all retries.
        """
        return self._start(retries=retries, delay=delay)

    def _start(self, retries: int = 3, delay: float = 0.2) -> subprocess.Popen:
        """
        Internal method to start the server process with retry logic.

        This method is intended to be called internally, not by external code.

        :param retries: Number of times to retry starting the process on failure (default: 3).
        :param delay: Delay in seconds between retries (default: 0.2).
        :return: The started process.
        :raises Exception: If the process fails to start after all retries.
        """
        env = os.environ.copy()
        env["PYTHONUNBUFFERED"] = "1"

        attempt = 0
        last_exception = None
        while attempt < retries:
            try:
                logger.info(
                    "Starting the server via command: %s (attempt %d/%d)",
                    self.command,
                    attempt + 1,
                    retries,
                )
                self.process = subprocess.Popen(
                    self.command,
                    cwd=self.cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.PIPE,
                    bufsize=0,
                    env=env,
                    universal_newlines=True,
                    shell=True,
                )

                # Start threads to read stdout and stderr
                # Store the PID for ProcessManager compatibility
                self._pid = self.process.pid

                # Start threads to read stdout and stderr
                self.stdout_thread = threading.Thread(target=self._read_output)
                self.stderr_thread = threading.Thread(target=self._read_error)

                self.stdout_thread.start()
                self.stderr_thread.start()

                return self.process
            except Exception as e:
                last_exception = e
                logger.error(
                    "Failed to start server process (attempt %d/%d): %s",
                    attempt + 1,
                    retries,
                    str(e),
                )
                attempt += 1
                if attempt < retries:
                    logger.info("Retrying in %.1f seconds...", delay)
                    time.sleep(delay)
        logger.critical(
            "All %d attempts to start the server process failed. Raising last exception.",
            retries,
        )
        raise last_exception

    def terminate(self) -> None:
        """
        Terminate the server process and ensure all output is processed.
        """
        if self.process:
            logger.info("Terminating the server process: %s", self.command)
            self.process.terminate()

            # Join the threads to make sure all output is processed
            self.stdout_thread.join()
            self.stderr_thread.join()

            # Close the streams
            self.process.stdout.close()
            self.process.stderr.close()

    def wait(self, timeout: Optional[float] = None) -> int:
        """
        Wait for the server process to complete and join the threads.

        :param timeout: Maximum time to wait in seconds (None for no timeout)
        :return: The exit code of the process
        """
        try:
            # Wait for the server process to complete
            returncode = self.process.wait(timeout=timeout)
        except KeyboardInterrupt:
            self.terminate()
            returncode = self.process.returncode or -1
        except subprocess.TimeoutExpired:
            raise

        if self.stdout_thread and self.stdout_thread.is_alive():
            self.stdout_thread.join(timeout=1.0)
        if self.stderr_thread and self.stderr_thread.is_alive():
            self.stderr_thread.join(timeout=1.0)

        return returncode if returncode is not None else -1

    def _read_output(self) -> None:
        """
        Read the process output in real time and write it to stdout.
        """
        for line in iter(self.process.stdout.readline, ""):
            if line:
                sys.stdout.write(line)
                sys.stdout.flush()  # Ensure that the output is flushed immediately

    def _read_error(self) -> None:
        """
        Read the process error in real time and write it to stderr.
        """
        for line in iter(self.process.stderr.readline, ""):
            if line:
                sys.stderr.write(line)
                sys.stderr.flush()  # Ensure that the error is flushed immediately

    # ProcessManager protocol compatibility methods
    @property
    def pid(self) -> Optional[int]:
        """Get the process ID."""
        if self.process:
            return self.process.pid
        return self._pid

    @property
    def returncode(self) -> Optional[int]:
        """Get the return code of the process."""
        if self.process:
            return self.process.returncode
        return None

    def poll(self) -> Optional[int]:
        """Check if the process has terminated and return its exit code."""
        if self.process:
            return self.process.poll()
        return None

    def kill(self) -> None:
        """Kill the server process forcefully."""
        if self.process:
            logger.info("Killing the server process: %s", self.command)
            self.process.kill()
