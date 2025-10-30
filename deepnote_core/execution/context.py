"""Execution context protocols and data classes.

This module defines the interface that execution contexts must implement,
allowing the same action handlers to work in different environments.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Mapping, Optional, Protocol, runtime_checkable


@runtime_checkable
class ProcessHandle(Protocol):
    """Protocol for process handles returned by actions."""

    def terminate(self) -> None:
        """Terminate the process."""
        ...

    def wait(self, timeout: Optional[float] = None) -> int:
        """Wait for process to complete and return exit code."""
        ...

    def poll(self) -> Optional[int]:
        """Check if process is still running."""
        ...

    def kill(self) -> None:
        """Forcefully terminate the process."""
        ...


@dataclass
class ExecutionResult:
    """Result of executing an action."""

    process: Optional[ProcessHandle] = None
    is_long_running: bool = False
    success: bool = True
    message: Optional[str] = None


@runtime_checkable
class ExecutionContext(Protocol):
    """Protocol defining the execution context interface.

    This abstraction allows the same action handlers to work
    in different environments (pip/CLI vs installer).
    """

    @property
    def env(self) -> Mapping[str, str]:
        """Effective environment variables for child processes."""
        ...

    @property
    def logger(self) -> logging.Logger:
        """Logger for observability."""
        ...

    def spawn(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> ProcessHandle:
        """Start a process with the given arguments.

        Args:
            argv: Command and arguments as a list (to avoid shell injection)
            env_override: Optional environment variables to merge with context env
            cwd: Optional working directory for the process

        Returns:
            A handle to the started process
        """
        ...

    def run_one_off(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """Execute a one-off command and wait for completion.

        Args:
            argv: Command and arguments as a list
            env_override: Optional environment variables to merge with context env
            timeout: Optional timeout in seconds

        Returns:
            Exit code of the command
        """
        ...

    def check_dependency(self, module: str, hint: str) -> None:
        """Check if a module is available, raise if not.

        Args:
            module: Module name to check
            hint: User-friendly installation hint

        Raises:
            SystemExit: If module is not available
        """
        ...

    def python_executable(self) -> str:
        """Return the path to the Python interpreter.

        Returns:
            Path to Python executable
        """
        ...
