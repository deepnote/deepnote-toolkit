"""Installer-specific execution context implementation."""

import logging
import shlex
from types import MappingProxyType
from typing import Dict, List, Mapping, Optional, Protocol, runtime_checkable

from deepnote_core.execution import ProcessHandle


@runtime_checkable
class VenvProtocol(Protocol):
    """Protocol for VirtualEnvironment interface used by installer."""

    def execute(self, command: str) -> str:
        """Execute a command in the virtual environment."""
        ...

    def start_server(self, command: str, cwd: Optional[str] = None) -> "ServerProcess":
        """Start a server process in the virtual environment.

        Returns an already-started server process ready for use.
        Handles startup errors internally.
        """
        ...


class ServerProcess(ProcessHandle, Protocol):
    """Protocol for server process objects returned by start_server.

    Extends ProcessHandle to indicate compatibility while maintaining
    the specific typing for VirtualEnvironment.start_server().
    """

    pass


class InstallerExecutionContext:
    """Execution context for installer environment."""

    def __init__(
        self,
        venv: VenvProtocol,
        env: Optional[Dict[str, str]] = None,
        logger: Optional[logging.Logger] = None,
    ):
        """Initialize installer execution context.

        Args:
            venv: VirtualEnvironment instance
            env: Environment variables
            logger: Logger instance
        """
        self.venv = venv
        self._env = env or {}
        self._logger = logger or logging.getLogger(__name__)

    @property
    def env(self) -> Mapping[str, str]:
        """Effective environment variables for child processes (read-only)."""
        return MappingProxyType(self._env)

    @property
    def logger(self) -> logging.Logger:
        """Logger for observability."""
        return self._logger

    def spawn(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        cwd: Optional[str] = None,
    ) -> ProcessHandle:
        """Start a process using venv.start_server.

        Args:
            argv: Command and arguments as a list
            env_override: Optional environment variables to set for the command
            cwd: Working directory for the process

        Returns:
            ServerProcess handle

        Note:
            Uses shlex.join() because the installer's start_server API
            requires a string command rather than argv list.
            For env_override, prefixes environment variables to the command string
            since installer uses shell execution.
        """
        if env_override:
            # Build command with environment variable prefix
            # Filter to only include new/changed env vars not in self.env
            env_to_prefix = {}
            for k, v in env_override.items():
                if k not in self.env or self.env.get(k) != v:
                    env_to_prefix[k] = v

            if env_to_prefix:
                env_prefix = " ".join(
                    f"{k}={shlex.quote(v)}" for k, v in env_to_prefix.items()
                )
                command = f"{env_prefix} {shlex.join(argv)}"
            else:
                command = shlex.join(argv)
        else:
            command = shlex.join(argv)

        self._logger.debug(f"Starting server in venv: {command}")

        # start_server now returns an already-started server process
        server_proc = self.venv.start_server(command, cwd=cwd)

        # Quick poll to catch fast failures
        exit_code = server_proc.poll()
        if exit_code is not None and exit_code != 0:
            self._logger.warning(
                f"Process failed quickly with exit code {exit_code}: {command}"
            )

        return server_proc

    def run_one_off(
        self,
        argv: List[str],
        env_override: Optional[Dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> int:
        """Execute a one-off command using venv.execute.

        Args:
            argv: Command and arguments as a list
            env_override: Optional environment variables to merge with context env
            timeout: Optional timeout in seconds

        Returns:
            Exit code of the command (0 for success)
        """
        # Build the command with environment variables if needed
        if env_override:
            # Build command with environment variable prefix
            env_to_prefix = {}
            for k, v in env_override.items():
                if k not in self.env or self.env.get(k) != v:
                    env_to_prefix[k] = v

            if env_to_prefix:
                env_prefix = " ".join(
                    f"{k}={shlex.quote(v)}" for k, v in env_to_prefix.items()
                )
                command = f"{env_prefix} {shlex.join(argv)}"
            else:
                command = shlex.join(argv)
        else:
            command = shlex.join(argv)

        # Add timeout if specified
        if timeout:
            # Use timeout command if available
            command = f"timeout {int(timeout)} {command}"

        self._logger.debug(f"Running one-off command in venv: {command}")

        try:
            self.venv.execute(command)
            return 0
        except Exception as e:
            self._logger.warning(f"Command failed: {command} - {e}")
            # Check if it was a timeout
            if timeout and "timeout" in str(e).lower():
                return 124  # Standard timeout exit code
            return 1

    def check_dependency(self, module: str, hint: str) -> None:
        """Check dependency by attempting to execute a Python import.

        Args:
            module: Module name to check
            hint: User-friendly installation hint

        Note:
            In installer context, dependencies should already be installed
            in the venv, so this primarily serves as a sanity check.
        """
        try:
            # Try to import the module in the venv Python
            check_cmd = f'python -c "import {module}"'
            self.venv.execute(check_cmd)
            self._logger.debug(f"Dependency {module} is available in venv")
        except Exception as e:
            self._logger.error(f"Missing dependency in venv: {module} - {e}")
            raise SystemExit(f"Missing dependency: {module}. Install: {hint}")

    def python_executable(self) -> str:
        """Return 'python' for installer environment.

        Returns:
            'python' which will be resolved within the venv

        Note:
            In the installer context, we use 'python' which gets
            resolved to the correct interpreter inside the venv.
        """
        return "python"
