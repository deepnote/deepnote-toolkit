""" This module contains the VirtualEnvironment class. """

import logging
import os
import subprocess
from pathlib import Path
from typing import Optional, Union

from .helper import get_current_python_version
from .server_process import ServerProcess

logger = logging.getLogger(__name__)


class VirtualEnvironment:
    """A class to manage virtual environments."""

    def __init__(self, path: Union[str, Path] = "~/venv", without_pip: bool = False):
        self._path = os.path.expanduser(path)
        self._site_packages_path = os.path.join(
            self._path, "lib", f"python{get_current_python_version()}", "site-packages"
        )
        self._activate_file_path = os.path.join(self._path, "bin", "activate")
        self._without_pip = without_pip

    def create(self) -> None:
        """
        Create a virtual environment at the specified path.
        """
        if os.path.isfile(self.activate_file_path):
            logger.info(
                "Skipping creation of virtual environment, because %s already exists.",
                self.activate_file_path,
            )
        else:

            command = [
                "python",
                "-m",
                "venv",
            ]
            if self._without_pip:
                command.append("--without-pip")
                logger.info("Creating virtual environment without pip.")
            command.append(self.path)

            self._run_command(command)
            logger.info("Virtual environment created successfully.")

    def execute(self, command: str) -> str:
        """
        Execute an arbitrary command with an option to use the virtual environment.

        :param command: The command to execute.
        :return: The standard output of the command if successful.
        :raises RuntimeError: If the command fails or the virtual environment is not found.
        """
        if not os.path.isfile(self.activate_file_path):
            raise RuntimeError(
                f"Virtual environment not found at {self.activate_file_path}"
            )

        full_command = [f". {self.activate_file_path} && {command}"]
        result = self._run_command(full_command, shell=True)
        return result.stdout

    def start_server(self, command: str, cwd: Optional[str] = None) -> ServerProcess:
        """
        Start a server process using the virtual environment.

        :param command: The command to start the server.
        :return: A started ServerProcess instance ready for use.
        :raises Exception: If the server fails to start.
        """
        full_command = f". {self.activate_file_path} && {command}"
        server_proc = ServerProcess(full_command, cwd=cwd)

        # Start the server internally and handle any startup errors
        server_proc.start()

        return server_proc

    def import_package_bundle(
        self, bundle_site_package_path: str, condition_env: Optional[str] = None
    ) -> None:
        """
        Import a package bundle to the virtual environment.

        :param bundle_site_package_path: Absolute path to the package bundle.
        :param condition_env: Optional environment variable name. If provided, the bundle
                         will only be loaded when this env var is set to 'true'.
        """
        pth_file_path = os.path.join(self._site_packages_path, "deepnote.pth")

        with open(pth_file_path, "a+", encoding="utf-8") as pth_file:
            if condition_env:
                # Write conditional import that checks environment variable
                pth_content = (
                    f"import os, sys; "
                    f"sys.path.insert(0, '{bundle_site_package_path}') "
                    f"if os.environ.get('{condition_env}', '').lower() == 'true' else None"
                )
                pth_file.write(pth_content + "\n")
                logger.info(
                    "Bundle was conditionally imported to the virtual environment "
                    f"(loads when {condition_env}=true)."
                )
            else:
                pth_file.write(bundle_site_package_path + "\n")
                logger.info(
                    "Bundle was successfully imported to the virtual environment."
                )

    def append_env_variable(self, env_variable: str, value: str) -> None:
        """
        Append an environment variable to the activate file of the virtual environment.

        :param env_variable: The environment variable to append.
        :param value: The value of the environment variable.
        """
        with open(self.activate_file_path, "a", encoding="utf-8") as activate_file:
            activate_file.write(f"export {env_variable}={value}\n")
        logger.info("Appended %s=%s to the virtual environment.", env_variable, value)

    @property
    def path(self) -> str:
        """The getter method for the path attribute."""
        return self._path

    @property
    def site_packages_path(self) -> str:
        """The getter method for the site_packages_path attribute."""
        return self._site_packages_path

    @property
    def activate_file_path(self) -> str:
        """The getter method for the activate_file_path attribute."""
        return self._activate_file_path

    def _run_command(
        self, command: list, shell: bool = False
    ) -> subprocess.CompletedProcess:
        """
        Run a command and handle errors.

        :param command: The command to execute.
        :param shell: Whether to execute the command through the shell.
        :return: The result of the subprocess.run call.
        :raises RuntimeError: If the command failed.
        """
        logger.info("Executing command: %s", command)

        result = subprocess.run(
            command, shell=shell, capture_output=True, text=True, check=False
        )

        if result.returncode != 0:
            error_message = result.stderr if result.stderr else result.stdout
            logger.error("Command '%s' failed with error: %s", command, error_message)
            raise RuntimeError(
                f"Command '{command}' failed with error: {error_message}"
            )

        return result
