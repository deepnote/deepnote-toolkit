""" This module provides functions to manage Jupyter kernels. """

import json
import logging
import os
import re
from pathlib import Path

from .helper import append_to_file
from .virtual_environment import VirtualEnvironment

logger = logging.getLogger(__name__)


def setup_non_python_kernels(venv: VirtualEnvironment, config_directory_path: str):
    """
    User environment can be configured in a way where also non python kernels are available.
    For example user can user R or Stata kernels.

    For all non-Python kernels, our standard approach to inserting integration environment
    variables does not work.We need to add the environment variables to the kernel before
    Jupyter starts to ensure that all kernels will inherit the integration environment variables.
    [TODO: Find more native solution]

    This function will setup the IR kernel if it is available.
    """

    logger.info("[Setup non python kernels] Checking for existing kernels")
    kernelspec_data = get_kernel_specs(venv)
    kernels = list(kernelspec_data["kernelspecs"].keys())
    logger.info("[Setup non python kernels] Kernels found: %s", kernels)

    non_python_kernel_exists = any("python" not in kernel for kernel in kernels)

    if non_python_kernel_exists:
        setup_integration_vars_script(venv, config_directory_path)
        ensure_symlinked_python_in_kernel_spec(venv, kernelspec_data["kernelspecs"])

    if "ir" in kernels:
        logger.info(
            "[Setup non python kernels] IR kernel detected. Running setup logic."
        )
        setup_ir_kernel()
        logger.info("[Setup non python kernels] IR kernel setup complete.")


def get_kernel_specs(venv: VirtualEnvironment):
    """
    Get Jupyter kernel specs in the given virtual environment.

    Parameters:
    venv (VirtualEnvironment): The virtual environment in which to list the kernels.

    Returns:
    dict: A dictionary of available kernel specifications.
    """
    try:
        # Execute the command to list kernels in JSON format
        output = venv.execute("jupyter kernelspec list --json")
        logger.info(
            "[Setup non python kernels] jupyter kernelspec list output: %s", output
        )
        # Parse the JSON output
        return json.loads(output)
    except json.JSONDecodeError as e:
        # Handle JSON decoding errors
        logger.error(f"Error decoding JSON output: {e}")
    except Exception as e:
        # Handle other exceptions
        logger.error(f"An error occurred while getting kernels: {e}")
    return {}


def setup_integration_vars_script(venv: VirtualEnvironment, config_directory_path: str):
    """
    Setup the integration variables script.
    """
    logger.info(
        "[Setup non python kernels] Setting command to fetch integration variables in venv activation"
    )
    integration_vars_script_path = os.path.join(
        config_directory_path, "scripts/get_integration_vars.py"
    )
    append_to_file(
        venv.activate_file_path,
        f'eval "$({integration_vars_script_path})"',
    )
    logger.info(
        "[Setup non python kernels] Integration variables script added to venv activation"
    )


def ensure_symlinked_python_in_kernel_spec(venv: VirtualEnvironment, kernelspecs: dict):
    """
    Ensure that the python kernel spec is symlinked to the python binary from the venv.
    """
    logger.debug("Received kernelspecs: %s", kernelspecs)
    pattern = re.compile(r".*python.*")

    for kernel_name, kernel_data in kernelspecs.items():
        kernel_dir = Path(kernel_data["resource_dir"]) / "kernel.json"
        temp_file = kernel_dir.with_suffix(".json.tmp")

        logger.info(
            "[Setup non python kernels] Ensuring %s uses symlinked python in kernel spec: %s",
            kernel_name,
            kernel_dir,
        )
        try:
            # Load the kernel.json file
            with kernel_dir.open("r") as f:
                kernel_spec = json.load(f)

            # Modify the argv array if it exists
            if "argv" in kernel_spec and isinstance(kernel_spec["argv"], list):
                if pattern.match(kernel_spec["argv"][0]):
                    kernel_spec["argv"][0] = "python"

            # Write to temporary file first
            with temp_file.open("w") as f:
                json.dump(kernel_spec, f, indent=4)

            # Atomic rename to target file
            temp_file.replace(kernel_dir)

            logger.info(
                "[Setup non python kernels] Modified kernel spec: %s",
                kernel_dir,
            )
        except Exception as e:
            logger.error(
                "[Setup non python kernels] Failed to process kernel spec: %s: %s",
                kernel_dir,
                e,
            )


def setup_ir_kernel():
    """
    Setup the IR kernel if it is available.
    """
    os.environ["R_LIBS"] = "/work/.R/library"
    append_to_file("~/.Rprofile", "setwd('~/work')")
