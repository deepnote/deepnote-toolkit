""" Module to create symlinks as part of installing toolkit."""

import logging
import os
import shutil
from pathlib import Path
from typing import Union

logger = logging.getLogger(__name__)


def create_work_symlink(work_mountpoint: Union[str, Path]) -> None:
    """
    Create a symlink for /work. The original source of the data is mounted at work_mountpoint.
    """
    is_dev_mode = (
        os.environ.get("DEEPNOTE_RUNNING_IN_DEV_MODE", "false").lower() == "true"
    )
    project_id = os.environ.get("PROJECT_ID")

    if is_dev_mode and project_id:
        if os.path.islink("/work"):
            os.unlink("/work")

        # In dev mode, all projects are mounted under /datasets/_deepnote_work/projects.
        # If project_id is available, create a symlink to the project-specific path.
        project_specific_path = os.path.join(work_mountpoint, "projects", project_id)

        logger.info("Creating symlink for /work pointing to %s", project_specific_path)

        os.symlink(project_specific_path, "/work")
        return

    if os.path.exists("/work") or os.path.ismount("/work") or os.path.islink("/work"):
        logger.warning("/work already exists; not creating symlink.")
    else:
        logger.info("Creating symlink for /work pointing to /datasets/_deepnote_work")
        os.symlink(work_mountpoint, "/work")


def create_home_work_symlink() -> None:
    """
    Create a symlink from the user's home directory (~/work) to the /work directory.
    This supports user custom settings by linking the home work directory to the known /work directory.
    """
    home_work_path = os.path.expanduser("~/work")

    if os.path.islink(home_work_path):
        os.unlink(home_work_path)
    else:
        shutil.rmtree(home_work_path, ignore_errors=True)

    os.symlink("/work", home_work_path)
    logger.info("Successfully created symlink from ~/work to /work.")


def create_python_symlink() -> None:
    """
    Create a symlink for python in /usr/local/bin if it does not already exist.
    """
    python_symlink_path = "/usr/local/bin/python"
    if not os.path.isfile(python_symlink_path):
        logger.info("Creating symlink for python")
        python_path = shutil.which("python")
        if python_path:
            os.symlink(python_path, python_symlink_path)
        else:
            logger.error("Python executable not found.")
    else:
        logger.warning("Symlink for python already exists.")


def create_notebook_to_jupyter_server_symlink(
    system_site_packages_path: str, server_site_packages_path: str
) -> None:
    """
    Symlinks a 'notebook' package directory in site-packages for compatibility with
    custom kernels (e.g. Stata) to `jupyter_server`. This is necessary for some
    custom kernels to work.

    Args:
        system_site_packages_path (str): The path to the system site-packages directory, where 'notebook' is to be symlinked.
        server_site_packages_path (str): The path to the server site-packages directory, where 'jupyter_server' is installed.
    """
    notebook_path = os.path.join(system_site_packages_path, "notebook")
    jupyter_server_path = os.path.join(server_site_packages_path, "jupyter_server")

    system_site_exists = os.path.exists(system_site_packages_path)
    if not system_site_exists:
        logger.info(
            f"system_site_packages_path does not exist: {system_site_packages_path}; skipping symlink creation."
        )
        return

    server_site_exists = os.path.exists(server_site_packages_path)
    if not server_site_exists:
        logger.info(
            f"server_site_packages_path does not exist: {server_site_packages_path}; skipping symlink creation."
        )
        return

    # If there's already a 'notebook' folder or symlink, skip
    if os.path.exists(notebook_path):
        logger.info(
            f"'notebook' already exists at {notebook_path}; skipping symlink creation."
        )
        return

    # If jupyter_server is not installed, skip
    if not os.path.exists(jupyter_server_path):
        logger.info(
            f"'jupyter_server' not found at {jupyter_server_path}; skipping symlink creation."
        )
        return

    # Create a symlink so that site-packages/notebook -> site-packages/jupyter_server
    logger.info(
        f"Symlinking '{notebook_path}' to our jupyter server package '{jupyter_server_path}'"
    )
    os.symlink(jupyter_server_path, notebook_path)
    logger.info(f"Created symlink for 'notebook' at: {notebook_path}")
