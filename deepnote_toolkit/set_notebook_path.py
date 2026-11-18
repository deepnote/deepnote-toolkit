import json
import os
import sys
import traceback

import requests
from ipykernel import connect

from . import env
from .config import get_config, resolve_notebook_root
from .logging import get_logger


def set_notebook_path() -> None:
    """Set the notebook path and working directory based on the Jupyter session.

    This function:
    - Sets DEEPNOTE_PROJECT_ID from runtime.project_id in detached or dev mode
    - Returns early in detached mode outside dev mode when neither
      paths.notebook_root nor paths.home_dir is set
    - Otherwise determines the notebook root directory from config or environment
    - Finds the current notebook's path using the Jupyter API; in detached or
      dev mode, parses DEEPNOTE_PROJECT_ID from the session name if still unset
    - Updates sys.path to include the notebook's directory
    - Changes the working directory to the notebook's directory
    """
    try:
        cfg = get_config()
        rt = cfg.runtime
        _detached = bool(rt.running_in_detached_mode or rt.dev_mode)
        project_id = rt.project_id
        # runtime-core starts Jupyter and the kernel in the directory it was given:
        # the .deepnote file's folder, or the deepnote CLI's --cwd
        _keep_kernel_cwd = (
            rt.running_in_detached_mode
            and not rt.dev_mode
            and not cfg.paths.notebook_root
            and not cfg.paths.home_dir
        )
    except Exception:
        _detached, _keep_kernel_cwd, project_id = False, False, None

    if _detached and project_id:
        env.set_env("DEEPNOTE_PROJECT_ID", project_id)
    if _keep_kernel_cwd:
        return

    root_path = resolve_notebook_root()

    try:
        connection_file = os.path.basename(connect.get_connection_file())
        kernel_id = connection_file.split("-", 1)[1].split(".")[0]

        token = env.get_env("DEEPNOTE_JUPYTER_TOKEN")
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"token {token}"

        try:
            cfg = get_config()
            port = str(cfg.server.jupyter_port)
        except Exception:
            port = "8888"

        r = requests.get(
            f"http://0.0.0.0:{port}/api/sessions",
            headers=headers,
        )
        session = next(
            session
            for session in json.loads(r.text)
            if session["kernel"]["id"] == kernel_id
        )

        # ProjectID is used for integration functionality
        # Expected session name format: "{schema_version}:{type}:{project_id}:{...additional segments}"
        if _detached and not env.has_env("DEEPNOTE_PROJECT_ID"):
            env.set_env("DEEPNOTE_PROJECT_ID", session["name"].split(":")[2])

        notebook_path = session["path"]
        if notebook_path.startswith("/"):
            notebook_path = notebook_path[1:]

        notebook_directory = os.path.dirname(
            os.path.abspath(os.path.join(root_path, notebook_path))
        )

        # We want to remove all empty strings from the sys.path
        sys.path = list(filter(lambda path: path and path.strip(), sys.path))

        # We want to remove the root_path ($HOME/work in case of k8s environment, /work in case of local development),
        # which is where our the kernel is started and Jupyter
        # adds the directory in which the kernel is started to the path.
        # This is different from vanilla Jupyter, which starts the kernel in the directory of the ipynb,
        # while we start it in the $HOME/work
        sys.path = list(
            filter(
                lambda path: not os.path.abspath(path).startswith(
                    os.path.abspath(root_path)
                ),
                sys.path,
            )
        )

        # We want to add the directory with the notebook. This way, we reproduce the behavior of Jupyter.
        if notebook_directory not in sys.path:
            sys.path.append(notebook_directory)
        os.chdir(notebook_directory)
        get_logger().info("Kernel working directory set to: %s", notebook_directory)
    except Exception as e:  # pylint: disable=broad-except
        get_logger().error("Failed to set notebook path: %s", e)
        traceback.print_exc()
