import json
import os
import sys
import traceback
from pathlib import Path

import requests
from ipykernel import connect

from . import env
from .config import get_config


def set_notebook_path() -> None:
    """Set the notebook path and working directory based on the Jupyter session.

    This function:
    - Determines the notebook root directory from config or environment
    - Finds the current notebook's path using the Jupyter API
    - Updates sys.path to include the notebook's directory
    - Changes the working directory to the notebook's directory
    - Sets DEEPNOTE_PROJECT_ID in detached mode
    """
    # Determine notebooks common root using config first, with env fallback
    try:
        cfg = get_config()
        # Prefer explicit notebook_root when provided
        if cfg.paths.notebook_root:
            root_path = Path(cfg.paths.notebook_root)
        elif cfg.paths.home_dir:
            root_path = Path(cfg.paths.home_dir)
        elif cfg.runtime.dev_mode:
            root_path = Path("/work")
        else:
            # Respect HOME if provided for compatibility with tests and environments
            root_path = Path(os.environ.get("HOME", str(Path.home()))) / "work"
    except Exception:
        # Fallback: respect HOME when available, but honor dev-mode env toggle
        root_path = (
            Path("/work")
            if env.get_env("DEEPNOTE_RUNNING_IN_DEV_MODE")
            else Path(os.environ.get("HOME", str(Path.home()))) / "work"
        )

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

        # If not provided as an environmental variable, use projectId parsed from session name
        # ProjectID is used for integration functionality
        # Expected session name format: "{schema_version}:{type}:{project_id}:{...additional segments}"
        try:
            rt = get_config().runtime
            _detached = bool(rt.running_in_detached_mode or rt.dev_mode)
        except Exception:
            _detached = False
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
    except Exception:  # pylint: disable=broad-except
        traceback.print_exc()
