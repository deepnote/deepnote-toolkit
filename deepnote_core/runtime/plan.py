"""Build runtime-agnostic server plans from configuration."""

import shlex
from typing import List

from deepnote_core.config.models import DeepnoteConfig

from .types import (
    EnableJupyterTerminalsAction,
    ExtraServerSpec,
    JupyterServerSpec,
    PythonLSPSpec,
    RuntimeAction,
    StreamlitSpec,
)


def build_server_plan(cfg: DeepnoteConfig) -> List[RuntimeAction]:
    """
    Build a runtime-agnostic plan of servers to start based on configuration.

    This is the single source of truth for what servers should run and how
    they should be configured. Both installer and CLI use this function.

    Args:
        cfg: Validated DeepnoteConfig instance

    Returns:
        List of RuntimeAction specifications to execute
    """
    actions: List[RuntimeAction] = []

    # Enable Jupyter terminals (idempotent system action)
    if cfg.server.enable_terminals:
        actions.append(EnableJupyterTerminalsAction())

    # Jupyter server
    if cfg.server.start_jupyter:
        import os

        # Need --allow-root if running as root (uid 0) or in bundle mode
        allow_root = (
            cfg.installation.install_method == "bundle" or os.getuid() == 0
            if hasattr(os, "getuid")
            else False
        )

        actions.append(
            JupyterServerSpec(
                port=cfg.server.jupyter_port,
                allow_root=allow_root,
                enable_terminals=cfg.server.enable_terminals,
                no_browser=True,
                extra_args=[],
            )
        )

    # Python Language Server
    if cfg.server.start_ls:
        actions.append(PythonLSPSpec(port=cfg.server.ls_port))

    # Streamlit servers (supported if provided by config)
    if cfg.server.start_streamlit_servers and cfg.server.streamlit_scripts:
        for script in cfg.server.streamlit_scripts:
            if isinstance(script, str):
                actions.append(StreamlitSpec(script=script))

    # Extra servers (custom commands)
    if cfg.server.start_extra_servers and cfg.server.extra_servers:
        for item in cfg.server.extra_servers:
            command_parts = []

            if isinstance(item, str):
                # Parse string command
                command_parts = shlex.split(item)
            elif isinstance(item, (list, tuple)):
                # Use list/tuple as command parts, filter empty strings
                command_parts = [str(part) for part in item if str(part).strip()]

            # Only append if we have non-empty command parts
            if command_parts:
                actions.append(ExtraServerSpec(command=command_parts))

    return actions
