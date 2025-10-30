from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from deepnote_core.config.loader import ConfigurationLoader
from deepnote_core.config.models import DeepnoteConfig

from .helper import redact_secrets
from .types import InstallerConfig

logger = logging.getLogger(__name__)


def deepnote_to_installer(cfg: DeepnoteConfig) -> InstallerConfig:
    """Convert a DeepnoteConfig into the legacy InstallerConfig dataclass.

    Args:
        cfg: A validated DeepnoteConfig instance.

    Returns:
        InstallerConfig populated with equivalent fields.

    Raises:
        ValueError: if required top-level properties are missing or invalid.
    """
    try:
        return InstallerConfig(
            work_mountpoint=str(cfg.paths.work_mountpoint),
            venv_path=str(cfg.paths.venv_path),
            python_kernel_only=cfg.server.python_kernel_only,
            start_jupyter=cfg.server.start_jupyter,
            start_ls=cfg.server.start_ls,
            start_streamlit_servers=cfg.server.start_streamlit_servers,
            start_extra_servers=cfg.server.start_extra_servers,
            enable_terminals=cfg.server.enable_terminals,
            # Dataclass init will reject unknown fields, all fields must be explicit
            run_in_detached_mode=cfg.runtime.running_in_detached_mode,
            venv_without_pip=cfg.runtime.venv_without_pip,
            jupyter_port=(
                str(cfg.server.jupyter_port)
                if cfg.server.jupyter_port is not None
                else None
            ),
            ls_port=str(cfg.server.ls_port) if cfg.server.ls_port is not None else None,
            root_dir=str(cfg.paths.root_dir) if cfg.paths.root_dir else None,
            home_dir=str(cfg.paths.home_dir) if cfg.paths.home_dir else None,
            log_dir=str(cfg.paths.log_dir) if cfg.paths.log_dir else None,
        )
    except (AttributeError, TypeError) as e:
        raise ValueError(f"Invalid DeepnoteConfig structure: {e}") from e


def parse_application_arguments(parser: argparse.ArgumentParser) -> InstallerConfig:
    """
    Parse command-line arguments for application and set configuration values.
    """
    parser.add_argument(
        "--run-in-detached-mode",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Run in detached mode",
    )
    parser.add_argument(
        "--work-mountpoint",
        default="/datasets/_deepnote_work",
        help="Path to work mountpoint",
    )
    parser.add_argument(
        "--venv-path", default="~/venv", help="Path to virtual environment"
    )

    parser.add_argument(
        "--python-kernel-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Only python kernel can be started",
    )

    parser.add_argument(
        "--start-jupyter",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Start subprocess with Jupyter server",
    )
    parser.add_argument(
        "--start-ls",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Start subprocess with LS server",
    )
    parser.add_argument(
        "--start-streamlit-servers",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Start subprocess for streamlit servers",
    )
    parser.add_argument(
        "--start-extra-servers",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Start subprocess for extra servers",
    )
    parser.add_argument(
        "--start-servers",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Start subprocess with all servers",
    )
    parser.add_argument(
        "--enable-terminals",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Enable jupyter extension for terminals",
    )
    parser.add_argument(
        "--venv-without-pip",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Create virtual environment without pip to control package installation location",
    )

    parser.add_argument(
        "--jupyter-port",
        help="Port for Jupyter server.",
        default=None,
    )
    parser.add_argument(
        "--ls-port",
        help="Port for Language Server.",
        default=None,
    )

    parser.add_argument(
        "--root-dir",
        help="Root directory for installation paths.",
    )
    parser.add_argument(
        "--home-dir",
        help="Home directory for kernel working directory (cwd).",
    )

    parser.add_argument(
        "--log-dir",
        help="Directory for log files.",
    )

    # New optional config file argument and debug dump
    parser.add_argument(
        "--config",
        help="Path to a deepnote-toolkit config file (TOML/YAML/JSON)",
    )
    parser.add_argument(
        "--print-effective-config",
        action="store_true",
        default=False,
        help="Print the merged effective configuration and exit",
    )

    args, _ = parser.parse_known_args()

    # Build new config with improved precedence: CLI > Env > File > Defaults
    loader = ConfigurationLoader(config_path=Path(args.config) if args.config else None)

    try:
        cfg = loader.load_with_args(args)
    except Exception:
        logger.exception(
            "Configuration load/validation failed (config=%s)", args.config
        )
        sys.exit(2)

    if args.print_effective_config:
        config_dict = cfg.model_dump(mode="json", by_alias=True, exclude_none=True)
        redacted_dict = redact_secrets(config_dict)
        print(json.dumps(redacted_dict, indent=2))
        sys.exit(0)

    # Convert to legacy InstallerConfig shape (non-breaking to downstream)
    return deepnote_to_installer(cfg)
