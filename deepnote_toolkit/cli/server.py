"""Server command implementation for Deepnote Toolkit CLI."""

import argparse
import logging
import time
from pathlib import Path

from deepnote_core.config.loader import ConfigurationLoader
from deepnote_core.runtime.plan import build_server_plan
from deepnote_toolkit.runtime.config_io import ensure_effective_config
from deepnote_toolkit.runtime.executor import run_actions_pip
from deepnote_toolkit.runtime.process_manager import managed_processes

logger = logging.getLogger(__name__)


def add_server_subparser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Add the 'server' subcommand parser."""
    p = subparsers.add_parser("server", help="Start Toolkit servers (Jupyter, LSP)")

    # Port configuration
    p.add_argument("--jupyter-port", type=int, dest="jupyter_port")
    p.add_argument("--ls-port", type=int, dest="ls_port")

    # Feature flags - using BooleanOptionalAction for Python 3.9+
    p.add_argument(
        "--enable-terminals",
        action=argparse.BooleanOptionalAction,
        default=None,
        dest="enable_terminals",
    )
    p.add_argument(
        "--python-kernel-only",
        action=argparse.BooleanOptionalAction,
        default=None,
        dest="python_kernel_only",
    )

    # Config file
    p.add_argument("--config", help="Path to config file")

    # Install handler for this subcommand
    p.set_defaults(handler=run_server_command)
    return p


def run_server_command(args: argparse.Namespace) -> int:
    """Execute the server command with proper process management."""
    try:
        # Load configuration with CLI overrides
        config_path = (
            Path(args.config).expanduser() if getattr(args, "config", None) else None
        )
        cfg = ConfigurationLoader(config_path=config_path).load_with_args(args)

        # Persist effective config and set environment
        ensure_effective_config(cfg)

        # Now that we have the config, set up file logging
        # This ensures log directory reflects the correct config
        from deepnote_toolkit.logging import get_logger

        # Determine log level from args (replicating logic from main.py)
        if getattr(args, "quiet", False):
            level = logging.ERROR
        elif getattr(args, "debug", False) or getattr(args, "verbose", 0) >= 2:
            level = logging.DEBUG
        else:
            level = logging.INFO  # Default to INFO to show useful runtime messages

        get_logger(level=level)

        # Build runtime plan
        actions = build_server_plan(cfg)

        # Execute with process management
        with managed_processes() as proc_manager:
            # Start all processes
            procs = run_actions_pip(cfg, actions)

            # Add all processes to manager
            for proc in procs:
                proc_manager.add_process(proc)

                # Check if process started successfully
                time.sleep(0.1)  # Give it a moment to potentially fail
                if proc.poll() is not None:
                    # Get command for better debugging
                    cmd = getattr(proc, "args", "unknown command")
                    if isinstance(cmd, list):
                        cmd = " ".join(str(arg) for arg in cmd)
                    logger.error(
                        f"Process exited immediately with code {proc.returncode}: {cmd}"
                    )
                    return 1

            logger.info(f"Started {len(procs)} server(s). Press Ctrl+C to stop.")

            # Monitor processes
            try:
                while proc_manager.processes:
                    # Check for any dead processes
                    dead_procs = proc_manager.check_processes()
                    if dead_procs:
                        logger.warning(
                            f"{len(dead_procs)} process(es) terminated unexpectedly"
                        )

                    if not proc_manager.processes:
                        logger.info("All processes have terminated")
                        break

                    time.sleep(1)

            except KeyboardInterrupt:
                logger.info("Received interrupt, shutting down servers...")
                # Context manager handles cleanup

        return 0

    except Exception as e:
        logger.error(f"Server command failed: {e}", exc_info=True)
        return 1
