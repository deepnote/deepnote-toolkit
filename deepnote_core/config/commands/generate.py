"""Generate configuration template command."""

import argparse
import sys
from pathlib import Path

import toml

from ..xdg_paths import XDGPaths


def generate_command(args: argparse.Namespace) -> int:
    """Generate config template.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        xdg = XDGPaths()
        target_file = (
            Path(args.file).expanduser()
            if getattr(args, "file", None)
            else xdg.config_dir / "config.toml"
        )

        if target_file.exists() and not getattr(args, "force", False):
            sys.stderr.write(
                f"File already exists: {target_file}. Use --force to overwrite.\n"
            )
            return 1

        template = {
            "server": {
                "jupyter_port": 8080,
                "ls_port": 8081,
                "start_jupyter": True,
                "start_ls": True,
                "enable_terminals": True,
                "python_kernel_only": False,
            },
            "paths": {
                "home_dir": str(Path.home()),
                "log_dir": str(xdg.log_dir),
            },
            "runtime": {
                "running_in_detached_mode": False,
                "dev_mode": False,
                "env_integration_enabled": False,
            },
        }

        target_file.parent.mkdir(parents=True, exist_ok=True)
        with open(target_file, "w", encoding="utf-8") as f:
            f.write("# Deepnote Toolkit Configuration\n\n")
            toml.dump(template, f)

        print(f"Config template generated at {target_file}")
        return 0
    except Exception as e:
        sys.stderr.write(f"Failed to generate config template: {e}\n")
        return 1
