"""Show configuration paths command."""

import argparse
import json
import sys

from ..xdg_paths import XDGPaths


def paths_command(args: argparse.Namespace) -> int:
    """Show configuration-related paths.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        xdg = XDGPaths()
        paths = {
            "config_dir": str(xdg.config_dir),
            "cache_dir": str(xdg.cache_dir),
            "log_dir": str(xdg.log_dir),
        }

        if getattr(args, "json", False):
            print(json.dumps(paths, indent=2))
        else:
            for key, value in paths.items():
                print(f"{key}: {value}")
        return 0
    except Exception as e:
        sys.stderr.write(f"Failed to show paths: {e}\n")
        return 1
