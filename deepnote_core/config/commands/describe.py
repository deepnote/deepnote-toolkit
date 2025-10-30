"""Describe configuration command."""

import argparse
import sys

from .utils import format_describe, get_loader


def describe_command(args: argparse.Namespace) -> int:
    """Describe configuration fields with current values.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success)
    """
    try:
        loader = get_loader(getattr(args, "config", None))
        cfg = loader.load_config()
        include_secrets = getattr(args, "include_secrets", False)
        print(format_describe(cfg, include_secrets=include_secrets))
    except Exception as e:
        sys.stderr.write(f"Failed to describe configuration: {e}\n")
        return 1
    return 0
