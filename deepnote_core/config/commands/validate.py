"""Validate configuration command."""

import argparse
import sys

from .utils import get_loader


def validate_command(args: argparse.Namespace) -> int:
    """Validate configuration and exit.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    loader = get_loader(getattr(args, "config", None))

    try:
        loader.load_config()
        return 0
    except (ValueError, AttributeError, TypeError) as e:
        sys.stderr.write(f"Validation failed: {e}\n")
        return 1
    except Exception as e:
        sys.stderr.write(f"Failed to validate configuration with unknown error: {e}\n")
        return 1
