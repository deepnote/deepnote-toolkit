"""Get configuration value command."""

import argparse
import json
import sys

from .utils import get_loader, get_nested_value, is_secret_path


def get_command(args: argparse.Namespace) -> int:
    """Get a specific config value.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        loader = get_loader(getattr(args, "config", None))
        cfg = loader.load_config()
        value = get_nested_value(cfg, args.key)

        # Check if this is a secret field and should be redacted
        if not getattr(args, "include_secrets", False):
            key_parts = tuple(args.key.split("."))
            if len(key_parts) >= 2 and is_secret_path(key_parts[:2]):
                if value:
                    value = "***REDACTED***"

        if getattr(args, "json", False):
            print(json.dumps(value, default=str))
        else:
            print(value)
        return 0
    except KeyError as e:
        sys.stderr.write(f"{e}\n")
        return 1
    except Exception as e:
        sys.stderr.write(f"Failed to get config value: {e}\n")
        return 1
