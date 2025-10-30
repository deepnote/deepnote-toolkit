"""Show configuration command."""

import argparse
import json
import sys

import toml
import yaml

from ...pydantic_compat_helpers import _model_dump_compat
from .utils import get_loader, redact_secrets, stringify_paths


def show_command(args: argparse.Namespace) -> int:
    """Display effective configuration in various formats.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        loader = get_loader(getattr(args, "config", None))
        cfg = loader.load_config()
        data = _model_dump_compat(cfg)

        # Redact secrets by default unless --include-secrets is specified
        if not getattr(args, "include_secrets", False):
            data = redact_secrets(data)

        # Convert Path objects to strings for YAML/TOML serialization
        if args.format in ("yaml", "toml"):
            data = stringify_paths(data)

        if args.format == "json":
            print(json.dumps(data, indent=2, default=str))
        elif args.format == "yaml":
            print(yaml.safe_dump(data, default_flow_style=False))
        elif args.format == "toml":
            print(toml.dumps(data))
        else:
            sys.stderr.write(f"Unsupported format: {getattr(args, 'format', None)}\n")
            return 1
        return 0
    except Exception as e:
        sys.stderr.write(f"Failed to show config: {e}\n")
        return 1
