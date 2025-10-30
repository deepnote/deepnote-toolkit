"""Print configuration command."""

import argparse
import json

from ...pydantic_compat_helpers import _model_dump_compat
from .utils import get_loader, redact_secrets


def print_command(args: argparse.Namespace) -> int:
    """Print the effective configuration as JSON.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success)
    """
    loader = get_loader(getattr(args, "config", None))
    cfg = loader.load_config()
    data = _model_dump_compat(cfg)

    # Redact secrets by default unless --include-secrets is specified
    if not getattr(args, "include_secrets", False):
        data = redact_secrets(data)

    print(json.dumps(data, indent=2, default=str))
    return 0
