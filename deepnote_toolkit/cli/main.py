"""Main CLI entry point for Deepnote Toolkit."""

import argparse
import logging
from typing import Optional

from deepnote_core.config.cli import add_config_subparser
from deepnote_toolkit._version import __version__

from .server import add_server_subparser


def build_parser() -> argparse.ArgumentParser:
    """Build the main argument parser with subcommands."""
    p = argparse.ArgumentParser(
        prog="deepnote-toolkit", description="Deepnote Toolkit CLI"
    )

    # Global options
    p.add_argument("-v", "--verbose", action="count", default=0)
    p.add_argument("-q", "--quiet", action="store_true")
    p.add_argument("-d", "--debug", action="store_true")
    p.add_argument("--version", action="version", version=f"%(prog)s {__version__}")

    # Subcommands
    sub = p.add_subparsers(dest="_cmd")
    add_server_subparser(sub)
    add_config_subparser(sub)

    return p


def main(argv: Optional[list] = None) -> int:
    """Main CLI entry point."""
    parser = build_parser()
    args = parser.parse_args(argv)

    # Setup logging using the main logging system
    # Determine log level based on CLI flags
    if args.quiet:
        level = logging.ERROR
    elif args.debug or args.verbose >= 2:
        level = logging.DEBUG
    else:
        level = logging.INFO  # Default to INFO to show useful runtime messages

    # Configure basic console logging only (no file logging here)
    # Include module name for better traceability
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )

    # Note: File logging is set up by server command after config is loaded
    # Other commands don't need file logging

    # Dispatch using handler set by subparser
    handler = getattr(args, "handler", None)
    if callable(handler):
        return handler(args)

    # No command handler specified
    parser.print_help()
    return 1
