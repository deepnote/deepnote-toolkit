from __future__ import annotations

import argparse

from . import commands

# Re-export for backward compatibility
from .commands.utils import format_describe as _format_describe
from .commands.utils import get_nested_value as _get_nested_value
from .commands.utils import set_nested_value as _set_nested_value

__all__ = ["_format_describe", "_get_nested_value", "_set_nested_value"]


def _add_common_config_args(
    parser: argparse.ArgumentParser, include_runtime: bool = False
) -> None:
    """Add common config arguments to a parser."""
    parser.add_argument("--config", help="Path to a config file (TOML/YAML/JSON)")
    if include_runtime:
        parser.add_argument(
            "--runtime",
            action="store_true",
            help="Load using runtime precedence (Env > File > Defaults)",
        )


def add_config_subparser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    """Add config management subcommands to a parent parser."""
    p = subparsers.add_parser("config", help="Configuration management")
    sub = p.add_subparsers(dest="config_cmd", required=True)

    # Add all the subcommands
    p_print = sub.add_parser("print", help="Print the effective configuration as JSON")
    _add_common_config_args(
        p_print, include_runtime=True
    )  # Include runtime flag for print command
    p_print.add_argument(
        "--include-secrets",
        action="store_true",
        help="Include secret values (by default they are redacted)",
    )

    p_desc = sub.add_parser(
        "describe", help="Describe configuration fields with current values"
    )
    _add_common_config_args(
        p_desc, include_runtime=True
    )  # Include runtime flag for describe command
    p_desc.add_argument(
        "--include-secrets",
        action="store_true",
        help="Include secret values (by default they are redacted)",
    )

    p_val = sub.add_parser("validate", help="Validate configuration and exit")
    _add_common_config_args(p_val)

    p_show = sub.add_parser("show", help="Display effective configuration")
    p_show.add_argument(
        "--format",
        choices=["json", "yaml", "toml"],
        default="json",
        help="Output format",
    )
    _add_common_config_args(
        p_show, include_runtime=False
    )  # Runtime flag doesn't affect behavior
    p_show.add_argument(
        "--include-secrets",
        action="store_true",
        help="Include secret values (by default they are redacted)",
    )

    p_get = sub.add_parser("get", help="Get a specific config value")
    p_get.add_argument(
        "key", help="Config key (dotted path, e.g., server.jupyter_port)"
    )
    p_get.add_argument("--json", action="store_true", help="Output raw JSON value")
    p_get.add_argument(
        "--include-secrets",
        action="store_true",
        help="Include secret values (by default they are redacted)",
    )
    _add_common_config_args(p_get)

    p_set = sub.add_parser("set", help="Set a config value")
    p_set.add_argument("key", help="Config key (dotted path)")
    p_set.add_argument("value", help="Value to set")
    p_set.add_argument(
        "--file", help="Config file to update in TOML format (default: user config)"
    )

    sub.add_parser("migrate", help="Migrate legacy configuration")

    p_generate = sub.add_parser("generate", help="Generate config template")
    p_generate.add_argument("--file", help="Output file path")
    p_generate.add_argument(
        "--force", action="store_true", help="Overwrite existing file"
    )

    p_paths = sub.add_parser("paths", help="Show configuration-related paths")
    p_paths.add_argument("--json", action="store_true", help="Output as JSON")

    # Register handler for the top-level 'config' subcommand
    p.set_defaults(handler=run_config_command)
    return p


def run_config_command(args: argparse.Namespace) -> int:
    """Execute config commands from the CLI.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    cmd = getattr(args, "config_cmd", getattr(args, "cmd", None))

    # Map command names to handler functions
    command_map = {
        "print": commands.print_command,
        "validate": commands.validate_command,
        "describe": commands.describe_command,
        "show": commands.show_command,
        "get": commands.get_command,
        "set": commands.set_command,
        "migrate": commands.migrate_command,
        "generate": commands.generate_command,
        "paths": commands.paths_command,
    }

    handler = command_map.get(cmd)
    if handler:
        return handler(args)

    # Unknown command
    return 1
