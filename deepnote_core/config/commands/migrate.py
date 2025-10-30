"""Migrate configuration command."""

import argparse
import sys

import toml

from ...pydantic_compat_helpers import _model_dump_compat
from ..xdg_paths import XDGPaths
from .utils import get_loader


def migrate_command(args: argparse.Namespace) -> int:
    """Migrate legacy configuration.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        loader = get_loader(getattr(args, "config", None))
        xdg = XDGPaths()
        target_file = xdg.config_dir / "config.toml"

        if target_file.exists():
            sys.stderr.write(f"Config file already exists at {target_file}\n")
            return 1

        cfg = loader.load_config()
        target_file.parent.mkdir(parents=True, exist_ok=True)
        cfg_dict = _model_dump_compat(cfg)

        # Remove secrets from migrated config
        if "runtime" in cfg_dict and "project_secret" in cfg_dict["runtime"]:
            del cfg_dict["runtime"]["project_secret"]

        with open(target_file, "w", encoding="utf-8") as f:
            toml.dump(cfg_dict, f)

        print(f"Configuration migrated to {target_file}")
        return 0
    except Exception as e:
        sys.stderr.write(f"Failed to migrate configuration: {e}\n")
        return 1
