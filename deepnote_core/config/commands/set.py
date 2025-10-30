"""Set configuration value command."""

import argparse
import os
import stat
import sys
import tempfile
from pathlib import Path

import toml

try:
    import tomllib  # type: ignore[import-not-found]
except ImportError:
    import tomli as tomllib  # type: ignore[import-not-found,no-redef]

from ..loader import ConfigurationLoader
from ..xdg_paths import XDGPaths
from .utils import set_nested_value


def set_command(args: argparse.Namespace) -> int:
    """Set a config value atomically.

    Args:
        args: Command line arguments

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    temp_fd = None
    temp_path = None
    try:
        xdg = XDGPaths()
        config_file = (
            Path(args.file).expanduser()
            if getattr(args, "file", None)
            else xdg.config_dir / "config.toml"
        )

        config_file.parent.mkdir(parents=True, exist_ok=True)

        # Load existing config or start with empty dict
        if config_file.exists():
            with open(config_file, "rb") as f:
                data = tomllib.load(f)
            # Preserve original file permissions
            original_stat = config_file.stat()
            file_mode = stat.S_IMODE(original_stat.st_mode)
        else:
            data = {}
            # Default permissions for new file (readable/writable by owner only)
            file_mode = 0o600

        # Apply the new value
        set_nested_value(data, args.key, args.value)

        # Write to a temporary file in the same directory (for atomic rename)
        temp_fd, temp_path = tempfile.mkstemp(
            dir=config_file.parent,
            prefix=f".{config_file.name}.",
            suffix=".tmp",
            text=False,
        )

        # Write the updated config to temp file
        with os.fdopen(temp_fd, "w", encoding="utf-8") as f:
            temp_fd = None  # Ownership transferred to fdopen
            toml.dump(data, f)
            f.flush()
            os.fsync(f.fileno())

        # Set permissions on temp file to match original
        os.chmod(temp_path, file_mode)

        # Validate the new configuration before committing
        try:
            # Test load the config with the temp file
            test_loader = ConfigurationLoader(config_path=Path(temp_path))
            test_loader.load_config()
        except Exception as e:
            # Validation failed - clean up temp file and report error
            os.unlink(temp_path)
            sys.stderr.write(f"Configuration validation failed: {e}\n")
            return 1

        # Validation passed - atomically replace the original file
        os.replace(temp_path, str(config_file))
        temp_path = None  # Successfully moved

        print(f"Config updated: {args.key} = {args.value}")
        return 0

    except Exception as e:
        sys.stderr.write(f"Failed to set config value: {e}\n")
        return 1
    finally:
        # Clean up any remaining temp file
        if temp_fd is not None:
            try:
                os.close(temp_fd)
            except Exception:
                pass
        if temp_path is not None and os.path.exists(temp_path):
            try:
                os.unlink(temp_path)
            except Exception:
                pass
