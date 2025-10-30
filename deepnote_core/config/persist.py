from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Union

from ..pydantic_compat_helpers import _model_dump_compat
from .models import DeepnoteConfig


def persist_effective_config(base_dir: Union[str, Path], cfg: DeepnoteConfig) -> Path:
    """Write the effective configuration to base_dir/effective-config.json atomically.

    This function writes the config atomically to prevent partial writes and ensures
    the file has restrictive permissions (0600) to protect sensitive data.

    Also sets DEEPNOTE_CONFIG_FILE env var for child processes to discover it.

    Args:
        base_dir: Directory where config file will be written.
        cfg: Configuration object to persist.

    Returns:
        Path to the written configuration file.
    """
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    target = base / "effective-config.json"

    # Write to temporary file first
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        dir=base,
        prefix=".effective-config-",
        suffix=".tmp",
        delete=False,
    ) as tmp_file:
        tmp_path = Path(tmp_file.name)
        try:
            payload = _model_dump_compat(cfg)
            json.dump(payload, tmp_file, indent=2)
            tmp_file.flush()
            os.fsync(tmp_file.fileno())
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

    # Ensure restrictive perms and atomically replace after the file is closed
    try:
        tmp_path.chmod(0o600)
    except OSError:
        pass
    tmp_path.replace(target)

    os.environ["DEEPNOTE_CONFIG_FILE"] = str(target.resolve())
    return target
