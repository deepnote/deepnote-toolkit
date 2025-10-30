"""Configuration I/O utilities for runtime."""

from pathlib import Path
from typing import Tuple

from deepnote_core.config.models import DeepnoteConfig
from deepnote_core.config.persist import persist_effective_config


def ensure_effective_config(cfg: DeepnoteConfig) -> Tuple[str, str]:
    """
    Ensure effective configuration is persisted and discoverable.

    Args:
        cfg: DeepnoteConfig instance

    Returns:
        Tuple of (config_dir, effective_config_path)
    """
    base = Path(cfg.paths.root_dir or Path.home()) / "deepnote-configs"
    base.mkdir(parents=True, exist_ok=True)
    path = persist_effective_config(str(base), cfg)
    return str(base), str(path)
