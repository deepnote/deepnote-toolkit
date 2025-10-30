"""Global config access for the toolkit.

Loads the effective configuration with precedence Env > File > Defaults.
The installer writes an effective config file and sets DEEPNOTE_CONFIG_FILE
so kernels can load it without relying on re-exported env variables.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path
from typing import Optional, Union

from deepnote_core.config.loader import ConfigurationLoader
from deepnote_core.config.models import DeepnoteConfig


@lru_cache(maxsize=128)
def _load_config_cached(config_path: Optional[Path]) -> DeepnoteConfig:
    """Internal function that loads config with LRU caching.

    Args:
        config_path: Optional resolved Path to config file.

    Returns:
        Loaded DeepnoteConfig instance.

    Note:
        This is the actual cached function. The Path must be resolved
        and hashable for the cache to work properly.
    """
    loader = ConfigurationLoader(config_path=config_path)
    return loader.load_config()


def clear_config_cache() -> None:
    """Clear the configuration cache. Useful for testing."""
    _load_config_cached.cache_clear()


def get_config(
    config_path: Optional[Union[str, os.PathLike[str]]] = None
) -> DeepnoteConfig:
    """Load the effective configuration (memoized per config_path).

    Args:
        config_path: Optional path to a config file to load (TOML/YAML/JSON).
                    Can be a string or path-like object.

    Returns:
        DeepnoteConfig loaded with precedence Env > File > Defaults.

    Note:
        Results are memoized for the process lifetime using functools.lru_cache
        which provides thread-safe caching. The same config_path will return
        the same DeepnoteConfig instance on subsequent calls.
    """
    # Convert path-like to Path object if provided
    cfg_path = (
        Path(config_path).expanduser().resolve() if config_path is not None else None
    )

    # Call the cached function
    return _load_config_cached(cfg_path)
