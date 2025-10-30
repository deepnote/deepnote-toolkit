"""Configuration models and utilities for Deepnote Toolkit.

This module contains all configuration-related functionality including:
- Pydantic models for configuration
- Configuration loader with precedence handling
- Configuration persistence utilities
- CLI argument parsing for configuration
- Path utilities for configuration files
- Runtime resource management for pip installations
"""

from .cli import add_config_subparser, run_config_command
from .loader import ConfigurationLoader
from .models import (
    DeepnoteConfig,
    InstallationConfig,
    PathConfig,
    RuntimeConfig,
    ServerConfig,
)
from .persist import persist_effective_config
from .resources import (
    PreparedResources,
    ResourceSetup,
    ensure_pip_resources,
    get_resources_source_path,
    prepare_runtime_resources,
    setup_runtime_resources,
)

__all__ = [
    "DeepnoteConfig",
    "ServerConfig",
    "PathConfig",
    "InstallationConfig",
    "RuntimeConfig",
    "ConfigurationLoader",
    "persist_effective_config",
    "add_config_subparser",
    "run_config_command",
    "ensure_pip_resources",
    "get_resources_source_path",
    "setup_runtime_resources",
    "prepare_runtime_resources",
    "ResourceSetup",
    "PreparedResources",
]
