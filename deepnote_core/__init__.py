"""Shared core components for Deepnote Toolkit.

This package contains shared functionality used by both the installer and toolkit:
- Configuration models and loading
- Execution contexts and action registry
- Runtime types and planning
- Pydantic compatibility helpers

The configuration system resolves from multiple sources using the precedence:
CLI > Environment > Config File > Installation Defaults > Built-in Defaults

Both the installer and the toolkit should depend on this module to ensure
consistent behavior without circular dependencies.
"""

# Re-export configuration components for backward compatibility
from .config import (
    ConfigurationLoader,
    DeepnoteConfig,
    InstallationConfig,
    PathConfig,
    RuntimeConfig,
    ServerConfig,
)

__all__ = [
    "DeepnoteConfig",
    "ServerConfig",
    "PathConfig",
    "InstallationConfig",
    "RuntimeConfig",
    "ConfigurationLoader",
]
