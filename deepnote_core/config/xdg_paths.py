import os
from pathlib import Path

"""XDG path helpers for configuration, cache, and state/log directories."""


class XDGPaths:
    """Compute XDG-compliant paths with sensible defaults for this app."""

    def __init__(self, app_name: str = "deepnote-toolkit"):
        self.app_name = app_name

    @property
    def config_dir(self) -> Path:
        """XDG config dir for the app (respects XDG_CONFIG_HOME, expands ~)."""
        base = os.environ.get("XDG_CONFIG_HOME")
        if base:
            return Path(base).expanduser() / self.app_name
        return Path.home() / ".config" / self.app_name

    @property
    def cache_dir(self) -> Path:
        """XDG cache dir for the app (respects XDG_CACHE_HOME, expands ~)."""
        base = os.environ.get("XDG_CACHE_HOME")
        if base:
            return Path(base).expanduser() / self.app_name
        return Path.home() / ".cache" / self.app_name

    @property
    def log_dir(self) -> Path:
        """XDG state dir/logs for the app (respects XDG_STATE_HOME, expands ~)."""
        base = os.environ.get("XDG_STATE_HOME")
        if base:
            return Path(base).expanduser() / self.app_name / "logs"
        return Path.home() / ".local" / "state" / self.app_name / "logs"

    @property
    def data_home(self) -> Path:
        """XDG data dir for the app (respects XDG_DATA_HOME, expands ~)."""
        base = os.environ.get("XDG_DATA_HOME")
        if base:
            return Path(base).expanduser() / self.app_name
        return Path.home() / ".local" / "share" / self.app_name

    # Backward compatibility alias
    def get_log_dir(self) -> Path:  # pragma: no cover - thin wrapper
        return self.log_dir
